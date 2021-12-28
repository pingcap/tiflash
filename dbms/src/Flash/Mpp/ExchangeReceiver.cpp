#include <Common/CPUAffinityManager.h>
#include <Common/ThreadFactory.h>
#include <Flash/Coprocessor/CoprocessorReader.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Flash/Mpp/MPPTunnel.h>
#include <fmt/core.h>

namespace DB
{
namespace
{
String getReceiverStateStr(const ExchangeReceiverState & s)
{
    switch (s)
    {
    case ExchangeReceiverState::NORMAL:
        return "NORMAL";
    case ExchangeReceiverState::ERROR:
        return "ERROR";
    case ExchangeReceiverState::CANCELED:
        return "CANCELED";
    case ExchangeReceiverState::CLOSED:
        return "CLOSED";
    default:
        return "UNKNOWN";
    }
}

struct ReactorTrigger
{
    MPMCQueue<size_t> * queue;
    size_t source_index;

    ReactorTrigger(
        MPMCQueue<size_t> * queue_,
        size_t source_index_)
        : queue(queue_)
        , source_index(source_index_)
    {
    }

    void trigger()
    {
        queue->push(source_index);
    }
};

template <typename RPCContext>
struct MakeReaderCallback : public UnaryCallback<std::shared_ptr<typename RPCContext::AsyncReader>>
{
    ReactorTrigger * trigger = nullptr;
    std::shared_ptr<typename RPCContext::AsyncReader> reader;

    explicit MakeReaderCallback(ReactorTrigger * trigger_)
        : trigger(trigger_)
    {}

    void execute(std::shared_ptr<typename RPCContext::AsyncReader> & res) override
    {
        reader = std::move(res);
        trigger->trigger();
    }
};

struct BatchReadCallback : public UnaryCallback<size_t>
{
    ReactorTrigger * trigger = nullptr;
    size_t packet_cnt = 0;
    MPPDataPacketPtrs packets;

    BatchReadCallback(ReactorTrigger * trigger_, size_t batch_packet_count)
        : trigger(trigger_)
    {
        packets.resize(batch_packet_count);
        for (auto & packet : packets)
            packet = std::make_shared<MPPDataPacket>();
    }

    void execute(size_t & res) override
    {
        packet_cnt = res;
        trigger->trigger();
    }
};

template <typename RPCContext>
struct FinishCallback : public UnaryCallback<typename RPCContext::Status>
{
    using Status = typename RPCContext::Status;
    ReactorTrigger * trigger = nullptr;
    Status status = RPCContext::getStatusOK();

    explicit FinishCallback(ReactorTrigger * trigger_)
        : trigger(trigger_)
    {}

    void execute(Status & res) override
    {
        status = res;
        trigger->trigger();
    }
};

enum class AsyncRequestStage
{
    NEED_INIT,
    WAIT_MAKE_READER,
    WAIT_BATCH_READ,
    WAIT_FINISH,
    WAIT_RETRY,
    FINISHED,
};

using Clock = std::chrono::system_clock;
using TimePoint = Clock::time_point;

static constexpr Int32 MAX_RETRY_TIMES = 10;
static constexpr Int32 BATCH_PACKET_COUNT = 16;

template <typename RPCContext>
struct AsyncRequestStat
{
    using Status = typename RPCContext::Status;
    using Request = typename RPCContext::Request;
    using AsyncReader = typename RPCContext::AsyncReader;

    std::shared_ptr<RPCContext> rpc_context;
    const Request * request = nullptr;
    MPMCQueue<std::shared_ptr<ReceivedMessage>> * msg_channel = nullptr;

    String req_info;
    bool meet_error = false;
    bool has_data = false;
    String err_msg;
    int retry_times = 0;
    TimePoint start_waiting_retry_ts{Clock::duration::zero()};
    AsyncRequestStage stage = AsyncRequestStage::NEED_INIT;

    ReactorTrigger trigger;
    MakeReaderCallback<RPCContext> make_reader_callback;
    BatchReadCallback batch_read_callback;
    FinishCallback<RPCContext> finish_callback;

    LogWithPrefixPtr log;

    AsyncRequestStat(
        MPMCQueue<size_t> * queue,
        MPMCQueue<std::shared_ptr<ReceivedMessage>> * msg_channel_,
        const std::shared_ptr<RPCContext> & context,
        const Request & req,
        const LogWithPrefixPtr & log_)
        : rpc_context(context)
        , request(&req)
        , msg_channel(msg_channel_)
        , req_info(fmt::format("tunnel{}+{}", req.send_task_id, req.recv_task_id))
        , trigger(queue, req.source_index)
        , make_reader_callback(&trigger)
        , batch_read_callback(&trigger, BATCH_PACKET_COUNT)
        , finish_callback(&trigger)
        , log(getMPPTaskLog(log_, req_info))
    {
    }

    void handle()
    {
        LOG_FMT_TRACE(log, "Enter {}. stage: {}", __PRETTY_FUNCTION__, stage);
        switch (stage)
        {
        case AsyncRequestStage::NEED_INIT:
            start();
            break;
        case AsyncRequestStage::WAIT_MAKE_READER:
            if (auto * reader = getReader())
            {
                reader->batchRead(getPackets(), &batch_read_callback);
                stage = AsyncRequestStage::WAIT_BATCH_READ;
            }
            else
            {
                LOG_FMT_WARNING(log, "MakeReader fail. retry time: {}", retry_times);

                if (retriable())
                    waitForRetry();
                else
                    setDone("Exchange receiver meet error : send async stream request fail");
            }
            break;
        case AsyncRequestStage::WAIT_BATCH_READ:
            if (getPacketCount() > 0)
            {
                LOG_FMT_TRACE(log, "Received {} packets.", getPacketCount());
                has_data = true;
                if (auto packet = firstErrorPacket())
                    setDone("Exchange receiver meet error : " + packet->error().msg());
                else if (sendPackets())
                    getReader()->batchRead(getPackets(), &batch_read_callback);
                else
                    setDone("Exchange receiver meet error : push packets fail");
            }
            else
            {
                getReader()->finish(&finish_callback);
                stage = AsyncRequestStage::WAIT_FINISH;
            }
            break;
        case AsyncRequestStage::WAIT_FINISH:
            if (getStatus().ok())
                setDone("");
            else
            {
                LOG_FMT_WARNING(
                    log,
                    "Finish fail. err code: {}, err msg: {}, retry time {}",
                    getStatus().error_code(),
                    getStatus().error_message(),
                    retry_times);

                if (retriable())
                    waitForRetry();
                else
                    setDone("Exchange receiver meet error : " + getStatus().error_message());
            }
            break;
        case AsyncRequestStage::WAIT_RETRY:
            throw Exception("Can't call handle() when WAIT_RETRY");
        case AsyncRequestStage::FINISHED:
            throw Exception("Can't call handle() when FINISHED");
        }
    }

    bool finished() const
    {
        return stage == AsyncRequestStage::FINISHED;
    }

    AsyncReader * getReader() const
    {
        return make_reader_callback.reader.get();
    }

    MPPDataPacketPtrs & getPackets()
    {
        return batch_read_callback.packets;
    }

    MPPDataPacketPtr firstErrorPacket() const
    {
        for (const auto & packet : batch_read_callback.packets)
        {
            if (packet->has_error())
                return packet;
        }
        return nullptr;
    }

    size_t getPacketCount() const
    {
        return batch_read_callback.packet_cnt;
    }

    const Status & getStatus() const
    {
        return finish_callback.status;
    }

    bool retriable() const
    {
        return !has_data && retry_times + 1 < MAX_RETRY_TIMES;
    }

    bool waitingForRetry() const
    {
        return stage == AsyncRequestStage::WAIT_RETRY;
    }

    void setDone(String msg)
    {
        if (!msg.empty())
        {
            meet_error = true;
            err_msg = std::move(msg);
        }
        stage = AsyncRequestStage::FINISHED;
    }

    void start()
    {
        rpc_context->makeAsyncReader(*request, &make_reader_callback);
        stage = AsyncRequestStage::WAIT_MAKE_READER;
    }

    bool retry()
    {
        if (Clock::now() - start_waiting_retry_ts >= std::chrono::seconds(1))
        {
            ++retry_times;
            start();
            return true;
        }
        return false;
    }

    void waitForRetry()
    {
        start_waiting_retry_ts = Clock::now();
        stage = AsyncRequestStage::WAIT_RETRY;
    }

    bool sendPackets()
    {
        for (auto & packet : getPackets())
        {
            auto recv_msg = std::make_shared<ReceivedMessage>();
            recv_msg->packet = packet;
            recv_msg->source_index = request->source_index;
            recv_msg->req_info = req_info;
            if (!msg_channel->push(std::move(recv_msg)))
                return false;
            packet = std::make_shared<MPPDataPacket>();
        }
        return true;
    }
};
} // namespace

template <typename RPCContext>
ExchangeReceiverBase<RPCContext>::ExchangeReceiverBase(
    std::shared_ptr<RPCContext> rpc_context_,
    size_t source_num_,
    size_t max_streams_,
    const std::shared_ptr<LogWithPrefix> & log_)
    : rpc_context(std::move(rpc_context_))
    , source_num(source_num_)
    , max_streams(max_streams_)
    , max_buffer_size(std::max(source_num, max_streams_) * 2)
    , msg_channel(max_buffer_size)
    , live_connections(source_num)
    , state(ExchangeReceiverState::NORMAL)
    , exc_log(getMPPTaskLog(log_, "ExchangeReceiver"))
{
    rpc_context->fillSchema(schema);
    setUpConnection();
}

template <typename RPCContext>
ExchangeReceiverBase<RPCContext>::~ExchangeReceiverBase()
{
    setState(ExchangeReceiverState::CLOSED);
    msg_channel.finish();

    for (auto & worker : workers)
        worker.join();
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::cancel()
{
    setState(ExchangeReceiverState::CANCELED);
    msg_channel.finish();
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::setUpConnection()
{
    std::vector<Request> async_requests;

    for (size_t index = 0; index < source_num; ++index)
    {
        auto req = rpc_context->makeRequest(index);
        if (rpc_context->supportAsync(req))
            async_requests.push_back(std::move(req));
        else
            workers.push_back(ThreadFactory::newThread("Receiver", &ExchangeReceiverBase<RPCContext>::readLoop, this, std::move(req)));
    }

    if (!async_requests.empty())
        workers.push_back(ThreadFactory::newThread("RecvReactor", &ExchangeReceiverBase<RPCContext>::reactor, this, std::move(async_requests)));
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::reactor(std::vector<Request> async_requests)
{
    CPUAffinityManager::getInstance().bindSelfQueryThread();

    size_t alive_async_connections = async_requests.size();
    MPMCQueue<size_t> ready_requests(alive_async_connections * 2);
    std::vector<size_t> waiting_for_retry_requests;

    std::vector<AsyncRequestStat<RPCContext>> stats;
    stats.reserve(alive_async_connections);
    for (const auto & req : async_requests)
    {
        stats.emplace_back(&ready_requests, &msg_channel, rpc_context, req, exc_log);
        stats.back().start();
    }

    static constexpr auto timeout = std::chrono::milliseconds(10);

    while (alive_async_connections > 0)
    {
        size_t source_index = 0;
        if (ready_requests.tryPop(source_index, timeout))
        {
            auto & stat = stats[source_index];
            stat.handle();
            if (stat.finished())
            {
                --alive_async_connections;
                connectionDone(stat.meet_error, stat.err_msg, stat.log);
            }
            else if (stat.waitingForRetry())
            {
                waiting_for_retry_requests.push_back(source_index);
            }
        }
        else if (!waiting_for_retry_requests.empty())
        {
            std::vector<size_t> tmp;
            for (auto i : waiting_for_retry_requests)
            {
                if (!stats[i].retry())
                    tmp.push_back(i);
            }
            waiting_for_retry_requests.swap(tmp);
        }
    }
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::readLoop(Request req)
{
    CPUAffinityManager::getInstance().bindSelfQueryThread();
    bool meet_error = false;
    String local_err_msg;
    String req_info = fmt::format("tunnel{}+{}", req.send_task_id, req.recv_task_id);

    LogWithPrefixPtr log = getMPPTaskLog(exc_log, req_info);

    try
    {
        auto status = RPCContext::getStatusOK();
        for (int i = 0; i < MAX_RETRY_TIMES; ++i)
        {
            auto reader = rpc_context->makeReader(req);
            bool has_data = false;
            for (;;)
            {
                LOG_TRACE(log, "begin next ");
                auto recv_msg = std::make_shared<ReceivedMessage>();
                recv_msg->packet = std::make_shared<MPPDataPacket>();
                recv_msg->req_info = req_info;
                recv_msg->source_index = req.source_index;
                bool success = reader->read(recv_msg->packet);
                if (!success)
                    break;
                has_data = true;
                if (recv_msg->packet->has_error())
                    throw Exception("Exchange receiver meet error : " + recv_msg->packet->error().msg());

                if (!msg_channel.push(std::move(recv_msg)))
                {
                    meet_error = true;
                    auto local_state = getState();
                    local_err_msg = "receiver's state is " + getReceiverStateStr(local_state) + ", exit from readLoop";
                    LOG_WARNING(log, local_err_msg);
                    break;
                }
            }
            // if meet error, such as decode packect fails, it will not retry.
            if (meet_error)
            {
                break;
            }
            status = reader->finish();
            if (status.ok())
            {
                LOG_DEBUG(log, "finish read : " << req.debugString());
                break;
            }
            else
            {
                bool retriable = !has_data && i + 1 < MAX_RETRY_TIMES;
                LOG_FMT_WARNING(
                    log,
                    "EstablishMPPConnectionRequest meets rpc fail. Err code = {}, err msg = {}, retriable = {}",
                    status.error_code(),
                    status.error_message(),
                    retriable);
                // if we have received some data, we should not retry.
                if (has_data)
                    break;

                using namespace std::chrono_literals;
                std::this_thread::sleep_for(1s);
            }
        }
        if (!status.ok())
        {
            meet_error = true;
            local_err_msg = status.error_message();
        }
    }
    catch (Exception & e)
    {
        meet_error = true;
        local_err_msg = e.message();
    }
    catch (std::exception & e)
    {
        meet_error = true;
        local_err_msg = e.what();
    }
    catch (...)
    {
        meet_error = true;
        local_err_msg = "fatal error";
    }
    connectionDone(meet_error, local_err_msg, log);
}

template <typename RPCContext>
DecodeDetail ExchangeReceiverBase<RPCContext>::decodeChunks(
    const std::shared_ptr<ReceivedMessage> & recv_msg,
    std::queue<Block> & block_queue,
    const Block & header)
{
    assert(recv_msg != nullptr);
    DecodeDetail detail;

    int chunk_size = recv_msg->packet->chunks_size();
    if (chunk_size == 0)
        return detail;

    detail.packet_bytes = recv_msg->packet->ByteSizeLong();
    /// ExchangeReceiverBase should receive chunks of TypeCHBlock
    for (int i = 0; i < chunk_size; ++i)
    {
        Block block = CHBlockChunkCodec::decode(recv_msg->packet->chunks(i), header);
        detail.rows += block.rows();
        if (unlikely(block.rows() == 0))
            continue;
        block_queue.push(std::move(block));
    }
    return detail;
}

template <typename RPCContext>
ExchangeReceiverResult ExchangeReceiverBase<RPCContext>::nextResult(std::queue<Block> & block_queue, const Block & header)
{
    std::shared_ptr<ReceivedMessage> recv_msg;
    if (!msg_channel.pop(recv_msg))
    {
        std::unique_lock lock(mu);

        if (state != ExchangeReceiverState::NORMAL)
        {
            String msg;
            if (state == ExchangeReceiverState::CANCELED)
                msg = "query canceled";
            else if (state == ExchangeReceiverState::CLOSED)
                msg = "ExchangeReceiver closed";
            else if (!err_msg.empty())
                msg = err_msg;
            else
                msg = "Unknown error";
            return {nullptr, 0, "ExchangeReceiver", true, msg, false};
        }
        else /// live_connections == 0, msg_channel is finished, and state is NORMAL, that is the end.
        {
            return {nullptr, 0, "ExchangeReceiver", false, "", true};
        }
    }
    assert(recv_msg != nullptr && recv_msg->packet != nullptr);
    ExchangeReceiverResult result;
    if (recv_msg->packet->has_error())
    {
        result = {nullptr, recv_msg->source_index, recv_msg->req_info, true, recv_msg->packet->error().msg(), false};
    }
    else
    {
        if (!recv_msg->packet->data().empty()) /// the data of the last packet is serialized from tipb::SelectResponse including execution summaries.
        {
            auto resp_ptr = std::make_shared<tipb::SelectResponse>();
            if (!resp_ptr->ParseFromString(recv_msg->packet->data()))
            {
                result = {nullptr, recv_msg->source_index, recv_msg->req_info, true, "decode error", false};
            }
            else
            {
                result = {resp_ptr, recv_msg->source_index, recv_msg->req_info, false, "", false};
                /// If mocking TiFlash as TiDB, here should decode chunks from resp_ptr.
                if (!resp_ptr->chunks().empty())
                {
                    assert(recv_msg->packet->chunks().empty());
                    result.decode_detail = CoprocessorReader::decodeChunks(resp_ptr, block_queue, header, schema);
                }
            }
        }
        else /// the non-last packets
        {
            result = {nullptr, recv_msg->source_index, recv_msg->req_info, false, "", false};
        }
        if (!result.meet_error && !recv_msg->packet->chunks().empty())
        {
            assert(result.decode_detail.rows == 0);
            result.decode_detail = decodeChunks(recv_msg, block_queue, header);
        }
    }
    return result;
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::setState(ExchangeReceiverState new_state)
{
    std::unique_lock lock(mu);
    state = new_state;
}

template <typename RPCContext>
ExchangeReceiverState ExchangeReceiverBase<RPCContext>::getState()
{
    std::unique_lock lock(mu);
    return state;
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::connectionDone(
    bool meet_error,
    const String & local_err_msg,
    const LogWithPrefixPtr & log)
{
    Int32 copy_live_conn = -1;
    {
        std::unique_lock lock(mu);
        if (meet_error)
        {
            if (state == ExchangeReceiverState::NORMAL)
                state = ExchangeReceiverState::ERROR;
            if (err_msg.empty())
                err_msg = local_err_msg;
        }
        copy_live_conn = --live_connections;
    }
    LOG_FMT_DEBUG(
        log,
        "connection end. meet error: {}, err msg: {}, current alive connections: {}",
        meet_error,
        local_err_msg,
        copy_live_conn);

    if (copy_live_conn == 0)
    {
        LOG_FMT_DEBUG(log, "All threads end in ExchangeReceiver");
    }
    else if (copy_live_conn < 0)
        throw Exception("live_connections should not be less than 0!");

    if (meet_error || copy_live_conn == 0)
        msg_channel.finish();
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class ExchangeReceiverBase<GRPCReceiverContext>;

} // namespace DB
