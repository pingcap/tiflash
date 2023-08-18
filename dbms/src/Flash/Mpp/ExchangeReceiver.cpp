// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/CPUAffinityManager.h>
#include <Common/ThreadFactory.h>
#include <Common/TiFlashMetrics.h>
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

constexpr Int32 max_retry_times = 10;
constexpr Int32 batch_packet_count = 16;

template <typename RPCContext>
class AsyncRequestHandler : public UnaryCallback<bool>
{
public:
    using Status = typename RPCContext::Status;
    using Request = typename RPCContext::Request;
    using AsyncReader = typename RPCContext::AsyncReader;
    using Self = AsyncRequestHandler<RPCContext>;

    AsyncRequestHandler(
        MPMCQueue<Self *> * queue,
        MPMCQueue<std::shared_ptr<ReceivedMessage>> * msg_channel_,
        const std::shared_ptr<RPCContext> & context,
        const Request & req,
        const String & req_id)
        : rpc_context(context)
        , request(&req)
        , notify_queue(queue)
        , msg_channel(msg_channel_)
        , req_info(fmt::format("tunnel{}+{}", req.send_task_id, req.recv_task_id))
        , log(Logger::get("ExchangeReceiver", req_id, req_info))
    {
        packets.resize(batch_packet_count);
        for (auto & packet : packets)
            packet = std::make_shared<MPPDataPacket>();

        start();
    }

    // execute will be called by RPC framework so it should be as light as possible.
    void execute(bool & ok) override
    {
        switch (stage)
        {
        case AsyncRequestStage::WAIT_MAKE_READER:
        {
            // Use lock to ensure reader is created already in reactor thread
            std::unique_lock lock(mu);
            if (!ok)
                reader.reset();
            notifyReactor();
            break;
        }
        case AsyncRequestStage::WAIT_BATCH_READ:
            if (ok)
                ++read_packet_index;
            if (!ok || read_packet_index == batch_packet_count || packets[read_packet_index - 1]->has_error())
                notifyReactor();
            else
                reader->read(packets[read_packet_index], thisAsUnaryCallback());
            break;
        case AsyncRequestStage::WAIT_FINISH:
            notifyReactor();
            break;
        default:
            __builtin_unreachable();
        }
    }

    // handle will be called by ExchangeReceiver::reactor.
    void handle()
    {
        LOG_FMT_TRACE(log, "stage: {}", stage);
        switch (stage)
        {
        case AsyncRequestStage::WAIT_MAKE_READER:
            if (reader)
            {
                stage = AsyncRequestStage::WAIT_BATCH_READ;
                read_packet_index = 0;
                reader->read(packets[0], thisAsUnaryCallback());
            }
            else
            {
                LOG_FMT_WARNING(log, "MakeReader fail. retry time: {}", retry_times);
                waitForRetryOrDone("Exchange receiver meet error : send async stream request fail");
            }
            break;
        case AsyncRequestStage::WAIT_BATCH_READ:
            LOG_FMT_TRACE(log, "Received {} packets.", read_packet_index);
            if (read_packet_index > 0)
                has_data = true;

            if (auto packet = getErrorPacket())
                setDone("Exchange receiver meet error : " + packet->error().msg());
            else if (!sendPackets())
                setDone("Exchange receiver meet error : push packets fail");
            else if (read_packet_index < batch_packet_count)
            {
                stage = AsyncRequestStage::WAIT_FINISH;
                reader->finish(finish_status, thisAsUnaryCallback());
            }
            else
            {
                read_packet_index = 0;
                reader->read(packets[0], thisAsUnaryCallback());
            }
            break;
        case AsyncRequestStage::WAIT_FINISH:
            if (finish_status.ok())
                setDone("");
            else
            {
                LOG_FMT_WARNING(
                    log,
                    "Finish fail. err code: {}, err msg: {}, retry time {}",
                    finish_status.error_code(),
                    finish_status.error_message(),
                    retry_times);
                waitForRetryOrDone("Exchange receiver meet error : " + finish_status.error_message());
            }
            break;
        default:
            __builtin_unreachable();
        }
    }

    bool finished() const
    {
        return stage == AsyncRequestStage::FINISHED;
    }

    bool waitingForRetry() const
    {
        return stage == AsyncRequestStage::WAIT_RETRY;
    }

    bool retrySucceed(TimePoint now)
    {
        if (now >= expected_retry_ts)
        {
            ++retry_times;
            start();
            return true;
        }
        return false;
    }

    bool meetError() const { return meet_error; }
    const String & getErrMsg() const { return err_msg; }
    const LoggerPtr & getLog() const { return log; }

private:
    void notifyReactor()
    {
        notify_queue->push(this);
    }

    MPPDataPacketPtr getErrorPacket() const
    {
        // only the last packet may has error, see execute().
        if (read_packet_index != 0 && packets[read_packet_index - 1]->has_error())
            return packets[read_packet_index - 1];
        return nullptr;
    }

    bool retriable() const
    {
        return !has_data && retry_times + 1 < max_retry_times;
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
        stage = AsyncRequestStage::WAIT_MAKE_READER;
        // Use lock to ensure async reader is unreachable from grpc thread before this function returns
        std::unique_lock lock(mu);
        rpc_context->makeAsyncReader(*request, reader, thisAsUnaryCallback());
    }

    void waitForRetryOrDone(String done_msg)
    {
        if (retriable())
        {
            expected_retry_ts = Clock::now() + std::chrono::seconds(1);
            stage = AsyncRequestStage::WAIT_RETRY;
            reader.reset();
        }
        else
            setDone(done_msg);
    }

    bool sendPackets()
    {
        for (size_t i = 0; i < read_packet_index; ++i)
        {
            auto & packet = packets[i];
            auto recv_msg = std::make_shared<ReceivedMessage>();
            recv_msg->packet = std::move(packet);
            recv_msg->source_index = request->source_index;
            recv_msg->req_info = req_info;
            if (!msg_channel->push(std::move(recv_msg)))
                return false;
            // can't reuse packet since it is sent to readers.
            packet = std::make_shared<MPPDataPacket>();
        }
        return true;
    }

    // in case of potential multiple inheritances.
    UnaryCallback<bool> * thisAsUnaryCallback()
    {
        return static_cast<UnaryCallback<bool> *>(this);
    }

    std::shared_ptr<RPCContext> rpc_context;
    const Request * request; // won't be null
    MPMCQueue<Self *> * notify_queue; // won't be null
    MPMCQueue<std::shared_ptr<ReceivedMessage>> * msg_channel; // won't be null

    String req_info;
    bool meet_error = false;
    bool has_data = false;
    String err_msg;
    int retry_times = 0;
    TimePoint expected_retry_ts{Clock::duration::zero()};
    AsyncRequestStage stage = AsyncRequestStage::NEED_INIT;

    std::shared_ptr<AsyncReader> reader;
    MPPDataPacketPtrs packets;
    size_t read_packet_index = 0;
    Status finish_status = RPCContext::getStatusOK();
    LoggerPtr log;
    std::mutex mu;
};
} // namespace

template <typename RPCContext>
ExchangeReceiverBase<RPCContext>::ExchangeReceiverBase(
    std::shared_ptr<RPCContext> rpc_context_,
    size_t source_num_,
    size_t max_streams_,
    const String & req_id,
    const String & executor_id)
    : rpc_context(std::move(rpc_context_))
    , source_num(source_num_)
    , max_streams(max_streams_)
    , max_buffer_size(std::max<size_t>(batch_packet_count, std::max(source_num, max_streams_) * 2))
    , thread_manager(newThreadManager())
    , msg_channel(max_buffer_size)
    , live_connections(source_num)
    , state(ExchangeReceiverState::NORMAL)
    , exc_log(Logger::get("ExchangeReceiver", req_id, executor_id))
    , collected(false)
{
    try
    {
        rpc_context->fillSchema(schema);
        setUpConnection();
    }
    catch (...)
    {
        try
        {
            cancel();
            thread_manager->wait();
        }
        catch (...)
        {
            tryLogCurrentException(exc_log, __PRETTY_FUNCTION__);
        }
        throw;
    }
}

template <typename RPCContext>
ExchangeReceiverBase<RPCContext>::~ExchangeReceiverBase()
{
    try
    {
        close();
        thread_manager->wait();
    }
    catch (...)
    {
        tryLogCurrentException(exc_log, __PRETTY_FUNCTION__);
    }
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::cancel()
{
    setEndState(ExchangeReceiverState::CANCELED);
    msg_channel.finish();
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::close()
{
    setEndState(ExchangeReceiverState::CLOSED);
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
        {
            thread_manager->schedule(true, "Receiver", [this, req = std::move(req)] { readLoop(req); });
            ++thread_count;
        }
    }

    // TODO: reduce this thread in the future.
    if (!async_requests.empty())
    {
        thread_manager->schedule(true, "RecvReactor", [this, async_requests = std::move(async_requests)] { reactor(async_requests); });
        ++thread_count;
    }
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::reactor(const std::vector<Request> & async_requests)
{
    using AsyncHandler = AsyncRequestHandler<RPCContext>;

    GET_METRIC(tiflash_thread_count, type_threads_of_receiver_reactor).Increment();
    SCOPE_EXIT({
        GET_METRIC(tiflash_thread_count, type_threads_of_receiver_reactor).Decrement();
    });

    CPUAffinityManager::getInstance().bindSelfQueryThread();

    size_t alive_async_connections = async_requests.size();
    MPMCQueue<AsyncHandler *> ready_requests(alive_async_connections * 2);
    std::vector<AsyncHandler *> waiting_for_retry_requests;

    std::vector<std::unique_ptr<AsyncHandler>> handlers;
    handlers.reserve(alive_async_connections);
    for (const auto & req : async_requests)
        handlers.emplace_back(std::make_unique<AsyncHandler>(&ready_requests, &msg_channel, rpc_context, req, exc_log->identifier()));

    while (alive_async_connections > 0)
    {
        // to avoid waiting_for_retry_requests starvation, so the max continuously popping
        // won't exceed 100ms (check_waiting_requests_freq * timeout) too much.
        static constexpr Int32 check_waiting_requests_freq = 10;
        static constexpr auto timeout = std::chrono::milliseconds(10);

        for (Int32 i = 0; i < check_waiting_requests_freq; ++i)
        {
            AsyncHandler * handler = nullptr;
            if (unlikely(!ready_requests.tryPop(handler, timeout)))
                break;

            handler->handle();
            if (handler->finished())
            {
                --alive_async_connections;
                connectionDone(handler->meetError(), handler->getErrMsg(), handler->getLog());
            }
            else if (handler->waitingForRetry())
            {
                waiting_for_retry_requests.push_back(handler);
            }
            else
            {
                // do nothing
            }
        }
        if (unlikely(!waiting_for_retry_requests.empty()))
        {
            auto now = Clock::now();
            std::vector<AsyncHandler *> tmp;
            for (auto * handler : waiting_for_retry_requests)
            {
                if (!handler->retrySucceed(now))
                    tmp.push_back(handler);
            }
            waiting_for_retry_requests.swap(tmp);
        }
    }
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::readLoop(const Request & req)
{
    GET_METRIC(tiflash_thread_count, type_threads_of_receiver_read_loop).Increment();
    SCOPE_EXIT({
        GET_METRIC(tiflash_thread_count, type_threads_of_receiver_read_loop).Decrement();
    });

    CPUAffinityManager::getInstance().bindSelfQueryThread();
    bool meet_error = false;
    String local_err_msg;
    String req_info = fmt::format("tunnel{}+{}", req.send_task_id, req.recv_task_id);

    LoggerPtr log = Logger::get("ExchangeReceiver", exc_log->identifier(), req_info);

    try
    {
        auto status = RPCContext::getStatusOK();
        for (int i = 0; i < max_retry_times; ++i)
        {
            auto reader = rpc_context->makeReader(req);
            bool has_data = false;
            for (;;)
            {
                LOG_FMT_TRACE(log, "begin next ");
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
                LOG_FMT_DEBUG(log, "finish read : {}", req.debugString());
                break;
            }
            else
            {
                bool retriable = !has_data && i + 1 < max_retry_times;
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
bool ExchangeReceiverBase<RPCContext>::setEndState(ExchangeReceiverState new_state)
{
    assert(new_state == ExchangeReceiverState::CANCELED || new_state == ExchangeReceiverState::CLOSED);
    std::unique_lock lock(mu);
    if (state == ExchangeReceiverState::CANCELED || state == ExchangeReceiverState::CLOSED)
    {
        return false;
    }
    state = new_state;
    return true;
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
    const LoggerPtr & log)
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
