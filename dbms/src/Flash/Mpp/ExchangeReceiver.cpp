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

constexpr Int32 MAX_RETRY_TIMES = 10;
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
    , thread_manager(newThreadManager())
    , msg_channel(max_buffer_size)
    , live_connections(source_num)
    , state(ExchangeReceiverState::NORMAL)
    , exc_log(getMPPTaskLog(log_, "ExchangeReceiver"))
    , collected(false)
{
    rpc_context->fillSchema(schema);
    setUpConnection();
}

template <typename RPCContext>
ExchangeReceiverBase<RPCContext>::~ExchangeReceiverBase()
{
    setState(ExchangeReceiverState::CLOSED);
    msg_channel.finish();
    thread_manager->wait();
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
    if (0 == source_num)
    {
        msg_channel.finish();
    }
    else
    {
        for (size_t index = 0; index < source_num; ++index)
        {
            auto req = rpc_context->makeRequest(index);
            thread_manager->schedule(true, "Receiver", [this, req = std::move(req)] { readLoop(req); });
        }
    }
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::readLoop(const Request & req)
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
