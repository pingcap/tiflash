#include <Common/CPUAffinityManager.h>
#include <Common/ThreadFactory.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <fmt/core.h>

namespace DB
{
template <typename RPCContext>
ExchangeReceiverBase<RPCContext>::ExchangeReceiverBase(
    std::shared_ptr<RPCContext> rpc_context_,
    const ::tipb::ExchangeReceiver & exc,
    const ::mpp::TaskMeta & meta,
    size_t max_streams_,
    const std::shared_ptr<LogWithPrefix> & log_)
    : rpc_context(std::move(rpc_context_))
    , pb_exchange_receiver(exc)
    , source_num(pb_exchange_receiver.encoded_task_meta_size())
    , task_meta(meta)
    , max_streams(max_streams_)
    , max_buffer_size(1024)
    , empty_buffer(max_buffer_size)
    , full_buffer(max_buffer_size)
    , live_connections(pb_exchange_receiver.encoded_task_meta_size())
    , state(ExchangeReceiverState::NORMAL)
    , log(getMPPTaskLog(log_, "ExchangeReceiver"))
{
    for (int i = 0; i < exc.field_types_size(); i++)
    {
        String name = "exchange_receiver_" + std::to_string(i);
        ColumnInfo info = TiDB::fieldTypeToColumnInfo(exc.field_types(i));
        schema.push_back(std::make_pair(name, info));
    }

    LOG_DEBUG(log, "initial push empty packets");
    while (empty_buffer.try_push(std::make_shared<ReceivedPacket>()) == boost::fibers::channel_op_status::success)
    {
        /// do nothing
    }

    setUpConnection();
}

template <typename RPCContext>
ExchangeReceiverBase<RPCContext>::~ExchangeReceiverBase()
{
    {
        std::unique_lock lk(mu);
        state = ExchangeReceiverState::CLOSED;
    }

    empty_buffer.close();
    full_buffer.close();

    for (size_t i = 0; i < source_num; ++i)
        workers_done[i].wait();
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::cancel()
{
    {
        std::unique_lock lk(mu);
        state = ExchangeReceiverState::CANCELED;
    }
    empty_buffer.close();
    full_buffer.close();
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::setUpConnection()
{
    LOG_DEBUG(log, "setUpConnection");
    for (size_t index = 0; index < source_num; ++index)
        workers_done.emplace_back(DefaultFiberPool::submit_job(&ExchangeReceiverBase<RPCContext>::readLoop, this, index).value());
}

static inline String getReceiverStateStr(const ExchangeReceiverState & s)
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

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::readLoop(size_t source_index)
{
    bool meet_error = false;
    String local_err_msg;

    Int64 send_task_id = -1;
    Int64 recv_task_id = task_meta.task_id();

    try
    {
        auto req = rpc_context->makeRequest(source_index, pb_exchange_receiver, task_meta);
        send_task_id = req.send_task_id;
        String req_info = "tunnel" + std::to_string(send_task_id) + "+" + std::to_string(recv_task_id);
        LOG_DEBUG(log, "begin start and read : " << req.debugString());
        auto status = RPCContext::getStatusOK();
        for (int i = 0; i < 10; i++)
        {
            auto reader = rpc_context->makeReader(req);
            reader->initialize();
            std::shared_ptr<ReceivedPacket> packet;
            bool has_data = false;
            for (;;)
            {
                LOG_TRACE(log, "begin next ");
                auto res = empty_buffer.pop(packet);
                if (res != boost::fibers::channel_op_status::success)
                {
                    std::unique_lock lock(mu);
                    assert(state != ExchangeReceiverState::NORMAL);
                    meet_error = true;
                    local_err_msg = "receiver's state is " + getReceiverStateStr(state) + ", exit from readLoop";
                    LOG_WARNING(log, local_err_msg);
                    break;
                }
                LOG_DEBUG(log, "got one empty packet");
                packet->req_info = req_info;
                packet->source_index = source_index;
                bool success = reader->read(packet->packet.get());
                if (!success)
                    break;
                else
                    has_data = true;
                LOG_DEBUG(log, "read one packet");
                if (packet->packet->has_error())
                {
                    throw Exception("Exchange receiver meet error : " + packet->packet->error().msg());
                }
                res = full_buffer.push(std::move(packet));
                if (res != boost::fibers::channel_op_status::success)
                {
                    std::unique_lock lock(mu);
                    assert(state != ExchangeReceiverState::NORMAL);
                    meet_error = true;
                    local_err_msg = "receiver's state is " + getReceiverStateStr(state) + ", exit from readLoop";
                    LOG_WARNING(log, local_err_msg);
                    break;
                }
                LOG_DEBUG(log, "pushed one full packet");
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
                LOG_WARNING(
                    log,
                    "EstablishMPPConnectionRequest meets rpc fail. Err msg is: " << status.error_message() << " req info " << req_info);
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
    Int32 copy_live_conn = -1;
    {
        std::unique_lock lock(mu);
        live_connections--;
        if (meet_error && state == ExchangeReceiverState::NORMAL)
            state = ExchangeReceiverState::ERROR;
        if (meet_error && err_msg.empty())
            err_msg = local_err_msg;
        copy_live_conn = live_connections;
    }
    LOG_DEBUG(log, fmt::format("{} -> {} end! current alive connections: {}", send_task_id, recv_task_id, copy_live_conn));

    if (meet_error || copy_live_conn == 0)
    {
        empty_buffer.close();
        full_buffer.close();
    }
    else if (copy_live_conn < 0)
        throw Exception("live_connections should not be less than 0!");
}

template <typename RPCContext>
ExchangeReceiverResult ExchangeReceiverBase<RPCContext>::nextResult()
{
    std::shared_ptr<ReceivedPacket> packet;
    auto res = full_buffer.pop(packet);
    if (res != boost::fibers::channel_op_status::success)
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
        else /// live_connections == 0, res_buffer is empty, and state is NORMAL, that is the end.
        {
            return {nullptr, 0, "ExchangeReceiver", false, "", true};
        }
    }
    assert(packet != nullptr && packet->packet != nullptr);
    ExchangeReceiverResult result;
    if (packet->packet->has_error())
    {
        result = {nullptr, packet->source_index, packet->req_info, true, packet->packet->error().msg(), false};
    }
    else
    {
        auto resp_ptr = std::make_shared<tipb::SelectResponse>();
        if (!resp_ptr->ParseFromString(packet->packet->data()))
        {
            result = {nullptr, packet->source_index, packet->req_info, true, "decode error", false};
        }
        else
        {
            result = {resp_ptr, packet->source_index, packet->req_info};
        }
    }
    packet->packet->Clear();
    empty_buffer.push(std::move(packet));
    return result;
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class ExchangeReceiverBase<GRPCReceiverContext>;

} // namespace DB
