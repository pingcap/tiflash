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
    , max_buffer_size((max_streams + source_num) * 5) // to reduce contention
    , live_connections(pb_exchange_receiver.encoded_task_meta_size())
    , state(ExchangeReceiverState::NORMAL)
    , received_packets(max_buffer_size)
    , empty_received_packets(max_buffer_size)
    , log(getMPPTaskLog(log_, "ExchangeReceiver"))
{
    for (int i = 0; i < exc.field_types_size(); i++)
    {
        String name = "exchange_receiver_" + std::to_string(i);
        ColumnInfo info = TiDB::fieldTypeToColumnInfo(exc.field_types(i));
        schema.push_back(std::make_pair(name, info));
    }

    for (size_t i = 0; i < max_buffer_size; ++i)
        empty_received_packets.push(std::make_unique<ReceivedPacket>());

    setUpConnection();
}

template <typename RPCContext>
ExchangeReceiverBase<RPCContext>::~ExchangeReceiverBase()
{
    cancel();

    for (auto & worker : workers)
    {
        worker.join();
    }
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::cancel()
{
    std::unique_lock<std::mutex> lk(mu);
    state = ExchangeReceiverState::CANCELED;
    received_packets.cancel();
    empty_received_packets.cancel();
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::setUpConnection()
{
    for (size_t index = 0; index < source_num; ++index)
        workers.emplace_back(&ExchangeReceiverBase::readLoop, this, index);
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
            bool has_data = false;
            for (;;)
            {
                LOG_TRACE(log, "begin next ");
                auto pop_res = empty_received_packets.pop();
                if (!pop_res.has_value() || !pop_res.value())
                {
			meet_error = true;
                        local_err_msg = "receiver's state is " + getReceiverStateStr(getState()) + ", exit from ReadLoop";
                        LOG_WARNING(log, local_err_msg);
                        break;
                }
                std::unique_ptr<ReceivedPacket> packet = std::move(pop_res.value());
                packet->req_info = req_info;
                packet->source_index = source_index;
                bool success = reader->read(&packet->packet);
                if (!success)
                    break;
                else
                    has_data = true;
                if (packet->packet.has_error())
                {
                    throw Exception("Exchange receiver meet error : " + packet->packet.error().msg());
                }
                bool push_res = received_packets.push(std::move(packet));
                if (!push_res)
                {
                    meet_error = true;
		    local_err_msg = "receiver's state is " + getReceiverStateStr(getState()) + ", exit from ReadLoop";
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
        std::unique_lock<std::mutex> lock(mu);
        live_connections--;
        if (unlikely(meet_error && state == ExchangeReceiverState::NORMAL))
            state = ExchangeReceiverState::ERROR;
        if (unlikely(meet_error && err_msg.empty()))
            err_msg = local_err_msg;
        copy_live_conn = live_connections;
    }
    LOG_DEBUG(log, fmt::format("{} -> {} end! current alive connections: {}", send_task_id, recv_task_id, copy_live_conn));

    if (meet_error)
    {
        received_packets.cancel();
    }
    else if (copy_live_conn == 0)
    {
        LOG_DEBUG(log, fmt::format("All threads end in ExchangeReceiver"));
        received_packets.finish();
    }
    else if (copy_live_conn < 0)
        throw Exception("live_connections should not be less than 0!");
}

template <typename RPCContext>
ExchangeReceiverResult ExchangeReceiverBase<RPCContext>::nextResult()
{
    auto res = received_packets.pop();
    if (!res.has_value())
    {
        String msg;
        std::unique_lock lock(mu);
        if (state == ExchangeReceiverState::NORMAL)
            return {nullptr, 0, "ExchangeReceiver", false, "", true};

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
    std::unique_ptr<ReceivedPacket> packet = std::move(res.value());
    assert(packet != nullptr);
    ExchangeReceiverResult result;
    if (packet->packet.has_error())
    {
        result = {nullptr, packet->source_index, packet->req_info, true, packet->packet.error().msg(), false};
    }
    else
    {
        auto resp_ptr = std::make_shared<tipb::SelectResponse>();
        if (!resp_ptr->ParseFromString(packet->packet.data()))
        {
            result = {nullptr, packet->source_index, packet->req_info, true, "decode error", false};
        }
        else
        {
            result = {resp_ptr, packet->source_index, packet->req_info};
        }
    }
    packet->packet.Clear();
    empty_received_packets.push(std::move(packet));
    return result;
}

template <typename RPCContext>
ExchangeReceiverState ExchangeReceiverBase<RPCContext>::getState() const
{
    std::unique_lock lk(mu);
    return state;
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class ExchangeReceiverBase<GRPCReceiverContext>;

} // namespace DB
