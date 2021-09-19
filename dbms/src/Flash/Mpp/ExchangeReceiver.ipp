#pragma once

#include <fmt/core.h>

namespace DB
{
template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::setUpConnection()
{
    for (size_t index = 0; index < source_num; ++index)
        workers.emplace_back(&ExchangeReceiverBase::ReadLoop, this, index);
}

inline String getReceiverStateStr(const ExchangeReceiverState & s)
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
void ExchangeReceiverBase<RPCContext>::ReadLoop(size_t source_index)
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
                {
                    std::unique_lock<std::mutex> lock(mu);
                    cv.wait(lock, [&] { return res_buffer.hasEmpty() || state != ExchangeReceiverState::NORMAL; });
                    if (state == ExchangeReceiverState::NORMAL)
                    {
                        res_buffer.popEmpty(packet);
                        cv.notify_all();
                    }
                    else
                    {
                        meet_error = true;
                        local_err_msg = "receiver's state is " + getReceiverStateStr(state) + ", exit from ReadLoop";
                        LOG_WARNING(log, local_err_msg);
                        break;
                    }
                }
                packet->req_info = req_info;
                packet->source_index = source_index;
                bool success = reader->read(packet->packet.get());
                if (!success)
                    break;
                else
                    has_data = true;
                if (packet->packet->has_error())
                {
                    throw Exception("Exchange receiver meet error : " + packet->packet->error().msg());
                }
                {
                    std::unique_lock<std::mutex> lock(mu);
                    cv.wait(lock, [&] { return res_buffer.canPush() || state != ExchangeReceiverState::NORMAL; });
                    if (state == ExchangeReceiverState::NORMAL)
                    {
                        res_buffer.pushObject(packet);
                        cv.notify_all();
                    }
                    else
                    {
                        meet_error = true;
                        local_err_msg = "receiver's state is " + getReceiverStateStr(state) + ", exit from ReadLoop";
                        LOG_WARNING(log, local_err_msg);
                        break;
                    }
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
        if (meet_error && state == ExchangeReceiverState::NORMAL)
            state = ExchangeReceiverState::ERROR;
        if (meet_error && err_msg.empty())
            err_msg = local_err_msg;
        copy_live_conn = live_connections;
        cv.notify_all();
    }
    LOG_DEBUG(log, fmt::format("{} -> {} end! current alive connections: {}", send_task_id, recv_task_id, copy_live_conn));

    if (copy_live_conn == 0)
        LOG_DEBUG(log, fmt::format("All threads end in ExchangeReceiver"));
    else if (copy_live_conn < 0)
        throw Exception("live_connections should not be less than 0!");
}

template <typename RPCContext>
ExchangeReceiverResult ExchangeReceiverBase<RPCContext>::nextResult()
{
    std::shared_ptr<ReceivedPacket> packet;
    {
        std::unique_lock<std::mutex> lock(mu);
        cv.wait(lock, [&] { return res_buffer.hasObjects() || live_connections == 0 || state != ExchangeReceiverState::NORMAL; });

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
        else if (res_buffer.hasObjects())
        {
            res_buffer.popObject(packet);
            cv.notify_all();
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
    std::unique_lock<std::mutex> lock(mu);
    cv.wait(lock, [&] { return res_buffer.canPushEmpty(); });
    res_buffer.pushEmpty(std::move(packet));
    cv.notify_all();
    return result;
}

} // namespace DB
