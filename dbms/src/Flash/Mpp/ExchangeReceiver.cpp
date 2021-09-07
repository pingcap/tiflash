#include <Flash/Mpp/ExchangeReceiver.h>
#include <fmt/core.h>

namespace pingcap
{
namespace kv
{
template <>
struct RpcTypeTraits<::mpp::EstablishMPPConnectionRequest>
{
    using RequestType = ::mpp::EstablishMPPConnectionRequest;
    using ResultType = ::mpp::MPPDataPacket;
    static std::unique_ptr<::grpc::ClientReader<::mpp::MPPDataPacket>> doRPCCall(
        grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client,
        const RequestType & req)
    {
        return client->stub->EstablishMPPConnection(context, req);
    }
    static std::unique_ptr<::grpc::ClientAsyncReader<::mpp::MPPDataPacket>> doAsyncRPCCall(grpc::ClientContext * context,
                                                                                           std::shared_ptr<KvConnClient> client,
                                                                                           const RequestType & req,
                                                                                           grpc::CompletionQueue & cq,
                                                                                           void * call)
    {
        return client->stub->AsyncEstablishMPPConnection(context, req, &cq, call);
    }
};

} // namespace kv
} // namespace pingcap

namespace DB
{
void ExchangeReceiver::setUpConnection()
{
    for (int index = 0; index < pb_exchange_receiver.encoded_task_meta_size(); index++)
    {
        auto & meta = pb_exchange_receiver.encoded_task_meta(index);
        std::thread t(&ExchangeReceiver::ReadLoop, this, std::ref(meta), index);
        workers.push_back(std::move(t));
    }
}

void ExchangeReceiver::ReadLoop(const String & meta_raw, size_t source_index)
{
    bool meet_error = false;
    String local_err_msg;

    Int64 send_task_id = -1;
    Int64 recv_task_id = task_meta.task_id();

    try
    {
        auto sender_task = new mpp::TaskMeta();
        send_task_id = sender_task->task_id();
        sender_task->ParseFromString(meta_raw);
        auto req = std::make_shared<mpp::EstablishMPPConnectionRequest>();
        req->set_allocated_receiver_meta(new mpp::TaskMeta(task_meta));
        req->set_allocated_sender_meta(sender_task);
        LOG_DEBUG(log, "begin start and read : " << req->DebugString());
        ::grpc::Status status = ::grpc::Status::OK;
        for (int i = 0; i < 10; i++)
        {
            pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> call(req);
            grpc::ClientContext client_context;
            auto reader = cluster->rpc_client->sendStreamRequest(req->sender_meta().address(), &client_context, call);
            reader->WaitForInitialMetadata();
            std::shared_ptr<ReceivedPacket> packet;
            String req_info = "tunnel" + std::to_string(send_task_id) + "+" + std::to_string(recv_task_id);
            bool has_data = false;
            for (;;)
            {
                LOG_TRACE(log, "begin next ");
                empty_packets.pop(packet);
                packet->req_info = req_info;
                packet->source_index = source_index;
                bool success = reader->Read(packet->packet.get());
                if (!success)
                    break;
                else
                    has_data = true;
                if (packet->packet->has_error())
                {
                    throw Exception("Exchange receiver meet error : " + packet->packet->error().msg());
                }

                std::unique_lock<std::mutex> lock(mu);
                cv.wait(lock, [&] { return !full_packets.isFull() || state != NORMAL; });

                if (state == NORMAL)
                {
                    full_packets.push(packet);
                    cv.notify_all();
                }
                else
                {
                    meet_error = true;
                    local_err_msg = "Decode packet meet error";
                    LOG_WARNING(log, "Decode packet meet error, exit from ReadLoop");
                    break;
                }
            }
            // if meet error, such as decode packect fails, it will not retry.
            if (meet_error)
            {
                break;
            }
            status = reader->Finish();
            if (status.ok())
            {
                LOG_DEBUG(log, "finish read : " << req->DebugString());
                break;
            }
            else
            {
                LOG_WARNING(log,
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
    std::lock_guard<std::mutex> lock(mu);
    live_connections--;

    // avoid concurrent conflict
    Int32 live_conn_copy = live_connections;

    if (meet_error && state == NORMAL)
        state = ERROR;
    if (meet_error && err_msg.empty())
        err_msg = local_err_msg;
    cv.notify_all();

    LOG_DEBUG(log, fmt::format("{} -> {} end! current alive connections: {}", send_task_id, recv_task_id, live_conn_copy));

    if (live_conn_copy == 0)
        LOG_DEBUG(log, fmt::format("All threads end in ExchangeReceiver"));
}

ExchangeReceiverResult ExchangeReceiver::nextResult()
{
    std::unique_lock<std::mutex> lock(mu);
    cv.wait(lock, [&] { return !full_packets.isEmpty() || live_connections == 0 || state != NORMAL; });

    ExchangeReceiverResult result;
    if (state != NORMAL)
    {
        String msg;
        if (state == CANCELED)
            msg = "query canceled";
        else if (state == CLOSED)
            msg = "ExchangeReceiver closed";
        else if (!err_msg.empty())
            msg = err_msg;
        else
            msg = "Unknown error";
        result = {nullptr, 0, "ExchangeReceiver", true, msg, false};
    }
    else if (!full_packets.isEmpty())
    {
        std::shared_ptr<ReceivedPacket> packet;
        full_packets.pop(packet);

        if (packet != nullptr)
        {
            if (packet->packet->has_error())
            {
                result = {nullptr, 0, "ExchangeReceiver", true, packet->packet->error().msg(), false};
            }
            else if (packet->packet != nullptr)
            {
                auto resp_ptr = std::make_shared<tipb::SelectResponse>();
                if (!resp_ptr->ParseFromString(packet->packet->data()))
                {
                    result = {nullptr, 0, "ExchangeReceiver", true, "decode error", false};
                }
                else
                {
                    result = {resp_ptr, packet->source_index, packet->req_info};
                    packet->packet->Clear();
                    empty_packets.push(std::move(packet));
                }
            }
            else
            {
                result = {nullptr, 0, "ExchangeReceiver", true, "unknown errors", false};
            }
        }
        else
        {
            result = {nullptr, 0, "ExchangeReceiver", true, "packet is null", false};
        }
    }
    else
    {
        if (live_connections > 0)
            result = {nullptr, 0, "ExchangeReceiver", true, "unknown errors", false};
        else
            result = {nullptr, 0, "ExchangeReceiver", false, "", true};
    }

    cv.notify_all();
    return result;
}

} // namespace DB
