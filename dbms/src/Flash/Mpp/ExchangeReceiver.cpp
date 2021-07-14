#include <Flash/Mpp/ExchangeReceiver.h>

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
        grpc::ClientContext * context, std::shared_ptr<KvConnClient> client, const RequestType & req)
    {
        return client->stub->EstablishMPPConnection(context, req);
    }
    static std::unique_ptr<::grpc::ClientAsyncReader<::mpp::MPPDataPacket>> doAsyncRPCCall(grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client, const RequestType & req, grpc::CompletionQueue & cq, void * call)
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
        live_connections++;
        workers.push_back(std::move(t));
    }
}

void ExchangeReceiver::ReadLoop(const String & meta_raw, size_t source_index)
{
    bool meet_error = false;
    try
    {
        auto sender_task = new mpp::TaskMeta();
        sender_task->ParseFromString(meta_raw);
        auto req = std::make_shared<mpp::EstablishMPPConnectionRequest>();
        req->set_allocated_receiver_meta(new mpp::TaskMeta(task_meta));
        req->set_allocated_sender_meta(sender_task);
        LOG_DEBUG(log, "begin start and read : " << req->DebugString());
        pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> call(req);
        grpc::ClientContext client_context;
        auto reader = cluster->rpc_client->sendStreamRequest(req->sender_meta().address(), &client_context, call);
        reader->WaitForInitialMetadata();
        String req_info = "tunnel" + std::to_string(sender_task->task_id()) + "+" + std::to_string(task_meta.task_id());
        // Block until the next result is available in the completion queue "cq".
        for (;;)
        {
            LOG_TRACE(log, "begin next ");
            std::shared_ptr<ReceivedPacket> packet;
            empty_packets.pop(packet);
            packet->req_info = &req_info;
            packet->source_index = source_index;
            bool success = reader->Read(packet->packet.get());
            if (!success)
                break;
            if (packet->packet->has_error())
            {
                throw Exception("exchange receiver meet error : " + packet->packet->error().msg());
            }
            if (state == NORMAL)
            {
                full_packets.push(packet);
            }
            else
            {
                LOG_WARNING(log, "Receiver's status is not normal, exit from ReadLoop");
                break;
            }
        }
        LOG_DEBUG(log, "finish read : " << req->DebugString());
    }
    catch (Exception & e)
    {
        meet_error = true;
        err = e;
    }
    catch (std::exception & e)
    {
        meet_error = true;
        err = Exception(e.what());
    }
    catch (...)
    {
        meet_error = true;
        err = Exception("fatal error");
    }
    std::lock_guard<std::mutex> lock(mu);
    live_connections--;
    if (meet_error && state == NORMAL)
    {
        state = ERROR;
    }
    if (live_connections == 0)
    {
        if (state == NORMAL)
        {
            /// in normal case, notify each stream to stop by sending it a nullptr
            for (size_t i = 0; i < max_streams; ++i)
            {
                full_packets.push(nullptr);
            }
        }
        else
        {
            /// in error case, prevent a stream from being blocked when pop a packet
            while ( full_packets.size() < max_streams)
            {
                full_packets.push(nullptr);
            }
        }
    }
    cv.notify_all();
    LOG_DEBUG(log, "read thread end!!! live connections: " << std::to_string(live_connections));
}

ExchangeReceiverResult ExchangeReceiver::nextResult()
{
    ExchangeReceiverResult result;
    if (state != NORMAL)
    {
        String msg;
        if (state == CANCELED)
            msg = "query canceled";
        else if (state == CLOSED)
            msg = "ExchangeReceiver closed";
        else
            msg = err.message();
        result = {nullptr, 0, "ExchangeReceiver", true, msg, false};
    }
    else if (full_packets.size()==0 && live_connections==0)
    {
        result = {nullptr, 0, "ExchangeReceiver", false, "", true};
    }
    else
    {
        std::shared_ptr<ReceivedPacket> packet;
        full_packets.pop(packet);
        if (packet != nullptr)
        {
            std::shared_ptr<tipb::SelectResponse> resp_ptr = std::make_shared<tipb::SelectResponse>();
            if (!resp_ptr->ParseFromString(packet->packet->data()))
            {
                result = {nullptr, 0, "ExchangeReceiver", true, "decode error", false};
            }
            else
            {
                result = {resp_ptr, packet->source_index, *packet->req_info};
                packet->packet->Clear();
                empty_packets.push(std::move(packet));
            }
        }
        else if (packet == nullptr)
        {
            if(live_connections > 0)
                result = {nullptr, 0, "ExchangeReceiver", true, "unknown errors", false};
            else
                result = {nullptr, 0, "ExchangeReceiver", false, "", true};
        }
    }
    return result;
}

} // namespace DB
