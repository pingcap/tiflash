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

void ExchangeReceiver::init()
{
    std::lock_guard<std::mutex> lk(mu);
    if (inited)
    {
        return;
    }
    for (auto & meta : pb_exchange_receiver.encoded_task_meta())
    {
        std::thread t(&ExchangeReceiver::ReadLoop, this, std::ref(meta));
        live_connections++;
        workers.push_back(std::move(t));
    }
    inited = true;
}

void ExchangeReceiver::ReadLoop(const String & meta_raw)
{
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
        auto reader = context.getCluster()->rpc_client->sendStreamRequest(req->sender_meta().address(), &client_context, call);
        reader->WaitForInitialMetadata();
        // Block until the next result is available in the completion queue "cq".
        mpp::MPPDataPacket packet;
        for (;;)
        {
            LOG_TRACE(log, "begin next ");
            bool success = reader->Read(&packet);
            if (!success)
                break;
            if (packet.has_error())
            {
                throw Exception("exchange receiver meet error : " + packet.error().msg());
            }
            decodePacket(packet);
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
    cv.notify_all();
    LOG_DEBUG(log, "read thread end!!! live connections: " << std::to_string(live_connections));
}

} // namespace DB
