#include <Common/Exception.h>
#include <Flash/Mpp/GRPCReceiverContext.h>

#include <tuple>

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
    static std::unique_ptr<::grpc::ClientAsyncReader<::mpp::MPPDataPacket>> doAsyncRPCCall(
        grpc::ClientContext * context,
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
GRPCReceiverContext::GRPCReceiverContext(pingcap::kv::Cluster * cluster_, std::shared_ptr<MPPTaskManager> task_manager_)
    : cluster(cluster_)
    , task_manager(task_manager_)
{}


GRPCReceiverContext::Request GRPCReceiverContext::makeRequest(
    int index,
    const tipb::ExchangeReceiver & pb_exchange_receiver,
    const ::mpp::TaskMeta & task_meta) const
{
    const auto & meta_raw = pb_exchange_receiver.encoded_task_meta(index);
    auto sender_task = std::make_unique<mpp::TaskMeta>();
    if (!sender_task->ParseFromString(meta_raw))
        throw Exception("parse task meta error!");

    Request req;
    req.send_task_id = sender_task->task_id();
    req.req = std::make_shared<mpp::EstablishMPPConnectionRequest>();
    req.req->set_allocated_receiver_meta(new mpp::TaskMeta(task_meta));
    req.req->set_allocated_sender_meta(sender_task.release());
    return req;
}

std::tuple<MPPTunnelPtr, grpc::Status> EstablishMPPConnectionLocal(const ::mpp::EstablishMPPConnectionRequest * request, const std::shared_ptr<MPPTaskManager> & task_manager)
{
    std::chrono::seconds timeout(10);
    std::string err_msg;
    MPPTunnelPtr tunnel = nullptr;
    {
        MPPTaskPtr sender_task = task_manager->findTaskWithTimeout(request->sender_meta(), timeout, err_msg);
        if (sender_task != nullptr)
        {
            std::tie(tunnel, err_msg) = sender_task->getTunnel(request);
        }
        if (tunnel == nullptr)
        {
            return std::make_tuple(tunnel, grpc::Status(grpc::StatusCode::INTERNAL, err_msg));
        }
    }
    if (!tunnel->isLocal())
    {
        std::string err_msg("EstablishMPPConnectionLocal into a remote channel !");
        return std::make_tuple(nullptr, grpc::Status(grpc::StatusCode::INTERNAL, err_msg));
    }
    tunnel->connect(nullptr);
    return std::make_tuple(tunnel, grpc::Status::OK);
}

std::shared_ptr<GRPCReceiverContext::Reader> GRPCReceiverContext::makeReader(const GRPCReceiverContext::Request & request, bool is_local) const
{
    if (is_local)
    {
        std::tuple<MPPTunnelPtr, grpc::Status> localConnRetPair = EstablishMPPConnectionLocal(request.req.get(), task_manager);
        MPPTunnelPtr tunnel = std::get<0>(localConnRetPair);
        grpc::Status status = std::get<1>(localConnRetPair);
        if (!status.ok())
        {
            throw Exception("Exchange receiver meet error : " + status.error_message());
        }
        LocalEnv local_env(tunnel);
        return std::make_shared<Reader>(local_env);
    }
    else
    {
        auto reader = std::make_shared<Reader>(request);
        reader->reader = cluster->rpc_client->sendStreamRequest(
            request.req->sender_meta().address(),
            &reader->client_context,
            *reader->call);
        return reader;
    }
}

String GRPCReceiverContext::Request::debugString() const
{
    return req->DebugString();
}


GRPCReceiverContext::Reader::Reader(const GRPCReceiverContext::Request & req)
{
    is_local = false;
    call = std::make_shared<pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest>>(req.req);
}

GRPCReceiverContext::Reader::~Reader()
{}

void GRPCReceiverContext::Reader::initialize() const
{
    if (!is_local)
    {
        reader->WaitForInitialMetadata();
    }
}

bool GRPCReceiverContext::Reader::read(std::shared_ptr<mpp::MPPDataPacket> & packet) const
{
    if (is_local)
    {
        std::shared_ptr<mpp::MPPDataPacket> tmp_packet = local_env.tunnel->readForLocal();
        bool success = tmp_packet != nullptr;
        if (success)
            packet = tmp_packet;
        return success;
    }
    else
    {
        return reader->Read(packet.get());
    }
}

GRPCReceiverContext::StatusType GRPCReceiverContext::Reader::finish() const
{
    if (is_local)
    {
        return ::grpc::Status::OK;
    }
    else
    {
        return reader->Finish();
    }
}

} // namespace DB
