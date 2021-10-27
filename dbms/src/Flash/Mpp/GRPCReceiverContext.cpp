#include <Common/Exception.h>
#include <Flash/Mpp/GRPCReceiverContext.h>

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
GRPCReceiverContext::GRPCReceiverContext(pingcap::kv::Cluster * cluster_)
    : cluster(cluster_)
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

std::shared_ptr<GRPCReceiverContext::Reader> GRPCReceiverContext::makeReader(const GRPCReceiverContext::Request & request) const
{
    auto reader = std::make_shared<Reader>(request);
    reader->reader = cluster->rpc_client->sendStreamRequest(
        request.req->sender_meta().address(),
        &reader->client_context,
        *reader->call);
    return reader;
}

String GRPCReceiverContext::Request::debugString() const
{
    return req->DebugString();
}

GRPCReceiverContext::Reader::Reader(const GRPCReceiverContext::Request & req)
{
    call = std::make_unique<pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest>>(req.req);
}

GRPCReceiverContext::Reader::~Reader()
{}

void GRPCReceiverContext::Reader::initialize() const
{
    // reader->WaitForInitialMetadata();
}

bool GRPCReceiverContext::Reader::read(mpp::MPPDataPacket * packet) const
{
    return reader->Read(packet);
}

GRPCReceiverContext::StatusType GRPCReceiverContext::Reader::finish() const
{
    return reader->Finish();
}

} // namespace DB
