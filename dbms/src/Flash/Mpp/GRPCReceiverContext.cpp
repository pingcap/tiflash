#include <Common/Exception.h>
#include <Flash/Mpp/GRPCCompletionQueuePool.h>
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
namespace
{
using BoolPromise = boost::fibers::promise<bool>; 
using SizePromise = boost::fibers::promise<size_t>; 

struct PromiseCallback : public GRPCCompletionQueuePool::Callback
{
    BoolPromise promise;

    void run(bool ok) override
    {
        promise.set_value(ok);
        delete this;
    }
};

struct BatchReadCallback : public GRPCCompletionQueuePool::Callback
{
    SizePromise promise;
    size_t read_index = 0;
    const std::vector<mpp::MPPDataPacket *> & packets;
    ::grpc::ClientAsyncReader<::mpp::MPPDataPacket> & reader;

    BatchReadCallback(
        const std::vector<mpp::MPPDataPacket *> & packets_,
        ::grpc::ClientAsyncReader<::mpp::MPPDataPacket> & reader_)
        : packets(packets_)
        , reader(reader_)
    {
    }

    void run(bool ok) override
    {
        if (!ok)
        {
            promise.set_value(read_index);
            delete this;
            return;
        }
        ++read_index;
        if (read_index == packets.size())
        {
            promise.set_value(read_index);
            delete this;
            return;
        }
        reader.Read(packets[read_index], this);
    }
};
} // namespace

GRPCReceiverContext::GRPCReceiverContext(pingcap::kv::Cluster * cluster_)
    : cluster(cluster_)
    , log(&Poco::Logger::get("GRPCReceiverContext"))
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
    auto callback = std::make_unique<PromiseCallback>();
    auto future = callback->promise.get_future();
    auto reader = std::make_shared<Reader>(request);
    reader->log = log;
    reader->reader = cluster->rpc_client->sendStreamRequestAsync(
        request.req->sender_meta().address(),
        &reader->client_context,
        *reader->call,
        GRPCCompletionQueuePool::Instance()->pickQueue(),
        callback.release());

    future.wait();
    auto res = future.get();
    if (!res)
        throw Exception("Send async stream request fail");

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
}

bool GRPCReceiverContext::Reader::read(mpp::MPPDataPacket * packet) const
{
    auto callback = std::make_unique<PromiseCallback>();
    auto future = callback->promise.get_future();
    reader->Read(packet, callback.release());

    future.wait();
    auto res = future.get();
    return res;
}

size_t GRPCReceiverContext::Reader::batchRead(const std::vector<mpp::MPPDataPacket *> & packets) const
{
    auto callback = std::make_unique<BatchReadCallback>(packets, *reader);
    auto future = callback->promise.get_future();
    reader->Read(packets[0], callback.release());

    future.wait();
    return future.get();
}

GRPCReceiverContext::StatusType GRPCReceiverContext::Reader::finish() const
{
    auto callback = std::make_unique<PromiseCallback>();
    auto future = callback->promise.get_future();
    auto status = getStatusOK();
    reader->Finish(&status, callback.release());

    future.wait();
    return status;
}

} // namespace DB
