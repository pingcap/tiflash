#include <Common/Exception.h>
#include <Flash/Mpp/GRPCCompletionQueuePool.h>
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
namespace
{
struct MakeReaderCallbackProxy : public UnaryCallback<bool>
{
    UnaryCallback<std::unique_ptr<ExchangePacketReader>> * callback = nullptr;
    std::unique_ptr<ExchangePacketReader> reader;

    void execute(bool & ok) override
    {
        if (!ok)
            reader.reset();
        callback->execute(reader);
        delete this;
    }
};

struct BatchReadCallbackProxy : public UnaryCallback<bool>
{
    UnaryCallback<size_t> * callback = nullptr;
    size_t read_index = 0;
    MPPDataPacketPtrs * packets = nullptr;
    ::grpc::ClientAsyncReader<MPPDataPacket> * reader = nullptr;

    void execute(bool & ok) override
    {
        if (!ok || ++read_index == packets->size())
        {
            callback->execute(read_index);
            delete this;
            return;
        }
        reader->Read((*packets)[read_index].get(), this);
    }
};

struct FinishCallbackProxy : public UnaryCallback<bool>
{
    UnaryCallback<::grpc::Status> * callback = nullptr;
    ::grpc::Status status = ::grpc::Status::OK;

    void execute(bool &) override
    {
        callback->execute(status);
        delete this;
    }
};

std::tuple<MPPTunnelPtr, grpc::Status> establishMPPConnectionLocal(
    const ::mpp::EstablishMPPConnectionRequest * request,
    const std::shared_ptr<MPPTaskManager> & task_manager)
{
    std::chrono::seconds timeout(10);
    String err_msg;
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
        String err_msg("EstablishMPPConnectionLocal into a remote channel !");
        return std::make_tuple(nullptr, grpc::Status(grpc::StatusCode::INTERNAL, err_msg));
    }
    tunnel->connect(nullptr);
    return std::make_tuple(tunnel, grpc::Status::OK);
}
} // namespace

GRPCReceiverContext::GRPCReceiverContext(
    const tipb::ExchangeReceiver & exchange_receiver_meta_,
    const mpp::TaskMeta & task_meta_,
    pingcap::kv::Cluster * cluster_,
    std::shared_ptr<MPPTaskManager> task_manager_,
    bool enable_local_tunnel_,
    bool enable_async_grpc_)
    : exchange_receiver_meta(exchange_receiver_meta_)
    , task_meta(task_meta_)
    , cluster(cluster_)
    , task_manager(std::move(task_manager_))
    , enable_local_tunnel(enable_local_tunnel_)
    , enable_async_grpc(enable_async_grpc_)
{}

ExchangeRecvRequest GRPCReceiverContext::makeRequest(int index) const
{
    const auto & meta_raw = exchange_receiver_meta.encoded_task_meta(index);
    auto sender_task = std::make_unique<mpp::TaskMeta>();
    if (!sender_task->ParseFromString(meta_raw))
        throw Exception("parse task meta error!");

    ExchangeRecvRequest req;
    req.source_index = index;
    req.is_local = enable_local_tunnel && sender_task->address() == task_meta.address();
    req.send_task_id = sender_task->task_id();
    req.recv_task_id = task_meta.task_id();
    req.req = std::make_shared<mpp::EstablishMPPConnectionRequest>();
    req.req->set_allocated_receiver_meta(new mpp::TaskMeta(task_meta));
    req.req->set_allocated_sender_meta(sender_task.release());
    return req;
}

bool GRPCReceiverContext::supportAsync(const ExchangeRecvRequest & request)
{
    return enable_async_grpc && !request.is_local;
}

std::shared_ptr<ExchangePacketReader> GRPCReceiverContext::makeReader(const ExchangeRecvRequest & request) const
{
    if (request.is_local)
    {
        auto [tunnel, status] = establishMPPConnectionLocal(request.req.get(), task_manager);
        if (!status.ok())
        {
            throw Exception("Exchange receiver meet error : " + status.error_message());
        }
        return std::make_shared<LocalExchangePacketReader>(tunnel);
    }
    else
    {
        auto reader = std::make_shared<GrpcExchangePacketReader>(request);
        reader->reader = cluster->rpc_client->sendStreamRequest(
            request.req->sender_meta().address(),
            &reader->client_context,
            *reader->call);
        return reader;
    }
}

void GRPCReceiverContext::makeAsyncReader(
    const ExchangeRecvRequest & request,
    UnaryCallback<std::unique_ptr<ExchangePacketReader>> * callback) const
{
    auto proxy = std::make_unique<MakeReaderCallbackProxy>();
    proxy->callback = callback;

    auto reader = std::make_unique<AsyncGrpcExchangePacketReader>(request); 
    reader->reader = cluster->rpc_client->sendStreamRequestAsync(
        request.req->sender_meta().address(),
        &reader->client_context,
        *reader->call,
        GRPCCompletionQueuePool::Instance()->pickQueue(),
        proxy.get());
    proxy->reader = std::move(reader); 
    proxy.release();
}
    
void GRPCReceiverContext::fillSchema(DAGSchema & schema) const
{
    schema.clear();
    for (int i = 0; i < exchange_receiver_meta.field_types_size(); i++)
    {
        String name = "exchange_receiver_" + std::to_string(i);
        ColumnInfo info = TiDB::fieldTypeToColumnInfo(exchange_receiver_meta.field_types(i));
        schema.push_back(std::make_pair(name, info));
    }
}

String ExchangeRecvRequest::debugString() const
{
    return req->DebugString();
}

GrpcExchangePacketReader::GrpcExchangePacketReader(const ExchangeRecvRequest & req)
{
    call = std::make_shared<pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest>>(req.req);
}

GrpcExchangePacketReader::~GrpcExchangePacketReader()
{}

bool GrpcExchangePacketReader::read(MPPDataPacketPtr & packet) const
{
    return reader->Read(packet.get());
}

void GrpcExchangePacketReader::batchRead(
    MPPDataPacketPtrs &,
    UnaryCallback<size_t> *) const
{
    throw Exception("GrpcExchangePacketReader::batchRead is not suported");
}

::grpc::Status GrpcExchangePacketReader::finish() const
{
    return reader->Finish();
}

void GrpcExchangePacketReader::finish(UnaryCallback<::grpc::Status> *) const
{
    throw Exception("GrpcExchangePacketReader::finish is not suported");
}

AsyncGrpcExchangePacketReader::AsyncGrpcExchangePacketReader(const ExchangeRecvRequest & req)
{
    call = std::make_shared<pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest>>(req.req);
}

AsyncGrpcExchangePacketReader::~AsyncGrpcExchangePacketReader()
{}

bool AsyncGrpcExchangePacketReader::read(MPPDataPacketPtr &) const
{
    throw Exception("AsyncGrpcExchangePacketReader::read is not supported");
}

void AsyncGrpcExchangePacketReader::batchRead(
    MPPDataPacketPtrs & packets,
    UnaryCallback<size_t> * callback) const
{
    auto proxy = std::make_unique<BatchReadCallbackProxy>();
    proxy->callback = callback;
    proxy->packets = &packets;
    proxy->reader = reader.get();
    reader->Read(packets[0].get(), proxy.release());
}

::grpc::Status AsyncGrpcExchangePacketReader::finish() const
{
    throw Exception("AsyncGrpcExchangePacketReader::finish is not supported");
}

void AsyncGrpcExchangePacketReader::finish(UnaryCallback<::grpc::Status> * callback) const
{
    auto proxy = std::make_unique<FinishCallbackProxy>();
    proxy->callback = callback;
    reader->Finish(&proxy->status, proxy.release());
}

bool LocalExchangePacketReader::read(MPPDataPacketPtr & packet) const
{
    MPPDataPacketPtr tmp_packet = tunnel->readForLocal();
    bool success = tmp_packet != nullptr;
    if (success)
        packet = tmp_packet;
    return success;
}

void LocalExchangePacketReader::batchRead(
    MPPDataPacketPtrs &,
    UnaryCallback<size_t> *) const
{
    throw Exception("LocalExchangePacketReader::batchRead is not suported");
}


::grpc::Status LocalExchangePacketReader::finish() const
{
    return ::grpc::Status::OK;
}

void LocalExchangePacketReader::finish(UnaryCallback<::grpc::Status> *) const
{
    throw Exception("LocalExchangePacketReader::finish is not suported");
}
} // namespace DB
