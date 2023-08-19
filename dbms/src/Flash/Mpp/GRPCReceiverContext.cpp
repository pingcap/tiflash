// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
struct GrpcExchangePacketReader : public ExchangePacketReader
{
    std::shared_ptr<pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest>> call;
    grpc::ClientContext client_context;
    std::unique_ptr<::grpc::ClientReader<::mpp::MPPDataPacket>> reader;

    explicit GrpcExchangePacketReader(const ExchangeRecvRequest & req)
    {
        call = std::make_shared<pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest>>(req.req);
    }

    bool read(MPPDataPacketPtr & packet) override
    {
        return reader->Read(packet.get());
    }

    ::grpc::Status finish() override
    {
        return reader->Finish();
    }
};

struct AsyncGrpcExchangePacketReader : public AsyncExchangePacketReader
{
    pingcap::kv::Cluster * cluster;
    const ExchangeRecvRequest & request;
    pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> call;
    grpc::ClientContext client_context;
    std::unique_ptr<::grpc::ClientAsyncReader<::mpp::MPPDataPacket>> reader;

    AsyncGrpcExchangePacketReader(
        pingcap::kv::Cluster * cluster_,
        const ExchangeRecvRequest & req)
        : cluster(cluster_)
        , request(req)
        , call(req.req)
    {
    }

    void init(UnaryCallback<bool> * callback) override
    {
        reader = cluster->rpc_client->sendStreamRequestAsync(
            request.req->sender_meta().address(),
            &client_context,
            call,
            GRPCCompletionQueuePool::global_instance->pickQueue(),
            callback);
    }

    void read(MPPDataPacketPtr & packet, UnaryCallback<bool> * callback) override
    {
        reader->Read(packet.get(), callback);
    }

    void finish(::grpc::Status & status, UnaryCallback<bool> * callback) override
    {
        reader->Finish(&status, callback);
    }
};

struct LocalExchangePacketReader : public ExchangePacketReader
{
    MPPTunnelPtr tunnel;

    explicit LocalExchangePacketReader(const std::shared_ptr<MPPTunnel> & tunnel_)
        : tunnel(tunnel_)
    {}

    /// put the implementation of dtor in .cpp so we don't need to put the specialization of
    /// pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> in header file.
    ~LocalExchangePacketReader() override
    {
        if (tunnel)
        {
            // In case that ExchangeReceiver throw error before finish reading from mpp_tunnel
            tunnel->consumerFinish("Receiver closed");
        }
    }

    bool read(MPPDataPacketPtr & packet) override
    {
        MPPDataPacketPtr tmp_packet = tunnel->readForLocal();
        bool success = tmp_packet != nullptr;
        if (success)
            packet = tmp_packet;
        return success;
    }

    ::grpc::Status finish() override
    {
        tunnel.reset();
        return ::grpc::Status::OK;
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

bool GRPCReceiverContext::supportAsync(const ExchangeRecvRequest & request) const
{
    return enable_async_grpc && !request.is_local;
}

ExchangePacketReaderPtr GRPCReceiverContext::makeReader(const ExchangeRecvRequest & request) const
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
    AsyncExchangePacketReaderPtr & reader,
    UnaryCallback<bool> * callback) const
{
    reader = std::make_shared<AsyncGrpcExchangePacketReader>(cluster, request);
    reader->init(callback);
}

void GRPCReceiverContext::fillSchema(DAGSchema & schema) const
{
    schema.clear();
    for (int i = 0; i < exchange_receiver_meta.field_types_size(); ++i)
    {
        String name = "exchange_receiver_" + std::to_string(i);
        ColumnInfo info = TiDB::fieldTypeToColumnInfo(exchange_receiver_meta.field_types(i));
        schema.emplace_back(std::move(name), std::move(info));
    }
}

String ExchangeRecvRequest::debugString() const
{
    return req->DebugString();
}
} // namespace DB
