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
#include <Common/FailPoint.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Mpp/GRPCCompletionQueuePool.h>
#include <Flash/Mpp/GRPCReceiverContext.h>
#include <fmt/core.h>
#include <grpcpp/completion_queue.h>

#include <cassert>
#include <tuple>
#include "Flash/Statistics/ConnectionProfileInfo.h"

namespace DB
{
namespace FailPoints
{
extern const char random_exception_when_connect_local_tunnel[];
} // namespace FailPoints

namespace
{

using RpcCallEstablishMPPConnection = pingcap::kv::RpcCall<pingcap::kv::RPC_NAME(EstablishMPPConnection)>;
using RpcCallAsyncEstablishMPPConnection = pingcap::kv::RpcCall<pingcap::kv::RPC_NAME(AsyncEstablishMPPConnection)>;

struct GrpcExchangePacketReader : public ExchangePacketReader
{
    grpc::ClientContext client_context;
    std::unique_ptr<grpc::ClientReader<mpp::MPPDataPacket>> reader;

    GrpcExchangePacketReader() = default;

    bool read(TrackedMppDataPacketPtr & packet) override { return packet->read(reader); }

    grpc::Status finish() override { return reader->Finish(); }

    void cancel(const String &) override {}
};

struct AsyncGrpcExchangePacketReader : public AsyncExchangePacketReader
{
    pingcap::kv::Cluster * cluster;
    const ExchangeRecvRequest & request;
    grpc::ClientContext client_context;
    grpc::CompletionQueue * cq; // won't be null
    std::unique_ptr<grpc::ClientAsyncReader<::mpp::MPPDataPacket>> reader;

    AsyncGrpcExchangePacketReader(
        pingcap::kv::Cluster * cluster_,
        grpc::CompletionQueue * cq_,
        const ExchangeRecvRequest & req_)
        : cluster(cluster_)
        , request(req_)
        , cq(cq_)
    {
        assert(cq != nullptr);
    }

    void init(GRPCKickTag * tag) override
    {
        RpcCallAsyncEstablishMPPConnection rpc(cluster->rpc_client, request.req.sender_meta().address());
        reader = rpc.call(&client_context, request.req, cq, tag);
    }

    void read(TrackedMppDataPacketPtr & packet, GRPCKickTag * tag) override { packet->read(reader, tag); }

    void finish(::grpc::Status & status, GRPCKickTag * tag) override { reader->Finish(&status, tag); }

    grpc::ClientContext * getClientContext() override { return &client_context; }
};

void checkLocalTunnel(const MPPTunnelPtr & tunnel, const String & err_msg)
{
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_exception_when_connect_local_tunnel);
    RUNTIME_CHECK_MSG(tunnel != nullptr, fmt::runtime(err_msg));
    RUNTIME_CHECK_MSG(tunnel->isLocal(), "Need a local tunnel, but get remote tunnel.");
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
{
    conn_type_vec.resize(exchange_receiver_meta.encoded_task_meta_size(), ConnectionProfileInfo::Local);
}

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
    req.req.set_allocated_receiver_meta(new mpp::TaskMeta(task_meta)); // NOLINT
    req.req.set_allocated_sender_meta(sender_task.release()); // NOLINT

    bool valid_zone_flag = exchange_receiver_meta.same_zone_flag_size() == exchange_receiver_meta.encoded_task_meta_size();
    if likely (valid_zone_flag) {
        conn_type_vec[index] = ConnectionProfileInfo::inferConnectionType(req.is_local, exchange_receiver_meta.same_zone_flag().Get(index));
    } else {
        conn_type_vec[index] = ConnectionProfileInfo::inferConnectionType(req.is_local, true);
    }
    return req;
}

bool GRPCReceiverContext::supportAsync(const ExchangeRecvRequest & request) const
{
    return enable_async_grpc && !request.is_local;
}

void GRPCReceiverContext::establishMPPConnectionLocalV2(
    const ExchangeRecvRequest & request,
    size_t source_index,
    LocalRequestHandler & local_request_handler,
    bool has_remote_conn)
{
    RUNTIME_CHECK_MSG(request.is_local, "This should be a local request");

    auto [tunnel, err_msg] = task_manager->findTunnelWithTimeout(&request.req, std::chrono::seconds(10));
    checkLocalTunnel(tunnel, err_msg);
    local_request_handler.recordWaitingTaskTime();
    tunnel->connectLocalV2(source_index, local_request_handler, has_remote_conn);
}

// TODO remove it in the future
std::tuple<MPPTunnelPtr, grpc::Status> GRPCReceiverContext::establishMPPConnectionLocalV1(
    const ::mpp::EstablishMPPConnectionRequest * request,
    const std::shared_ptr<MPPTaskManager> & task_manager)
{
    std::chrono::seconds timeout(10);
    auto [tunnel, err_msg] = task_manager->findTunnelWithTimeout(request, timeout);
    if (tunnel == nullptr)
    {
        return std::make_tuple(tunnel, grpc::Status(grpc::StatusCode::INTERNAL, err_msg));
    }
    if (!tunnel->isLocal())
    {
        return std::make_tuple(
            nullptr,
            grpc::Status(grpc::StatusCode::INTERNAL, "EstablishMPPConnectionLocal into a remote channel!"));
    }
    tunnel->connectLocalV1(nullptr);
    return std::make_tuple(tunnel, grpc::Status::OK);
}

// TODO remove it in the future
struct LocalExchangePacketReader : public ExchangePacketReader
{
    LocalTunnelSenderV1Ptr local_tunnel_sender;

    explicit LocalExchangePacketReader(const LocalTunnelSenderV1Ptr & local_tunnel_sender_)
        : local_tunnel_sender(local_tunnel_sender_)
    {}

    ~LocalExchangePacketReader() override
    {
        if (local_tunnel_sender)
        {
            // In case that ExchangeReceiver throw error before finish reading from mpp_tunnel
            local_tunnel_sender->consumerFinish("Receiver exists");
            local_tunnel_sender.reset();
        }
    }

    bool read(TrackedMppDataPacketPtr & packet) override
    {
        TrackedMppDataPacketPtr tmp_packet = local_tunnel_sender->readForLocal();
        bool success = tmp_packet != nullptr;
        if (success)
            packet = tmp_packet;
        return success;
    }

    void cancel(const String & reason) override
    {
        if (local_tunnel_sender)
        {
            local_tunnel_sender->consumerFinish(fmt::format("Receiver cancelled, reason: {}", reason));
            local_tunnel_sender.reset();
        }
    }

    grpc::Status finish() override
    {
        if (local_tunnel_sender)
        {
            local_tunnel_sender->consumerFinish("Receiver finished!");
            local_tunnel_sender.reset();
        }
        return ::grpc::Status::OK;
    }
};

// TODO remove it in the future
ExchangePacketReaderPtr GRPCReceiverContext::makeReader(const ExchangeRecvRequest & request) const
{
    if (request.is_local)
    {
        auto [tunnel, status] = establishMPPConnectionLocalV1(&request.req, task_manager);
        if (!status.ok())
        {
            throw Exception("Exchange receiver meet error : " + status.error_message());
        }
        return std::make_unique<LocalExchangePacketReader>(tunnel->getLocalTunnelSenderV1());
    }
    else
    {
        RpcCallEstablishMPPConnection rpc(cluster->rpc_client, request.req.sender_meta().address());
        auto reader = std::make_unique<GrpcExchangePacketReader>();
        reader->reader = rpc.call(&reader->client_context, request.req);
        return reader;
    }
}

ExchangePacketReaderPtr GRPCReceiverContext::makeSyncReader(const ExchangeRecvRequest & request) const
{
    RpcCallEstablishMPPConnection rpc(cluster->rpc_client, request.req.sender_meta().address());
    auto reader = std::make_unique<GrpcExchangePacketReader>();
    reader->reader = rpc.call(&reader->client_context, request.req);
    return reader;
}

AsyncExchangePacketReaderPtr GRPCReceiverContext::makeAsyncReader(
    const ExchangeRecvRequest & request,
    grpc::CompletionQueue * cq,
    GRPCKickTag * tag) const
{
    auto reader = std::make_unique<AsyncGrpcExchangePacketReader>(cluster, cq, request);
    reader->init(tag);
    return reader;
}

void GRPCReceiverContext::fillSchema(DAGSchema & schema) const
{
    schema.clear();
    for (int i = 0; i < exchange_receiver_meta.field_types_size(); ++i)
    {
        String name = genNameForExchangeReceiver(i);
        TiDB::ColumnInfo info = TiDB::fieldTypeToColumnInfo(exchange_receiver_meta.field_types(i));
        schema.emplace_back(std::move(name), std::move(info));
    }
}

String ExchangeRecvRequest::debugString() const
{
    return req.DebugString();
}
} // namespace DB
