// Copyright 2023 PingCAP, Ltd.
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
#include <Storages/Transaction/TMTContext.h>
#include <fmt/core.h>
#include <grpcpp/completion_queue.h>

#include <cassert>
#include <tuple>

namespace pingcap
{
namespace kv
{
template <>
struct RpcTypeTraits<::mpp::EstablishMPPConnectionRequest>
{
    using RequestType = mpp::EstablishMPPConnectionRequest;
    using ResultType = mpp::MPPDataPacket;
    static std::unique_ptr<grpc::ClientReader<::mpp::MPPDataPacket>> doRPCCall(
        grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client,
        const RequestType & req)
    {
        return client->stub->EstablishMPPConnection(context, req);
    }
    static std::unique_ptr<grpc::ClientAsyncReader<::mpp::MPPDataPacket>> doAsyncRPCCall(
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
namespace FailPoints
{
extern const char random_exception_when_connect_local_tunnel[];
} // namespace FailPoints

namespace
{
struct GrpcExchangePacketReader : public ExchangePacketReader
{
    std::shared_ptr<pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest>> call;
    grpc::ClientContext client_context;
    std::unique_ptr<grpc::ClientReader<mpp::MPPDataPacket>> reader;

    explicit GrpcExchangePacketReader(const ExchangeRecvRequest & req)
    {
        call = std::make_shared<pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest>>(req.req);
    }

    bool read(TrackedMppDataPacketPtr & packet) override
    {
        return packet->read(reader);
    }

    grpc::Status finish() override
    {
        return reader->Finish();
    }

    void cancel(const String &) override {}
};

struct AsyncGrpcExchangePacketReader : public AsyncExchangePacketReader
{
    pingcap::kv::Cluster * cluster;
    const ExchangeRecvRequest & request;
    pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> call;
    grpc::ClientContext client_context;
    grpc::CompletionQueue * cq; // won't be null
    std::unique_ptr<grpc::ClientAsyncReader<::mpp::MPPDataPacket>> reader;

    AsyncGrpcExchangePacketReader(
        pingcap::kv::Cluster * cluster_,
        grpc::CompletionQueue * cq_,
        const ExchangeRecvRequest & req_)
        : cluster(cluster_)
        , request(req_)
        , call(req_.req)
        , cq(cq_)
    {
        assert(cq != nullptr);
    }

    void init(UnaryCallback<bool> * callback) override
    {
        reader = cluster->rpc_client->sendStreamRequestAsync(
            request.req->sender_meta().address(),
            &client_context,
            call,
            *cq,
            callback);
    }

    void read(TrackedMppDataPacketPtr & packet, UnaryCallback<bool> * callback) override
    {
        packet->read(reader, callback);
    }

    void finish(::grpc::Status & status, UnaryCallback<bool> * callback) override
    {
        reader->Finish(&status, callback);
    }
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
    req.req->set_allocated_receiver_meta(new mpp::TaskMeta(task_meta)); // NOLINT
    req.req->set_allocated_sender_meta(sender_task.release()); // NOLINT
    return req;
}

void GRPCReceiverContext::sendMPPTaskToTiFlashStorageNode(
    LoggerPtr log,
    const std::vector<RequestAndRegionIDs> & disaggregated_dispatch_reqs)
{
    if (disaggregated_dispatch_reqs.empty())
        throw Exception("unexpected disaggregated_dispatch_reqs, it's empty.");

    std::shared_ptr<ThreadManager> thread_manager = newThreadManager();
    for (const RequestAndRegionIDs & dispatch_req : disaggregated_dispatch_reqs)
    {
        LOG_DEBUG(log, "tiflash_compute node start to send MPPTask({})", std::get<0>(dispatch_req)->DebugString());
        thread_manager->schedule(/*propagate_memory_tracker=*/false, "", [&dispatch_req, this] {
            // When send req succeed or backoff timeout, need_retry is false.
            bool need_retry = true;
            pingcap::kv::Backoffer bo(pingcap::kv::copNextMaxBackoff);
            while (need_retry)
            {
                try
                {
                    pingcap::kv::RpcCall<mpp::DispatchTaskRequest> rpc_call(std::get<0>(dispatch_req));
                    this->cluster->rpc_client->sendRequest(std::get<0>(dispatch_req)->meta().address(), rpc_call, /*timeout=*/60);
                    need_retry = false;
                    const auto & resp = rpc_call.getResp();
                    if (resp->has_error())
                    {
                        this->setDispatchMPPTaskErrMsg(resp->error().msg());
                        return;
                    }
                    for (const auto & retry_region : resp->retry_regions())
                    {
                        auto region_id = pingcap::kv::RegionVerID(
                            retry_region.id(),
                            retry_region.region_epoch().conf_ver(),
                            retry_region.region_epoch().version());
                        this->cluster->region_cache->dropRegion(region_id);
                    }
                }
                catch (...)
                {
                    std::string local_err_msg = getCurrentExceptionMessage(true);
                    try
                    {
                        bo.backoff(pingcap::kv::boTiFlashRPC, pingcap::Exception(local_err_msg));
                    }
                    catch (...)
                    {
                        need_retry = false;
                        this->setDispatchMPPTaskErrMsg(local_err_msg);
                        this->cluster->region_cache->onSendReqFailForBatchRegions(std::get<1>(dispatch_req), std::get<2>(dispatch_req));
                    }
                }
            }
        });
    }

    thread_manager->wait();

    // No need to lock, because all concurrent threads are done.
    if (!dispatch_mpp_task_err_msg.empty())
        throw Exception(dispatch_mpp_task_err_msg);
}

void GRPCReceiverContext::setDispatchMPPTaskErrMsg(const std::string & err)
{
    std::lock_guard<std::mutex> lock(dispatch_mpp_task_err_msg_mu);
    // Only record first dispatch_mpp_task_err_msg.
    if (dispatch_mpp_task_err_msg.empty())
    {
        dispatch_mpp_task_err_msg = err;
    }
}

void GRPCReceiverContext::cancelMPPTaskOnTiFlashStorageNode(LoggerPtr log)
{
    auto sender_task_size = exchange_receiver_meta.encoded_task_meta_size();
    auto thread_manager = newThreadManager();
    for (auto i = 0; i < sender_task_size; ++i)
    {
        auto sender_task = std::make_unique<mpp::TaskMeta>();
        if (unlikely(!sender_task->ParseFromString(exchange_receiver_meta.encoded_task_meta(i))))
        {
            LOG_WARNING(log, "parse exchange_receiver_meta.encoded_task_meta failed when canceling MPPTask on tiflash_storage node, will ignore this error");
            return;
        }
        auto cancel_req = std::make_shared<mpp::CancelTaskRequest>();
        cancel_req->set_allocated_meta(sender_task.release());
        auto rpc_call = std::make_shared<pingcap::kv::RpcCall<mpp::CancelTaskRequest>>(cancel_req);
        thread_manager->schedule(/*propagate_memory_tracker=*/false, "", [cancel_req, log, this] {
            try
            {
                auto rpc_call = pingcap::kv::RpcCall<mpp::CancelTaskRequest>(cancel_req);
                // No need to retry.
                this->cluster->rpc_client->sendRequest(cancel_req->meta().address(), rpc_call, /*timeout=*/30);
                const auto & resp = rpc_call.getResp();
                if (resp->has_error())
                    throw Exception(resp->error().msg());
            }
            catch (...)
            {
                String cancel_err_msg = getCurrentExceptionMessage(true);
                LOG_WARNING(log, "cancel MPPTasks on tiflash_storage nodes failed: {}. will ignore this error", cancel_err_msg);
            }
        });
    }
    thread_manager->wait();
}

bool GRPCReceiverContext::supportAsync(const ExchangeRecvRequest & request) const
{
    return enable_async_grpc && !request.is_local;
}

void GRPCReceiverContext::establishMPPConnectionLocalV2(
    const ExchangeRecvRequest & request,
    size_t source_index,
    LocalRequestHandler & local_request_handler,
    bool is_fine_grained,
    bool has_remote_conn)
{
    RUNTIME_CHECK_MSG(request.is_local, "This should be a local request");

    auto [tunnel, err_msg] = task_manager->findTunnelWithTimeout(request.req.get(), std::chrono::seconds(10));
    checkLocalTunnel(tunnel, err_msg);
    tunnel->connectLocalV2(source_index, local_request_handler, is_fine_grained, has_remote_conn);
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
        return std::make_tuple(nullptr, grpc::Status(grpc::StatusCode::INTERNAL, "EstablishMPPConnectionLocal into a remote channel!"));
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

    /// put the implementation of dtor in .cpp so we don't need to put the specialization of
    /// pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> in header file.
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
        auto [tunnel, status] = establishMPPConnectionLocalV1(request.req.get(), task_manager);
        if (!status.ok())
        {
            throw Exception("Exchange receiver meet error : " + status.error_message());
        }
        return std::make_shared<LocalExchangePacketReader>(tunnel->getLocalTunnelSenderV1());
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

ExchangePacketReaderPtr GRPCReceiverContext::makeSyncReader(const ExchangeRecvRequest & request) const
{
    auto reader = std::make_shared<GrpcExchangePacketReader>(request);
    reader->reader = cluster->rpc_client->sendStreamRequest(
        request.req->sender_meta().address(),
        &reader->client_context,
        *reader->call);
    return reader;
}

void GRPCReceiverContext::makeAsyncReader(
    const ExchangeRecvRequest & request,
    AsyncExchangePacketReaderPtr & reader,
    grpc::CompletionQueue * cq,
    UnaryCallback<bool> * callback) const
{
    reader = std::make_shared<AsyncGrpcExchangePacketReader>(cluster, cq, request);
    reader->init(callback);
}

void GRPCReceiverContext::fillSchema(DAGSchema & schema) const
{
    schema.clear();
    for (int i = 0; i < exchange_receiver_meta.field_types_size(); ++i)
    {
        String name = genNameForExchangeReceiver(i);
        ColumnInfo info = TiDB::fieldTypeToColumnInfo(exchange_receiver_meta.field_types(i));
        schema.emplace_back(std::move(name), std::move(info));
    }
}

String ExchangeRecvRequest::debugString() const
{
    return req->DebugString();
}
} // namespace DB
