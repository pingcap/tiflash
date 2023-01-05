// Copyright 2022 PingCAP, Ltd.
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
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Disaggregated/GRPCPageReceiverContext.h>
#include <Flash/Mpp/GRPCCompletionQueuePool.h>
#include <Storages/DeltaMerge/Remote/RemoteReadTask.h>
#include <Storages/Transaction/TMTContext.h>
#include <grpcpp/completion_queue.h>
#include <kvproto/mpp.pb.h>

#include <cassert>
#include <tuple>

namespace pingcap
{
namespace kv
{
template <>
struct RpcTypeTraits<::mpp::FetchDisaggregatedPagesRequest>
{
    using RequestType = mpp::FetchDisaggregatedPagesRequest;
    using ResultType = mpp::PagesPacket;
    static std::unique_ptr<grpc::ClientReader<::mpp::PagesPacket>> doRPCCall(
        grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client,
        const RequestType & req)
    {
        return client->stub->FetchDisaggregatedPages(context, req);
    }
    static std::unique_ptr<grpc::ClientAsyncReader<::mpp::PagesPacket>> doAsyncRPCCall(
        grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client,
        const RequestType & req,
        grpc::CompletionQueue & cq,
        void * call)
    {
        return client->stub->AsyncFetchDisaggregatedPages(context, req, &cq, call);
    }
};

} // namespace kv
} // namespace pingcap

namespace DB
{
namespace
{
struct GrpcExchangePacketReader : public ExchangePagePacketReader
{
    std::shared_ptr<pingcap::kv::RpcCall<mpp::FetchDisaggregatedPagesRequest>> call;
    grpc::ClientContext client_context;
    std::unique_ptr<grpc::ClientReader<mpp::PagesPacket>> reader;

    explicit GrpcExchangePacketReader(const FetchPagesRequest & req)
    {
        call = std::make_shared<pingcap::kv::RpcCall<mpp::FetchDisaggregatedPagesRequest>>(req.req);
    }

    bool read(TrackedPageDataPacketPtr & packet) override
    {
        return reader->Read(packet.get());
    }

    grpc::Status finish() override
    {
        return reader->Finish();
    }

    void cancel(const String &) override {}
};

struct AsyncGrpcExchangePacketReader : public AsyncExchangePagePacketReader
{
    pingcap::kv::Cluster * cluster;
    const FetchPagesRequest & request;
    pingcap::kv::RpcCall<mpp::FetchDisaggregatedPagesRequest> call;
    grpc::ClientContext client_context;
    grpc::CompletionQueue * cq; // won't be null
    std::unique_ptr<grpc::ClientAsyncReader<::mpp::PagesPacket>> reader;

    AsyncGrpcExchangePacketReader(
        pingcap::kv::Cluster * cluster_,
        grpc::CompletionQueue * cq_,
        const FetchPagesRequest & req_)
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
            request.req->address(),
            &client_context,
            call,
            *cq,
            callback);
    }

    void read(TrackedPageDataPacketPtr & packet, UnaryCallback<bool> * callback) override
    {
        reader->Read(packet.get(), callback);
    }

    void finish(::grpc::Status & status, UnaryCallback<bool> * callback) override
    {
        reader->Finish(&status, callback);
    }
};

} // namespace

GRPCPagesReceiverContext::GRPCPagesReceiverContext(
    const DM::RemoteReadTaskPtr & remote_read_tasks_,
    pingcap::kv::Cluster * cluster_,
    bool enable_async_grpc_)
    : remote_read_tasks(remote_read_tasks_)
    , cluster(cluster_)
    , enable_async_grpc(enable_async_grpc_)
{}

FetchPagesRequest::FetchPagesRequest(DM::RemoteSegmentReadTaskPtr seg_task_)
    : seg_task(std::move(seg_task_))
    , req(std::make_shared<mpp::FetchDisaggregatedPagesRequest>())
{
    // Invalid task, just skip
    if (!seg_task)
        return;

    req->set_address(seg_task->address);
    req->set_lease(60); // 60 seconds

    *req->mutable_meta() = seg_task->snapshot_id.toMeta();
    req->set_table_id(seg_task->table_id);
    req->set_segment_id(seg_task->segment_id);
    for (auto page_id : seg_task->pendingPageIds())
    {
        req->add_pages(page_id);
    }
}

GRPCPagesReceiverContext::Request GRPCPagesReceiverContext::popRequest() const
{
    auto seg_task = remote_read_tasks->nextFetchTask();
    return Request(std::move(seg_task));
}

void GRPCPagesReceiverContext::finishTaskEstablish(const Request & req, bool meet_error)
{
    remote_read_tasks->updateTaskState(req.seg_task, DM::SegmentReadTaskState::Receiving, meet_error);
}

void GRPCPagesReceiverContext::finishTaskReceive(const DM::RemoteSegmentReadTaskPtr & seg_task)
{
    remote_read_tasks->updateTaskState(seg_task, DM::SegmentReadTaskState::AllReady, false);
}

void GRPCPagesReceiverContext::finishAllReceivingTasks(const String & err_msg)
{
    remote_read_tasks->allDataReceive(err_msg);
}

void GRPCPagesReceiverContext::cancelMPPTaskOnTiFlashStorageNode(LoggerPtr /*log*/)
{
    // TODO cancel
}

bool GRPCPagesReceiverContext::supportAsync(const Request & /*request*/) const
{
    return enable_async_grpc;
}

ExchangePagePacketReaderPtr GRPCPagesReceiverContext::makeReader(const Request & request) const
{
    auto reader = std::make_shared<GrpcExchangePacketReader>(request);
    reader->reader = cluster->rpc_client->sendStreamRequest(
        request.req->address(),
        &reader->client_context,
        *reader->call);
    return reader;
}

void GRPCPagesReceiverContext::makeAsyncReader(
    const Request & request,
    AsyncExchangePagePacketReaderPtr & reader,
    grpc::CompletionQueue * cq,
    UnaryCallback<bool> * callback) const
{
    reader = std::make_shared<AsyncGrpcExchangePacketReader>(cluster, cq, request);
    reader->init(callback);
}

String FetchPagesRequest::debugString() const
{
    return req->ShortDebugString();
}
} // namespace DB
