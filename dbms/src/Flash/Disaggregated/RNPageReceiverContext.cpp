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
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Disaggregated/RNPageReceiverContext.h>
#include <Flash/Mpp/GRPCCompletionQueuePool.h>
#include <Storages/DeltaMerge/Remote/RNRemoteReadTask.h>
#include <Storages/Transaction/TMTContext.h>
#include <grpcpp/completion_queue.h>
#include <kvproto/disaggregated.pb.h>

#include <cassert>
#include <tuple>

namespace pingcap::kv
{
template <>
struct RpcTypeTraits<disaggregated::FetchDisaggPagesRequest>
{
    using RequestType = disaggregated::FetchDisaggPagesRequest;
    using ResultType = disaggregated::PagesPacket;
    static std::unique_ptr<grpc::ClientReader<disaggregated::PagesPacket>> doRPCCall(
        grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client,
        const RequestType & req)
    {
        return client->stub->FetchDisaggPages(context, req);
    }
    static std::unique_ptr<grpc::ClientAsyncReader<disaggregated::PagesPacket>> doAsyncRPCCall(
        grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client,
        const RequestType & req,
        grpc::CompletionQueue & cq,
        void * call)
    {
        return client->stub->AsyncFetchDisaggPages(context, req, &cq, call);
    }
};

} // namespace pingcap::kv

namespace DB
{
namespace
{
struct FetchPagesStreamReader : public PagePacketReader
{
    std::shared_ptr<pingcap::kv::RpcCall<disaggregated::FetchDisaggPagesRequest>> call;
    grpc::ClientContext client_context;
    std::unique_ptr<grpc::ClientReader<disaggregated::PagesPacket>> reader;

    explicit FetchPagesStreamReader(const FetchPagesRequest & req)
    {
        call = std::make_shared<pingcap::kv::RpcCall<disaggregated::FetchDisaggPagesRequest>>(req.req);
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

} // namespace

GRPCPagesReceiverContext::GRPCPagesReceiverContext(
    const DM::RNRemoteReadTaskPtr & remote_read_tasks_,
    pingcap::kv::Cluster * cluster_)
    : remote_read_tasks(remote_read_tasks_)
    , cluster(cluster_)
{}

FetchPagesRequest::FetchPagesRequest(DM::RNRemoteSegmentReadTaskPtr seg_task_)
    : seg_task(std::move(seg_task_))
    , req(std::make_shared<disaggregated::FetchDisaggPagesRequest>())
{
    // Invalid task, just skip
    if (!seg_task)
        return;

    *req->mutable_snapshot_id() = seg_task->snapshot_id.toMeta();
    req->set_table_id(seg_task->table_id);
    req->set_segment_id(seg_task->segment_id);
    for (auto page_id : seg_task->pendingPageIds())
    {
        req->add_page_ids(page_id);
    }
}

const String & FetchPagesRequest::address() const
{
    assert(seg_task != nullptr);
    return seg_task->address;
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

void GRPCPagesReceiverContext::finishTaskReceive(const DM::RNRemoteSegmentReadTaskPtr & seg_task)
{
    remote_read_tasks->updateTaskState(seg_task, DM::SegmentReadTaskState::DataReady, false);
}

void GRPCPagesReceiverContext::finishAllReceivingTasks(const String & err_msg)
{
    remote_read_tasks->allDataReceive(err_msg);
}

void GRPCPagesReceiverContext::cancelMPPTaskOnTiFlashStorageNode(LoggerPtr /*log*/)
{
    // TODO cancel
}

ExchangePagePacketReaderPtr GRPCPagesReceiverContext::makeReader(const Request & request) const
{
    auto reader = std::make_shared<FetchPagesStreamReader>(request);
    reader->reader = cluster->rpc_client->sendStreamRequest(
        request.address(),
        &reader->client_context,
        *reader->call);
    return reader;
}

String FetchPagesRequest::debugString() const
{
    return req->ShortDebugString();
}
} // namespace DB
