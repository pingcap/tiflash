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
#include <Common/TiFlashMetrics.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Disaggregated/RNPageReceiverContext.h>
#include <Flash/Mpp/GRPCCompletionQueuePool.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/Remote/RNLocalPageCache.h>
#include <Storages/DeltaMerge/Remote/RNRemoteReadTask.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/Types.h>
#include <grpcpp/completion_queue.h>
#include <kvproto/disaggregated.pb.h>
#include <kvproto/kvrpcpb.pb.h>

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
struct GRPCFetchPagesResponseReader : public FetchPagesResponseReader
{
    std::shared_ptr<pingcap::kv::RpcCall<disaggregated::FetchDisaggPagesRequest>> call;
    grpc::ClientContext client_context;
    std::unique_ptr<grpc::ClientReader<disaggregated::PagesPacket>> reader;

    explicit GRPCFetchPagesResponseReader(const FetchPagesRequest & req)
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

    auto meta = seg_task->snapshot_id.toMeta();
    // The keyspace_id here is not vital, as we locate the table and segment by given
    // snapshot_id. But it could be helpful for debugging.
    auto keyspace_id = seg_task->ks_table_id.first;
    meta.set_keyspace_id(keyspace_id);
    meta.set_api_version(keyspace_id == NullspaceID ? kvrpcpb::APIVersion::V1 : kvrpcpb::APIVersion::V2);
    *req->mutable_snapshot_id() = meta;
    req->set_table_id(seg_task->ks_table_id.second);
    req->set_segment_id(seg_task->segment_id);

    {
        std::vector<DM::Remote::PageOID> cf_tiny_oids;
        cf_tiny_oids.reserve(seg_task->delta_tinycf_page_ids.size());
        for (const auto & page_id : seg_task->delta_tinycf_page_ids)
        {
            auto page_oid = DM::Remote::PageOID{
                .store_id = seg_task->store_id,
                .ks_table_id = seg_task->ks_table_id,
                .page_id = page_id,
            };
            cf_tiny_oids.emplace_back(page_oid);
        }

        // Note: We must occupySpace segment by segment, because we need to read
        // at least the complete data of one segment in order to drive everything forward.
        // Currently we call occupySpace for each FetchPagesRequest, which is fine,
        // because we send one request each seg_task. If we want to split
        // FetchPagesRequest into multiples in future, then we need to change
        // the moment of calling `occupySpace`.
        auto page_cache = seg_task->dm_context->db_context.getSharedContextDisagg()->rn_page_cache;

        Stopwatch w_occupy;
        auto occupy_result = page_cache->occupySpace(cf_tiny_oids, seg_task->delta_tinycf_page_sizes);
        GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_cache_occupy).Observe(w_occupy.elapsedSeconds());

        for (auto page_id : occupy_result.pages_not_in_cache)
            req->add_page_ids(page_id.page_id);

        auto cftiny_total = seg_task->delta_tinycf_page_ids.size();
        auto cftiny_fetch = occupy_result.pages_not_in_cache.size();
        LOG_INFO(
            Logger::get(),
            "read task local cache hit rate: {}, pages_not_in_cache={}",
            cftiny_total == 0 ? "N/A" : fmt::format("{:.2f}%", 100.0 - 100.0 * cftiny_fetch / cftiny_total),
            occupy_result.pages_not_in_cache);
        GET_METRIC(tiflash_disaggregated_details, type_cftiny_read).Increment(cftiny_total);
        GET_METRIC(tiflash_disaggregated_details, type_cftiny_fetch).Increment(cftiny_fetch);

        seg_task->initColumnFileDataProvider(occupy_result.pages_guard);
    }
}

const String & FetchPagesRequest::address() const
{
    assert(seg_task != nullptr);
    return seg_task->address;
}

FetchPagesRequest GRPCPagesReceiverContext::nextFetchPagesRequest() const
{
    auto seg_task = remote_read_tasks->nextFetchTask();
    return FetchPagesRequest(std::move(seg_task));
}

void GRPCPagesReceiverContext::finishTaskEstablish(const FetchPagesRequest & req, bool meet_error)
{
    remote_read_tasks->updateTaskState(req.seg_task, DM::SegmentReadTaskState::Receiving, meet_error);
}

void GRPCPagesReceiverContext::finishTaskReceive(const DM::RNRemoteSegmentReadTaskPtr & seg_task)
{
    remote_read_tasks->updateTaskState(seg_task, DM::SegmentReadTaskState::DataReady, false);
}

void GRPCPagesReceiverContext::cancelDisaggTaskOnTiFlashStorageNode(LoggerPtr /*log*/)
{
    // TODO cancel
}

FetchPagesResponseReaderPtr GRPCPagesReceiverContext::doRequest(const FetchPagesRequest & request) const
{
    auto reader = std::make_shared<GRPCFetchPagesResponseReader>(request);
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
