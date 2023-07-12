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
#include <Common/MemoryTracker.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Mpp/GRPCCompletionQueuePool.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <IO/IOThreadPools.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/Remote/ObjectId.h>
#include <Storages/DeltaMerge/Remote/RNLocalPageCache.h>
#include <Storages/DeltaMerge/Remote/RNReadTask.h>
#include <Storages/DeltaMerge/Remote/RNWorkerFetchPages.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <common/logger_useful.h>
#include <kvproto/disaggregated.pb.h>
#include <kvproto/kvrpcpb.pb.h>

#include <ext/scope_guard.h>
#include <magic_enum.hpp>

using namespace std::chrono_literals;

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

namespace DB::DM::Remote
{

RNLocalPageCache::OccupySpaceResult blockingOccupySpaceForTask(const RNReadSegmentTaskPtr & seg_task)
{
    std::vector<PageOID> cf_tiny_oids;
    {
        cf_tiny_oids.reserve(seg_task->meta.delta_tinycf_page_ids.size());
        for (const auto & page_id : seg_task->meta.delta_tinycf_page_ids)
        {
            auto page_oid = PageOID{
                .store_id = seg_task->meta.store_id,
                .ks_table_id = {seg_task->meta.keyspace_id, seg_task->meta.physical_table_id},
                .page_id = page_id,
            };
            cf_tiny_oids.emplace_back(page_oid);
        }
    }

    // Note: We must occupySpace segment by segment, because we need to read
    // at least the complete data of one segment in order to drive everything forward.
    // Currently we call occupySpace for each FetchPagesRequest, which is fine,
    // because we send one request each seg_task. If we want to split
    // FetchPagesRequest into multiples in future, then we need to change
    // the moment of calling `occupySpace`.
    auto page_cache = seg_task->meta.dm_context->db_context.getSharedContextDisagg()->rn_page_cache;

    Stopwatch w_occupy;
    auto occupy_result = page_cache->occupySpace(cf_tiny_oids, seg_task->meta.delta_tinycf_page_sizes);
    // This metric is per-segment.
    GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_cache_occupy).Observe(w_occupy.elapsedSeconds());

    return occupy_result;
}

std::shared_ptr<disaggregated::FetchDisaggPagesRequest> buildFetchPagesRequest(
    const RNReadSegmentTaskPtr & seg_task,
    const std::vector<PageOID> & pages_not_in_cache)
{
    auto req = std::make_shared<disaggregated::FetchDisaggPagesRequest>();
    auto meta = seg_task->meta.snapshot_id.toMeta();
    // The keyspace_id here is not vital, as we locate the table and segment by given
    // snapshot_id. But it could be helpful for debugging.
    auto keyspace_id = seg_task->meta.keyspace_id;
    meta.set_keyspace_id(keyspace_id);
    meta.set_api_version(keyspace_id == NullspaceID ? kvrpcpb::APIVersion::V1 : kvrpcpb::APIVersion::V2);
    *req->mutable_snapshot_id() = meta;
    req->set_table_id(seg_task->meta.physical_table_id);
    req->set_segment_id(seg_task->meta.segment_id);

    for (auto page_id : pages_not_in_cache)
        req->add_page_ids(page_id.page_id);

    return req;
}

RNReadSegmentTaskPtr RNWorkerFetchPages::doWork(const RNReadSegmentTaskPtr & seg_task)
{
    MemoryTrackerSetter setter(true, fetch_pages_mem_tracker.get());
    Stopwatch watch_work{CLOCK_MONOTONIC_COARSE};
    SCOPE_EXIT({
        // This metric is per-segment.
        GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_worker_fetch_page).Observe(watch_work.elapsedSeconds());
    });

    auto occupy_result = blockingOccupySpaceForTask(seg_task);
    auto req = buildFetchPagesRequest(seg_task, occupy_result.pages_not_in_cache);
    {
        auto cftiny_total = seg_task->meta.delta_tinycf_page_ids.size();
        auto cftiny_fetch = occupy_result.pages_not_in_cache.size();
        LOG_DEBUG(
            log,
            "Ready to fetch pages, seg_task={} page_hit_rate={} pages_not_in_cache={}",
            seg_task->info(),
            cftiny_total == 0 ? "N/A" : fmt::format("{:.2f}%", 100.0 - 100.0 * cftiny_fetch / cftiny_total),
            occupy_result.pages_not_in_cache);
        GET_METRIC(tiflash_disaggregated_details, type_cftiny_read).Increment(cftiny_total);
        GET_METRIC(tiflash_disaggregated_details, type_cftiny_fetch).Increment(cftiny_fetch);
    }

    const size_t max_retry_times = 3;
    std::exception_ptr last_exception;

    // TODO: Maybe don't need to re-fetch all pages when retry.
    for (size_t i = 0; i < max_retry_times; ++i)
    {
        try
        {
            doFetchPages(seg_task, req);
            seg_task->initColumnFileDataProvider(occupy_result.pages_guard);

            // We finished fetch all pages for this seg task, just return it for downstream
            // workers. If we have met any errors, page guard will not be persisted.
            return seg_task;
        }
        catch (const pingcap::Exception & e)
        {
            last_exception = std::current_exception();
            LOG_WARNING(
                log,
                "Meet RPC client exception when fetching pages: {}, will be retried. seg_task={}",
                e.displayText(),
                seg_task->info());
            std::this_thread::sleep_for(1s);
        }
    }

    // Still failed after retry...
    RUNTIME_CHECK(last_exception);
    std::rethrow_exception(last_exception);
}

// In order to make network and disk run parallelly,
// `doFetchPages` will receive data pages from WN,
// package these data pages into several `WritePageTask` objects
// and send them to `RNWritePageCachePool` to write into local page cache.
struct WritePageTask
{
    explicit WritePageTask(RNLocalPageCache * page_cache_)
        : page_cache(page_cache_)
    {}
    RNLocalPageCache * page_cache;
    UniversalWriteBatch wb;
    std::list<DM::RemotePb::RemotePage> remote_pages; // Hold the data of wb.
    std::list<MemTrackerWrapper> remote_page_mem_tracker_wrappers; // Hold the memory stat of remote_pages.
};
using WritePageTaskPtr = std::unique_ptr<WritePageTask>;


void RNWorkerFetchPages::doFetchPages(
    const RNReadSegmentTaskPtr & seg_task,
    std::shared_ptr<disaggregated::FetchDisaggPagesRequest> request)
{
    // No page need to be fetched.
    if (request->page_ids_size() == 0)
        return;

    Stopwatch sw_total;
    Stopwatch watch_rpc{CLOCK_MONOTONIC_COARSE};
    bool rpc_is_observed = false;
    double total_write_page_cache_sec = 0.0;

    grpc::ClientContext client_context;
    auto rpc_call = std::make_shared<pingcap::kv::RpcCall<disaggregated::FetchDisaggPagesRequest>>(request);
    auto stream_resp = cluster->rpc_client->sendStreamRequest(
        seg_task->meta.store_address,
        &client_context,
        *rpc_call);

    SCOPE_EXIT({
        // TODO: Not sure whether we really need this. Maybe RAII is already there?
        stream_resp->Finish();
    });

    // Used to verify all pages are fetched.
    std::set<UInt64> remaining_pages_to_fetch;
    for (auto p : request->page_ids())
        remaining_pages_to_fetch.insert(p);

    UInt64 read_stream_ns = 0;
    UInt64 deserialize_page_ns = 0;
    UInt64 schedule_write_page_ns = 0;
    UInt64 packet_count = 0;
    UInt64 task_count = 0;
    UInt64 page_count = request->page_ids_size();

    auto schedule_task = [&task_count, &schedule_write_page_ns](WritePageTaskPtr && write_page_task) {
        task_count += 1;
        auto task = std::make_shared<std::packaged_task<void()>>(
            [write_page_task = std::move(write_page_task)]() {
                write_page_task->page_cache->write(std::move(write_page_task->wb));
            });
        Stopwatch sw;
        RNWritePageCachePool::get().scheduleOrThrowOnError([task]() { (*task)(); });
        schedule_write_page_ns += sw.elapsed();
        return task->get_future();
    };

    WritePageTaskPtr write_page_task;
    std::vector<std::future<void>> write_page_results;

    // Keep reading packets.
    while (true)
    {
        Stopwatch sw_packet;
        auto packet = std::make_shared<disaggregated::PagesPacket>();
        if (bool more = stream_resp->Read(packet.get()); !more)
            break;

        MemTrackerWrapper packet_mem_tracker_wrapper(packet->SpaceUsedLong(), fetch_pages_mem_tracker.get());

        read_stream_ns += sw_packet.elapsedFromLastTime();
        packet_count += 1;
        if (!rpc_is_observed)
        {
            // Count RPC time as sending request + receive first response packet.
            rpc_is_observed = true;
            // This metric is per-segment, because we only count once for each task.
            GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_rpc_fetch_page).Observe(watch_rpc.elapsedSeconds());
        }

        if (packet->has_error())
        {
            throw Exception(fmt::format("{} (from {})", packet->error().msg(), seg_task->info()));
        }

        Stopwatch watch_write_page_cache{CLOCK_MONOTONIC_COARSE};
        SCOPE_EXIT({
            total_write_page_cache_sec += watch_write_page_cache.elapsedSeconds();
        });

        std::vector<UInt64> received_page_ids;
        for (const String & page : packet->pages())
        {
            if (write_page_task == nullptr)
            {
                write_page_task = std::make_unique<WritePageTask>(seg_task->meta.dm_context->db_context.getSharedContextDisagg()->rn_page_cache.get());
            }
            auto & remote_page = write_page_task->remote_pages.emplace_back(); // NOLINT(bugprone-use-after-move)
            bool parsed = remote_page.ParseFromString(page);
            RUNTIME_CHECK_MSG(parsed, "Failed to parse page data (from {})", seg_task->info());
            write_page_task->remote_page_mem_tracker_wrappers.emplace_back(remote_page.SpaceUsedLong(), fetch_pages_mem_tracker.get());

            RUNTIME_CHECK(
                remaining_pages_to_fetch.contains(remote_page.page_id()),
                remaining_pages_to_fetch,
                remote_page.page_id());

            received_page_ids.emplace_back(remote_page.page_id());
            remaining_pages_to_fetch.erase(remote_page.page_id());

            // Write page into LocalPageCache. Note that the page must be occupied.
            auto oid = Remote::PageOID{
                .store_id = seg_task->meta.store_id,
                .ks_table_id = {seg_task->meta.keyspace_id, seg_task->meta.physical_table_id},
                .page_id = remote_page.page_id(),
            };
            auto read_buffer = std::make_shared<ReadBufferFromMemory>(remote_page.data().data(), remote_page.data().size());
            PageFieldSizes field_sizes;
            field_sizes.reserve(remote_page.field_sizes_size());
            for (const auto & field_sz : remote_page.field_sizes())
            {
                field_sizes.emplace_back(field_sz);
            }
            deserialize_page_ns += sw_packet.elapsedFromLastTime();

            auto page_id = RNLocalPageCache::buildCacheId(oid);
            write_page_task->wb.putPage(page_id, 0, std::move(read_buffer), remote_page.data().size(), std::move(field_sizes));
            auto write_batch_limit_size = seg_task->meta.dm_context->db_context.getSettingsRef().dt_write_page_cache_limit_size;
            if (write_page_task->wb.getTotalDataSize() >= write_batch_limit_size)
            {
                write_page_results.push_back(schedule_task(std::move(write_page_task))); // write_page_task is moved and reset.
            }
        }
    }

    if (write_page_task != nullptr && write_page_task->wb.getTotalDataSize() > 0)
    {
        write_page_results.push_back(schedule_task(std::move(write_page_task)));
    }

    Stopwatch sw_wait_write_page_finished;
    for (auto & f : write_page_results)
    {
        f.get();
    }
    auto wait_write_page_finished_ns = sw_wait_write_page_finished.elapsed();

    // This metric is per-segment.
    GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_write_page_cache).Observe(total_write_page_cache_sec);

    // Verify all pending pages are now received.
    RUNTIME_CHECK_MSG(
        remaining_pages_to_fetch.empty(),
        "Failed to fetch all pages (from {}), remaining_pages_to_fetch={}",
        seg_task->info(),
        remaining_pages_to_fetch);

    LOG_DEBUG(log,
              "Finished fetch pages, seg_task={}, page_count={}, packet_count={}, task_count={}, "
              "total_ms={}, read_stream_ms={}, deserialize_page_ms={}, schedule_write_page_ms={}, wait_write_page_finished_ms={}",
              seg_task->info(),
              page_count,
              packet_count,
              task_count,
              sw_total.elapsed() / 1000000,
              read_stream_ns / 1000000,
              deserialize_page_ns / 1000000,
              schedule_write_page_ns / 1000000,
              wait_write_page_finished_ns / 1000000);
}

} // namespace DB::DM::Remote
