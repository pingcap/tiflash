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

#include <Common/CurrentMetrics.h>
#include <IO/IOThreadPools.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Remote/RNDataProvider.h>
#include <Storages/DeltaMerge/Remote/Serializer.h>
#include <Storages/DeltaMerge/RowKeyRangeUtils.h>
#include <Storages/DeltaMerge/SegmentReadTask.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>

using namespace std::chrono_literals;
using namespace DB::DM::Remote;

namespace CurrentMetrics
{
extern const Metric DT_SegmentReadTasks;
}

namespace DB::ErrorCodes
{
extern const int DT_DELTA_INDEX_ERROR;
extern const int FETCH_PAGES_ERROR;
} // namespace DB::ErrorCodes

namespace DB::DM
{
SegmentReadTask::SegmentReadTask(
    const SegmentPtr & segment_, //
    const SegmentSnapshotPtr & read_snapshot_,
    const DMContextPtr & dm_context_,
    const RowKeyRanges & ranges_)
    : store_id(dm_context_->global_context.getTMTContext().getKVStore()->getStoreID())
    , segment(segment_)
    , read_snapshot(read_snapshot_)
    , dm_context(dm_context_)
    , ranges(ranges_)
{
    CurrentMetrics::add(CurrentMetrics::DT_SegmentReadTasks);
}

SegmentReadTask::SegmentReadTask(
    const LoggerPtr & log,
    const Context & db_context,
    const ScanContextPtr & scan_context,
    const RemotePb::RemoteSegment & proto,
    const DisaggTaskId & snapshot_id,
    StoreID store_id_,
    const String & store_address,
    KeyspaceID keyspace_id,
    TableID physical_table_id)
    : store_id(store_id_)
{
    CurrentMetrics::add(CurrentMetrics::DT_SegmentReadTasks);
    auto tracing_id = fmt::format(
        "{} segment_id={} epoch={} delta_epoch={}",
        log->identifier(),
        proto.segment_id(),
        proto.segment_epoch(),
        proto.delta_index_epoch());

    auto rb = ReadBufferFromString(proto.key_range());
    auto segment_range = RowKeyRange::deserialize(rb);

    dm_context = DMContext::create(
        db_context,
        /* path_pool */ nullptr,
        /* storage_pool */ nullptr,
        /* min_version */ 0,
        keyspace_id,
        physical_table_id,
        /* is_common_handle */ segment_range.is_common_handle,
        /* rowkey_column_size */ segment_range.rowkey_column_size,
        db_context.getSettingsRef(),
        scan_context,
        tracing_id);

    segment = std::make_shared<Segment>(
        Logger::get(),
        /*epoch*/ proto.segment_epoch(),
        segment_range,
        proto.segment_id(),
        /*next_segment_id*/ 0,
        nullptr,
        nullptr);

    read_snapshot = Serializer::deserializeSegment(*dm_context, store_id, keyspace_id, physical_table_id, proto);

    ranges.reserve(proto.read_key_ranges_size());
    for (const auto & read_key_range : proto.read_key_ranges())
    {
        auto rb = ReadBufferFromString(read_key_range);
        ranges.push_back(RowKeyRange::deserialize(rb));
    }

    std::vector<UInt64> remote_page_ids;
    std::vector<size_t> remote_page_sizes;
    {
        // The number of ColumnFileTiny of MemTableSet is unknown, but there is a very high probability that it is zero.
        // So ignoring the number of ColumnFileTiny of MemTableSet is better than always adding all the number of ColumnFile of MemTableSet when reserving.
        const auto & cfs = read_snapshot->delta->getPersistedFileSetSnapshot()->getColumnFiles();
        remote_page_ids.reserve(cfs.size());
        remote_page_sizes.reserve(cfs.size());
    }
    auto extract_remote_pages = [&remote_page_ids, &remote_page_sizes](const ColumnFiles & cfs) {
        UInt64 count = 0;
        for (const auto & cf : cfs)
        {
            if (auto * tiny = cf->tryToTinyFile(); tiny)
            {
                remote_page_ids.emplace_back(tiny->getDataPageId());
                remote_page_sizes.emplace_back(tiny->getDataPageSize());
                ++count;
            }
        }
        return count;
    };
    auto memory_page_count = extract_remote_pages(read_snapshot->delta->getMemTableSetSnapshot()->getColumnFiles());
    auto persisted_page_count
        = extract_remote_pages(read_snapshot->delta->getPersistedFileSetSnapshot()->getColumnFiles());

    extra_remote_info.emplace(ExtraRemoteSegmentInfo{
        .store_address = store_address,
        .snapshot_id = snapshot_id,
        .remote_page_ids = std::move(remote_page_ids),
        .remote_page_sizes = std::move(remote_page_sizes),
    });

    LOG_DEBUG(
        read_snapshot->log,
        "memory_cfs_count={} memory_page_count={} persisted_cfs_count={} persisted_page_count={} remote_page_ids={} "
        "delta_index={} store_address={}",
        read_snapshot->delta->getMemTableSetSnapshot()->getColumnFileCount(),
        memory_page_count,
        read_snapshot->delta->getPersistedFileSetSnapshot()->getColumnFileCount(),
        persisted_page_count,
        extra_remote_info->remote_page_ids,
        read_snapshot->delta->getSharedDeltaIndex()->toString(),
        store_address);
}

SegmentReadTask::~SegmentReadTask()
{
    CurrentMetrics::sub(CurrentMetrics::DT_SegmentReadTasks);
}

void SegmentReadTask::addRange(const RowKeyRange & range)
{
    ranges.push_back(range);
}

void SegmentReadTask::mergeRanges()
{
    ranges = DM::tryMergeRanges(std::move(ranges), 1);
}

SegmentReadTasks SegmentReadTask::trySplitReadTasks(const SegmentReadTasks & tasks, size_t expected_size)
{
    if (tasks.empty() || tasks.size() >= expected_size)
        return tasks;

    // Note that expected_size is normally small(less than 100), so the algorithm complexity here does not matter.

    // Construct a max heap, determined by ranges' count.
    auto cmp = [](const SegmentReadTaskPtr & a, const SegmentReadTaskPtr & b) {
        return a->ranges.size() < b->ranges.size();
    };
    std::priority_queue<SegmentReadTaskPtr, std::vector<SegmentReadTaskPtr>, decltype(cmp)> largest_ranges_first(
        tasks.begin(),
        tasks.end(),
        cmp);

    // Split the top task.
    while (largest_ranges_first.size() < expected_size && largest_ranges_first.top()->ranges.size() > 1)
    {
        auto top = largest_ranges_first.top();
        largest_ranges_first.pop();

        size_t split_count = top->ranges.size() / 2;

        auto left = std::make_shared<SegmentReadTask>(
            top->segment,
            top->read_snapshot->clone(),
            top->dm_context,
            RowKeyRanges(top->ranges.begin(), top->ranges.begin() + split_count));
        auto right = std::make_shared<SegmentReadTask>(
            top->segment,
            top->read_snapshot->clone(),
            top->dm_context,
            RowKeyRanges(top->ranges.begin() + split_count, top->ranges.end()));

        largest_ranges_first.push(left);
        largest_ranges_first.push(right);
    }

    SegmentReadTasks result_tasks;
    while (!largest_ranges_first.empty())
    {
        result_tasks.push_back(largest_ranges_first.top());
        largest_ranges_first.pop();
    }

    return result_tasks;
}

void SegmentReadTask::initColumnFileDataProvider(const Remote::RNLocalPageCacheGuardPtr & pages_guard)
{
    RUNTIME_CHECK(extra_remote_info.has_value());
    auto page_cache = dm_context->global_context.getSharedContextDisagg()->rn_page_cache;
    auto page_data_provider = std::make_shared<Remote::ColumnFileDataProviderRNLocalPageCache>(
        page_cache,
        pages_guard,
        store_id,
        KeyspaceTableID{dm_context->keyspace_id, dm_context->physical_table_id});

    auto & persisted_cf_set_data_provider = read_snapshot->delta->getPersistedFileSetSnapshot()->data_provider;
    RUNTIME_CHECK(std::dynamic_pointer_cast<ColumnFileDataProviderNop>(persisted_cf_set_data_provider));
    persisted_cf_set_data_provider = page_data_provider;

    auto & memory_cf_set_data_provider = read_snapshot->delta->getMemTableSetSnapshot()->data_provider;
    RUNTIME_CHECK(std::dynamic_pointer_cast<ColumnFileDataProviderNop>(memory_cf_set_data_provider));
    memory_cf_set_data_provider = page_data_provider;
}


void SegmentReadTask::initInputStream(
    const ColumnDefines & columns_to_read,
    UInt64 start_ts,
    const PushDownFilterPtr & push_down_filter,
    ReadMode read_mode,
    size_t expected_block_size,
    bool enable_delta_index_error_fallback)
{
    if (likely(doInitInputStreamWithErrorFallback(
            columns_to_read,
            start_ts,
            push_down_filter,
            read_mode,
            expected_block_size,
            enable_delta_index_error_fallback)))
    {
        return;
    }

    // Exception DT_DELTA_INDEX_ERROR raised. Reset delta index and try again.
    DeltaIndex empty_delta_index;
    read_snapshot->delta->getSharedDeltaIndex()->swap(empty_delta_index);
    if (auto cache = dm_context->global_context.getSharedContextDisagg()->rn_delta_index_cache; cache)
    {
        cache->setDeltaIndex(read_snapshot->delta->getSharedDeltaIndex());
    }
    doInitInputStream(columns_to_read, start_ts, push_down_filter, read_mode, expected_block_size);
}

bool SegmentReadTask::doInitInputStreamWithErrorFallback(
    const ColumnDefines & columns_to_read,
    UInt64 start_ts,
    const PushDownFilterPtr & push_down_filter,
    ReadMode read_mode,
    size_t expected_block_size,
    bool enable_delta_index_error_fallback)
{
    try
    {
        doInitInputStream(columns_to_read, start_ts, push_down_filter, read_mode, expected_block_size);
        return true;
    }
    catch (const Exception & e)
    {
        if (enable_delta_index_error_fallback && e.code() == ErrorCodes::DT_DELTA_INDEX_ERROR)
        {
            LOG_ERROR(read_snapshot->log, "{}", e.message());
            return false;
        }
        else
        {
            throw;
        }
    }
}

void SegmentReadTask::doInitInputStream(
    const ColumnDefines & columns_to_read,
    UInt64 start_ts,
    const PushDownFilterPtr & push_down_filter,
    ReadMode read_mode,
    size_t expected_block_size)
{
    RUNTIME_CHECK(input_stream == nullptr);
    Stopwatch watch_work{CLOCK_MONOTONIC_COARSE};
    SCOPE_EXIT({
        GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_worker_prepare_stream)
            .Observe(watch_work.elapsedSeconds());
    });

    input_stream = segment->getInputStream(
        read_mode,
        *dm_context,
        columns_to_read,
        read_snapshot,
        ranges,
        push_down_filter,
        start_ts,
        expected_block_size);
}


void SegmentReadTask::fetchPages()
{
    // Not remote segment.
    if (!extra_remote_info.has_value())
    {
        return;
    }
    // Stable-only segment.
    if (extra_remote_info->remote_page_ids.empty() && !needFetchMemTableSet())
    {
        LOG_DEBUG(read_snapshot->log, "Neither ColumnFileTiny or ColumnFileInMemory need to be fetched from WN.");
        return;
    }

    Stopwatch watch_work{CLOCK_MONOTONIC_COARSE};
    SCOPE_EXIT({
        GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_worker_fetch_page)
            .Observe(watch_work.elapsedSeconds());
    });

    auto occupy_result = blockingOccupySpaceForTask();
    auto req = buildFetchPagesRequest(occupy_result.pages_not_in_cache);
    {
        auto cftiny_total = extra_remote_info->remote_page_ids.size();
        auto cftiny_fetch = occupy_result.pages_not_in_cache.size();
        LOG_DEBUG(
            read_snapshot->log,
            "Ready to fetch pages, seg_task={} page_hit_rate={} pages_not_in_cache={}",
            *this,
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
            doFetchPages(req);
            initColumnFileDataProvider(occupy_result.pages_guard);
            // We finished fetch all pages for this seg task, just return it for downstream
            // workers. If we have met any errors, page guard will not be persisted.
            return;
        }
        catch (const pingcap::Exception & e)
        {
            last_exception = std::current_exception();
            LOG_WARNING(
                read_snapshot->log,
                "Meet RPC client exception when fetching pages: {}, will be retried. seg_task={}",
                e.displayText(),
                *this);
            std::this_thread::sleep_for(1s); // FIXME: yield instead of sleep.
        }
        catch (...)
        {
            LOG_ERROR(read_snapshot->log, "{}: {}", *this, getCurrentExceptionMessage(true));
            throw;
        }
    }

    // Still failed after retry...
    RUNTIME_CHECK(last_exception);
    std::rethrow_exception(last_exception);
}

std::vector<Remote::PageOID> SegmentReadTask::buildRemotePageOID() const
{
    std::vector<Remote::PageOID> cf_tiny_oids;
    cf_tiny_oids.reserve(extra_remote_info->remote_page_ids.size());
    for (const auto & page_id : extra_remote_info->remote_page_ids)
    {
        cf_tiny_oids.emplace_back(Remote::PageOID{
            .store_id = store_id,
            .ks_table_id = {dm_context->keyspace_id, dm_context->physical_table_id},
            .page_id = page_id,
        });
    }
    return cf_tiny_oids;
}

Remote::RNLocalPageCache::OccupySpaceResult SegmentReadTask::blockingOccupySpaceForTask() const
{
    auto cf_tiny_oids = buildRemotePageOID();
    // Note: We must occupySpace segment by segment, because we need to read
    // at least the complete data of one segment in order to drive everything forward.
    // Currently we call occupySpace for each FetchPagesRequest, which is fine,
    // because we send one request each seg_task. If we want to split
    // FetchPagesRequest into multiples in future, then we need to change
    // the moment of calling `occupySpace`.
    Stopwatch w_occupy;
    SCOPE_EXIT({
        GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_cache_occupy)
            .Observe(w_occupy.elapsedSeconds());
    });
    auto page_cache = dm_context->global_context.getSharedContextDisagg()->rn_page_cache;
    auto scan_context = dm_context->scan_context;
    return page_cache->occupySpace(cf_tiny_oids, extra_remote_info->remote_page_sizes, scan_context);
}

disaggregated::FetchDisaggPagesRequest SegmentReadTask::buildFetchPagesRequest(
    const std::vector<Remote::PageOID> & pages_not_in_cache) const
{
    disaggregated::FetchDisaggPagesRequest req;
    auto meta = extra_remote_info->snapshot_id.toMeta();
    // The keyspace_id here is not vital, as we locate the table and segment by given
    // snapshot_id. But it could be helpful for debugging.
    auto keyspace_id = dm_context->keyspace_id;
    meta.set_keyspace_id(keyspace_id);
    meta.set_api_version(keyspace_id == NullspaceID ? kvrpcpb::APIVersion::V1 : kvrpcpb::APIVersion::V2);
    *req.mutable_snapshot_id() = meta;
    req.set_table_id(dm_context->physical_table_id);
    req.set_segment_id(segment->segmentId());

    req.mutable_page_ids()->Reserve(pages_not_in_cache.size());
    for (auto page_id : pages_not_in_cache)
        req.add_page_ids(page_id.page_id);

    return req;
}

// In order to make network and disk run parallelly,
// `doFetchPages` will receive data pages from WN,
// package these data pages into several `WritePageTask` objects
// and send them to `RNWritePageCachePool` to write into local page cache.
struct WritePageTask
{
    explicit WritePageTask(Remote::RNLocalPageCache * page_cache_)
        : page_cache(page_cache_)
    {
        RUNTIME_CHECK(page_cache != nullptr);
    }
    Remote::RNLocalPageCache * page_cache;
    UniversalWriteBatch wb;
    std::forward_list<DM::RemotePb::RemotePage> remote_pages; // Hold the data of wb.
    std::forward_list<MemTrackerWrapper> remote_page_mem_tracker_wrappers; // Hold the memory stat of remote_pages.
};
using WritePageTaskPtr = std::unique_ptr<WritePageTask>;

void SegmentReadTask::checkMemTableSet(const ColumnFileSetSnapshotPtr & mem_table_snap) const
{
    const auto & old_mem_table_snap = read_snapshot->delta->getMemTableSetSnapshot();

    RUNTIME_CHECK_MSG(
        mem_table_snap->getColumnFileCount() == old_mem_table_snap->getColumnFileCount(),
        "log_id={}, new_cf_count={}, old_cf_count={}",
        read_snapshot->log->identifier(),
        mem_table_snap->getColumnFileCount(),
        old_mem_table_snap->getColumnFileCount());

    const auto & column_files = mem_table_snap->getColumnFiles();
    const auto & old_column_files = old_mem_table_snap->getColumnFiles();
    auto check_rows = [](UInt64 rows, UInt64 old_rows, bool last_cf) {
        // Only the last ColumnFileInMemory is appendable.
        return last_cf ? rows >= old_rows : rows == old_rows;
    };
    for (size_t i = 0; i < column_files.size(); ++i)
    {
        const auto & cf = column_files[i];
        const auto & old_cf = old_column_files[i];
        RUNTIME_CHECK_MSG(
            cf->getType() == old_cf->getType()
                && check_rows(cf->getRows(), old_cf->getRows(), i == column_files.size() - 1),
            "log_id={}, new_type={}, old_type={}, new_rows={}, old_rows={}, cf_count={}, cf_index={}",
            read_snapshot->log->identifier(),
            magic_enum::enum_name(cf->getType()),
            magic_enum::enum_name(old_cf->getType()),
            cf->getRows(),
            old_cf->getRows(),
            column_files.size(),
            i);
    }
}

void SegmentReadTask::checkMemTableSetReady() const
{
    const auto & mem_table_snap = read_snapshot->delta->getMemTableSetSnapshot();
    for (auto & cf : mem_table_snap->getColumnFiles())
    {
        if (auto * in_mem_cf = cf->tryToInMemoryFile(); in_mem_cf)
        {
            RUNTIME_CHECK_MSG(in_mem_cf->getCache() != nullptr, "Fail to fetch MemTableSet from {}", *this);
        }
    }
}

bool SegmentReadTask::needFetchMemTableSet() const
{
    // Check if any object of ColumnFileInMemory does not contain data.
    for (const auto & cf : read_snapshot->delta->getMemTableSetSnapshot()->getColumnFiles())
    {
        if (auto * cf_in_mem = cf->tryToInMemoryFile(); cf_in_mem)
        {
            if (cf_in_mem->getCache() == nullptr)
            {
                return true;
            }
        }
    }
    return false;
}

static void checkPageID(
    UInt64 page_id,
    std::vector<UInt64> & received_page_ids,
    std::unordered_set<UInt64> & remaining_pages_to_fetch)
{
    RUNTIME_CHECK(remaining_pages_to_fetch.contains(page_id), remaining_pages_to_fetch, page_id);

    received_page_ids.emplace_back(page_id);
    remaining_pages_to_fetch.erase(page_id);
}

void SegmentReadTask::doFetchPages(const disaggregated::FetchDisaggPagesRequest & request)
{
    // No matter all delta data is cached or not, call FetchDisaggPages to release snapshot in WN.
    const auto * cluster = dm_context->global_context.getTMTContext().getKVCluster();
    pingcap::kv::RpcCall<pingcap::kv::RPC_NAME(FetchDisaggPages)> rpc(
        cluster->rpc_client,
        extra_remote_info->store_address);
    grpc::ClientContext client_context;
    // set timeout for the streaming call to avoid inf wait before `Finish()`
    rpc.setClientContext(client_context, dm_context->global_context.getSettingsRef().disagg_fetch_pages_timeout);
    auto stream_resp = rpc.call(&client_context, request);
    RUNTIME_CHECK(stream_resp != nullptr);
    SCOPE_EXIT({
        // Most of the time, it will call `Finish()` and check the status of grpc when `Read()` return false.
        // `Finish()` will be called here when exceptions thrown.
        if (unlikely(stream_resp != nullptr))
        {
            stream_resp->Finish();
        }
    });

    // All delta data is cached.
    if (request.page_ids_size() == 0 && !needFetchMemTableSet())
    {
        finishPagesPacketStream(stream_resp);
        return;
    }

    doFetchPagesImpl(
        [&stream_resp, this](disaggregated::PagesPacket & packet) {
            if (stream_resp->Read(&packet))
            {
                return true;
            }
            else
            {
                finishPagesPacketStream(stream_resp);
                return false;
            }
        },
        std::unordered_set<UInt64>(request.page_ids().begin(), request.page_ids().end()));
}

void SegmentReadTask::doFetchPagesImpl(
    std::function<bool(disaggregated::PagesPacket &)> && read_packet,
    std::unordered_set<UInt64> remaining_pages_to_fetch)
{
    UInt64 read_page_ns = 0;
    UInt64 deserialize_page_ns = 0;
    UInt64 wait_write_page_ns = 0;

    Stopwatch sw_total;
    UInt64 packet_count = 0;
    UInt64 write_page_task_count = 0;
    const UInt64 page_count = remaining_pages_to_fetch.size();

    auto schedule_write_page_task = [&write_page_task_count, &wait_write_page_ns](WritePageTaskPtr && write_page_task) {
        write_page_task_count += 1;
        auto task = std::make_shared<std::packaged_task<void()>>([write_page_task = std::move(write_page_task)]() {
            write_page_task->page_cache->write(std::move(write_page_task->wb));
        });
        Stopwatch sw;
        RNWritePageCachePool::get().scheduleOrThrowOnError([task]() { (*task)(); });
        wait_write_page_ns += sw.elapsed();
        return task->get_future();
    };

    WritePageTaskPtr write_page_task;
    std::vector<std::future<void>> write_page_results;

    google::protobuf::RepeatedPtrField<RemotePb::ColumnFileRemote> memtableset_cfs;

    // Keep reading packets.
    while (true)
    {
        Stopwatch sw_read_packet;
        disaggregated::PagesPacket packet;
        if (!read_packet(packet))
            break;
        if (packet.has_error())
            throw Exception(ErrorCodes::FETCH_PAGES_ERROR, "{} (from {})", packet.error().msg(), *this);

        read_page_ns = sw_read_packet.elapsed();
        packet_count += 1;
        MemTrackerWrapper packet_mem_tracker_wrapper(packet.SpaceUsedLong(), fetch_pages_mem_tracker.get());

        // Handle `chunks`.
        for (const auto & s : packet.chunks())
        {
            RUNTIME_CHECK(memtableset_cfs.Add()->ParseFromString(s), read_snapshot->log->identifier());
        }

        // Handle `pages`.
        std::vector<UInt64> received_page_ids;
        received_page_ids.reserve(packet.pages_size());
        for (const auto & page : packet.pages())
        {
            Stopwatch sw;
            if (write_page_task == nullptr)
            {
                write_page_task = std::make_unique<WritePageTask>(
                    dm_context->global_context.getSharedContextDisagg()->rn_page_cache.get());
            }
            auto & remote_page = write_page_task->remote_pages.emplace_front(); // NOLINT(bugprone-use-after-move)
            bool parsed = remote_page.ParseFromString(page);
            RUNTIME_CHECK_MSG(parsed, "Failed to parse page data (from {})", *this);
            write_page_task->remote_page_mem_tracker_wrappers.emplace_front(
                remote_page.SpaceUsedLong(),
                fetch_pages_mem_tracker.get());

            checkPageID(remote_page.page_id(), received_page_ids, remaining_pages_to_fetch);

            // Write page into LocalPageCache. Note that the page must be occupied.
            auto oid = Remote::PageOID{
                .store_id = store_id,
                .ks_table_id = {dm_context->keyspace_id, dm_context->physical_table_id},
                .page_id = remote_page.page_id(),
            };
            auto read_buffer
                = std::make_shared<ReadBufferFromMemory>(remote_page.data().data(), remote_page.data().size());
            PageFieldSizes field_sizes;
            field_sizes.reserve(remote_page.field_sizes_size());
            for (const auto & field_sz : remote_page.field_sizes())
            {
                field_sizes.emplace_back(field_sz);
            }
            deserialize_page_ns += sw.elapsed();

            auto page_id = Remote::RNLocalPageCache::buildCacheId(oid);
            write_page_task->wb
                .putPage(page_id, 0, std::move(read_buffer), remote_page.data().size(), std::move(field_sizes));
            auto write_batch_limit_size = dm_context->global_context.getSettingsRef().dt_write_page_cache_limit_size;
            if (write_page_task->wb.getTotalDataSize() >= write_batch_limit_size)
            {
                // write_page_task is moved and reset.
                write_page_results.push_back(schedule_write_page_task(std::move(write_page_task)));
            }
        }
    }

    if (write_page_task != nullptr && write_page_task->wb.getTotalDataSize() > 0)
    {
        write_page_results.push_back(schedule_write_page_task(std::move(write_page_task)));
    }

    if (!memtableset_cfs.empty())
    {
        const auto & data_store = dm_context->global_context.getSharedContextDisagg()->remote_data_store;
        auto mem_table_snap
            = Serializer::deserializeColumnFileSet(*dm_context, memtableset_cfs, data_store, segment->getRowKeyRange());
        checkMemTableSet(mem_table_snap);
        read_snapshot->delta->setMemTableSetSnapshot(mem_table_snap);
    }

    Stopwatch sw_wait_write_page_finished;
    for (auto & f : write_page_results)
    {
        f.get();
    }
    wait_write_page_ns += sw_wait_write_page_finished.elapsed();

    // Verify all pending pages are now received.
    checkMemTableSetReady();
    RUNTIME_CHECK_MSG(
        remaining_pages_to_fetch.empty(),
        "Failed to fetch all pages for {}, remaining_pages_to_fetch={}, wn_address={}",
        *this,
        remaining_pages_to_fetch,
        extra_remote_info->store_address);

    GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_rpc_fetch_page)
        .Observe(read_page_ns / 1000000000.0);
    GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_deserialize_page)
        .Observe(deserialize_page_ns / 1000000000.0);
    GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_write_page_cache)
        .Observe(wait_write_page_ns / 1000000000.0);

    LOG_DEBUG(
        read_snapshot->log,
        "Finished fetch pages, seg_task={}, page_count={}, packet_count={}, write_page_task_count={}, "
        "total_ms={}, read_stream_ms={}, deserialize_page_ms={}, schedule_write_page_ms={}",
        *this,
        page_count,
        packet_count,
        write_page_task_count,
        sw_total.elapsed() / 1000000,
        read_page_ns / 1000000,
        deserialize_page_ns / 1000000,
        wait_write_page_ns / 1000000);
}

String SegmentReadTask::toString() const
{
    if (dm_context->keyspace_id == DB::NullspaceID)
    {
        return fmt::format(
            "s{}_t{}_{}_{}_{}_{}",
            store_id,
            dm_context->physical_table_id,
            segment->segmentId(),
            segment->segmentEpoch(),
            read_snapshot->delta->getDeltaIndexEpoch(),
            read_snapshot->getRows());
    }
    return fmt::format(
        "s{}_ks{}_t{}_{}_{}_{}_{}",
        store_id,
        dm_context->keyspace_id,
        dm_context->physical_table_id,
        segment->segmentId(),
        segment->segmentEpoch(),
        read_snapshot->delta->getDeltaIndexEpoch(),
        read_snapshot->getRows());
}

GlobalSegmentID SegmentReadTask::getGlobalSegmentID() const
{
    return GlobalSegmentID{
        .store_id = store_id,
        .keyspace_id = dm_context->keyspace_id,
        .physical_table_id = dm_context->physical_table_id,
        .segment_id = segment->segmentId(),
        .segment_epoch = segment->segmentEpoch(),
    };
}

void SegmentReadTask::finishPagesPacketStream(
    std::unique_ptr<grpc::ClientReader<disaggregated::PagesPacket>> & stream_resp)
{
    if unlikely (stream_resp == nullptr)
        return;

    auto status = stream_resp->Finish();
    stream_resp.reset(); // Reset to avoid calling `Finish()` repeatedly.
    RUNTIME_CHECK_MSG(
        status.ok(),
        "Failed to fetch all pages for {}, status={}, message={}, wn_address={}",
        *this,
        static_cast<int>(status.error_code()),
        status.error_message(),
        extra_remote_info->store_address);
}

bool SegmentReadTask::hasColumnFileToFetch() const
{
    auto need_to_fetch = [](const ColumnFilePtr & cf) {
        // Only ColumnFileMemory and ColumnFileTiny need too fetch.
        // ColumnFileDeleteRange and ColumnFileBig do not need to fetch.
        return cf->isInMemoryFile() || cf->isTinyFile();
    };

    const auto & mem_cfs = read_snapshot->delta->getMemTableSetSnapshot()->getColumnFiles();
    if (std::any_of(mem_cfs.cbegin(), mem_cfs.cend(), need_to_fetch))
        return true;

    const auto & persisted_cfs = read_snapshot->delta->getPersistedFileSetSnapshot()->getColumnFiles();
    if (std::any_of(persisted_cfs.cbegin(), persisted_cfs.cend(), need_to_fetch))
        return true;

    return false;
}
} // namespace DB::DM
