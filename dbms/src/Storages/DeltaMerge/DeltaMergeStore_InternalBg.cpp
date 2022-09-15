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

#include <Common/SyncPoint/SyncPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/Transaction/TMTContext.h>

#include <magic_enum.hpp>

namespace CurrentMetrics
{
extern const Metric DT_SnapshotOfDeltaMerge;
} // namespace CurrentMetrics

namespace DB
{
namespace FailPoints
{
extern const char gc_skip_update_safe_point[];
extern const char pause_before_dt_background_delta_merge[];
extern const char pause_until_dt_background_delta_merge[];
} // namespace FailPoints

namespace DM
{
void DeltaMergeStore::setUpBackgroundTask(const DMContextPtr & dm_context)
{
    // Callbacks for cleaning outdated DTFiles. Note that there is a chance
    // that callbacks is called after the `DeltaMergeStore` dropped, we must
    // make the callbacks safe.
    ExternalPageCallbacks callbacks;
    callbacks.ns_id = storage_pool->getNamespaceId();
    callbacks.scanner = [path_pool_weak_ref = std::weak_ptr<StoragePathPool>(path_pool), file_provider = global_context.getFileProvider()]() {
        ExternalPageCallbacks::PathAndIdsVec path_and_ids_vec;

        // If the StoragePathPool is invalid, meaning we call `scanner` after dropping the table,
        // simply return an empty list is OK.
        auto path_pool = path_pool_weak_ref.lock();
        if (!path_pool)
            return path_and_ids_vec;

        // Return the DTFiles on disks.
        auto delegate = path_pool->getStableDiskDelegator();
        // Only return the DTFiles can be GC. The page id of not able to be GC files, which is being ingested or in the middle of
        // SegmentSplit/Merge/MergeDelta, is not yet applied
        // to PageStorage is marked as not able to be GC, so we don't return them and run the `remover`
        DMFile::ListOptions options;
        options.only_list_can_gc = true;
        for (auto & root_path : delegate.listPaths())
        {
            std::set<PageId> ids_under_path;
            auto file_ids_in_current_path = DMFile::listAllInPath(file_provider, root_path, options);
            path_and_ids_vec.emplace_back(root_path, std::move(file_ids_in_current_path));
        }
        return path_and_ids_vec;
    };
    callbacks.remover = [path_pool_weak_ref = std::weak_ptr<StoragePathPool>(path_pool), //
                         file_provider = global_context.getFileProvider(),
                         logger = log](const ExternalPageCallbacks::PathAndIdsVec & path_and_ids_vec, const std::set<PageId> & valid_ids) {
        // If the StoragePathPool is invalid, meaning we call `remover` after dropping the table,
        // simply skip is OK.
        auto path_pool = path_pool_weak_ref.lock();
        if (!path_pool)
            return;

        SYNC_FOR("before_DeltaMergeStore::callbacks_remover_remove");
        auto delegate = path_pool->getStableDiskDelegator();
        for (const auto & [path, ids] : path_and_ids_vec)
        {
            for (auto id : ids)
            {
                if (valid_ids.count(id))
                    continue;

                // Note that page_id is useless here.
                auto dmfile = DMFile::restore(file_provider, id, /* page_id= */ 0, path, DMFile::ReadMetaMode::none());
                if (unlikely(!dmfile))
                {
                    // If the dtfile directory is not exist, it means `StoragePathPool::drop` have been
                    // called in another thread. Just try to clean if any id is left.
                    try
                    {
                        delegate.removeDTFile(id);
                    }
                    catch (DB::Exception & e)
                    {
                        // just ignore
                    }
                    LOG_FMT_INFO(logger,
                                 "GC try remove useless DM file, but file not found and may have been removed, dmfile={}",
                                 DMFile::getPathByStatus(path, id, DMFile::Status::READABLE));
                }
                else if (dmfile->canGC())
                {
                    // StoragePathPool::drop may be called concurrently, ignore and continue next file if any exception thrown
                    String err_msg;
                    try
                    {
                        // scanner should only return dtfiles that can GC,
                        // just another check here.
                        delegate.removeDTFile(dmfile->fileId());
                        dmfile->remove(file_provider);
                    }
                    catch (DB::Exception & e)
                    {
                        err_msg = e.message();
                    }
                    catch (Poco::Exception & e)
                    {
                        err_msg = e.message();
                    }
                    if (err_msg.empty())
                        LOG_FMT_INFO(logger, "GC removed useless DM file, dmfile={}", dmfile->path());
                    else
                        LOG_FMT_INFO(logger, "GC try remove useless DM file, but error happen, dmfile={} err_msg={}", dmfile->path(), err_msg);
                }
            }
        }
    };
    // remember to unregister it when shutdown
    storage_pool->dataRegisterExternalPagesCallbacks(callbacks);
    storage_pool->enableGC();

    background_task_handle = background_pool.addTask([this] { return handleBackgroundTask(false); });

    blockable_background_pool_handle = blockable_background_pool.addTask([this] { return handleBackgroundTask(true); });

    // Do place delta index.
    for (auto & [end, segment] : segments)
    {
        (void)end;
        checkSegmentUpdate(dm_context, segment, ThreadType::Init);
    }

    // Wake up to do place delta index tasks.
    background_task_handle->wake();
    blockable_background_pool_handle->wake();
}

std::vector<SegmentPtr> DeltaMergeStore::getMergeableSegments(const DMContextPtr & context, const SegmentPtr & baseSegment)
{
    // Last segment cannot be merged.
    if (baseSegment->getRowKeyRange().isEndInfinite())
        return {};

    // We only merge small segments into a larger one.
    // Note: it is possible that there is a very small segment close to a very large segment.
    // In this case, the small segment will not get merged. It is possible that we can allow
    // segment merging for this case in future.
    auto max_total_rows = context->small_segment_rows;
    auto max_total_bytes = context->small_segment_bytes;

    std::vector<SegmentPtr> results;
    {
        std::shared_lock lock(read_write_mutex);

        if (!isSegmentValid(lock, baseSegment))
            return {};

        results.reserve(4); // In most cases we will only find <= 4 segments to merge.
        results.emplace_back(baseSegment);
        auto accumulated_rows = baseSegment->getEstimatedRows();
        auto accumulated_bytes = baseSegment->getEstimatedBytes();

        auto it = segments.upper_bound(baseSegment->getRowKeyRange().getEnd());
        while (it != segments.end())
        {
            const auto & this_seg = it->second;
            const auto this_rows = this_seg->getEstimatedRows();
            const auto this_bytes = this_seg->getEstimatedBytes();
            if (accumulated_rows + this_rows >= max_total_rows || accumulated_bytes + this_bytes >= max_total_bytes)
                break;
            results.emplace_back(this_seg);
            accumulated_rows += this_rows;
            accumulated_bytes += this_bytes;
            it++;
        }
    }

    if (results.size() < 2)
        return {};

    return results;
}

bool DeltaMergeStore::updateGCSafePoint()
{
    if (auto pd_client = global_context.getTMTContext().getPDClient(); !pd_client->isMock())
    {
        auto safe_point = PDClientHelper::getGCSafePointWithRetry(
            pd_client,
            /* ignore_cache= */ false,
            global_context.getSettingsRef().safe_point_update_interval_seconds);
        latest_gc_safe_point.store(safe_point, std::memory_order_release);
        return true;
    }
    return false;
}

bool DeltaMergeStore::handleBackgroundTask(bool heavy)
{
    auto task = background_tasks.nextTask(heavy, log);
    if (!task)
        return false;

    // Update GC safe point before background task
    // Foreground task don't get GC safe point from remote, but we better make it as up to date as possible.
    if (updateGCSafePoint())
    {
        /// Note that `task.dm_context->db_context` will be free after query is finish. We should not use that in background task.
        task.dm_context->min_version = latest_gc_safe_point.load(std::memory_order_relaxed);
        LOG_FMT_DEBUG(log, "Task {} GC safe point: {}", magic_enum::enum_name(task.type), task.dm_context->min_version);
    }

    SegmentPtr left, right;
    ThreadType type = ThreadType::Write;
    try
    {
        switch (task.type)
        {
        case TaskType::Split:
            std::tie(left, right) = segmentSplit(*task.dm_context, task.segment, SegmentSplitReason::Background);
            type = ThreadType::BG_Split;
            break;
        case TaskType::MergeDelta:
        {
            FAIL_POINT_PAUSE(FailPoints::pause_before_dt_background_delta_merge);
            left = segmentMergeDelta(*task.dm_context, task.segment, MergeDeltaReason::BackgroundThreadPool);
            type = ThreadType::BG_MergeDelta;
            // Wake up all waiting threads if failpoint is enabled
            FailPointHelper::disableFailPoint(FailPoints::pause_until_dt_background_delta_merge);
            break;
        }
        case TaskType::Compact:
            task.segment->compactDelta(*task.dm_context);
            left = task.segment;
            type = ThreadType::BG_Compact;
            break;
        case TaskType::Flush:
            task.segment->flushCache(*task.dm_context);
            // After flush cache, better place delta index.
            task.segment->placeDeltaIndex(*task.dm_context);
            left = task.segment;
            type = ThreadType::BG_Flush;
            break;
        case TaskType::PlaceIndex:
            task.segment->placeDeltaIndex(*task.dm_context);
            break;
        default:
            throw Exception(fmt::format("Unsupported task type: {}", magic_enum::enum_name(task.type)));
        }
    }
    catch (const Exception & e)
    {
        LOG_FMT_ERROR(
            log,
            "Execute task on segment failed, task={} segment={} err={}",
            magic_enum::enum_name(task.type),
            task.segment->simpleInfo(),
            e.message());
        e.rethrow();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }

    // continue to check whether we need to apply more tasks after this task is ended.
    if (left)
        checkSegmentUpdate(task.dm_context, left, type);
    if (right)
        checkSegmentUpdate(task.dm_context, right, type);

    return true;
}

namespace GC

{
enum class MergeDeltaReason
{
    Unknown,
    TooManyDeleteRange,
    TooMuchOutOfRange,
    TooManyInvalidVersion,
};

static std::string toString(MergeDeltaReason type)
{
    switch (type)
    {
    case MergeDeltaReason::TooManyDeleteRange:
        return "TooManyDeleteRange";
    case MergeDeltaReason::TooMuchOutOfRange:
        return "TooMuchOutOfRange";
    case MergeDeltaReason::TooManyInvalidVersion:
        return "TooManyInvalidVersion";
    default:
        return "Unknown";
    }
}

// Returns true if it needs gc.
// This is for optimization purpose, does not mean to be accurate.
bool shouldCompactStableWithTooManyInvalidVersion(const SegmentPtr & seg, DB::Timestamp gc_safepoint, double ratio_threshold, const LoggerPtr & log)
{
    // Always GC.
    if (ratio_threshold < 1.0)
        return true;

    const auto & property = seg->getStable()->getStableProperty();
    LOG_FMT_TRACE(log, "{}", property.toDebugString());
    // No data older than safe_point to GC.
    if (property.gc_hint_version > gc_safepoint)
        return false;
    // A lot of MVCC versions to GC.
    if (property.num_versions > property.num_rows * ratio_threshold)
        return true;
    // A lot of non-effective MVCC versions to GC.
    if (property.num_versions > property.num_puts * ratio_threshold)
        return true;
    return false;
}

bool shouldCompactDeltaWithStable(const DMContext & context, const SegmentSnapshotPtr & snap, const RowKeyRange & segment_range, double invalid_data_ratio_threshold, const LoggerPtr & log)
{
    auto actual_delete_range = snap->delta->getSquashDeleteRange().shrink(segment_range);
    if (actual_delete_range.none())
        return false;

    auto [delete_rows, delete_bytes] = snap->stable->getApproxRowsAndBytes(context, actual_delete_range);

    auto stable_rows = snap->stable->getRows();
    auto stable_bytes = snap->stable->getBytes();

    LOG_FMT_TRACE(log, "delete range rows [{}], delete_bytes [{}] stable_rows [{}] stable_bytes [{}]", delete_rows, delete_bytes, stable_rows, stable_bytes);

    // 1. for small tables, the data may just reside in delta and stable_rows may be 0,
    //   so the `=` in `>=` is needed to cover the scenario when set tiflash replica of small tables to 0.
    //   (i.e. `actual_delete_range` is not none, but `delete_rows` and `stable_rows` are both 0).
    // 2. the disadvantage of `=` in `>=` is that it may trigger an extra gc when write apply snapshot file to an empty segment,
    //   because before write apply snapshot file, it will write a delete range first, and will meet the following gc criteria.
    //   But the cost should be really minor because merge delta on an empty segment should be very fast.
    //   What's more, we can ignore this kind of delete range in future to avoid this extra gc.
    return (delete_rows >= stable_rows * invalid_data_ratio_threshold) || (delete_bytes >= stable_bytes * invalid_data_ratio_threshold);
}

std::unordered_set<UInt64> getDMFileIDs(const SegmentPtr & seg)
{
    std::unordered_set<UInt64> file_ids;
    // We get the file ids in the segment no matter it is abandoned or not,
    // because even it is abandoned, we don't know whether the new segment will ref the old dtfiles.
    // So we just be conservative here to return the old file ids.
    // This is ok because the next check will get the right file ids in such case.
    if (seg)
    {
        const auto & dm_files = seg->getStable()->getDMFiles();
        for (const auto & file : dm_files)
        {
            file_ids.emplace(file->fileId());
        }
    }
    return file_ids;
}

bool shouldCompactStableWithTooMuchDataOutOfSegmentRange(const DMContext & context, //
                                                         const SegmentPtr & seg,
                                                         const SegmentSnapshotPtr & snap,
                                                         const SegmentPtr & prev_seg,
                                                         const SegmentPtr & next_seg,
                                                         double invalid_data_ratio_threshold,
                                                         const LoggerPtr & log)
{
    std::unordered_set<UInt64> prev_segment_file_ids = getDMFileIDs(prev_seg);
    std::unordered_set<UInt64> next_segment_file_ids = getDMFileIDs(next_seg);
    auto [first_pack_included, last_pack_included] = snap->stable->isFirstAndLastPackIncludedInRange(context, seg->getRowKeyRange());
    // Do a quick check about whether the DTFile is completely included in the segment range
    if (first_pack_included && last_pack_included)
    {
        seg->setValidDataRatioChecked();
        return false;
    }
    bool contains_invalid_data = false;
    const auto & dt_files = snap->stable->getDMFiles();
    if (!first_pack_included)
    {
        auto first_file_id = dt_files[0]->fileId();
        if (prev_segment_file_ids.count(first_file_id) == 0)
        {
            contains_invalid_data = true;
        }
    }
    if (!last_pack_included)
    {
        auto last_file_id = dt_files[dt_files.size() - 1]->fileId();
        if (next_segment_file_ids.count(last_file_id) == 0)
        {
            contains_invalid_data = true;
        }
    }
    // Only try to compact the segment when there is data out of this segment range and is also not shared by neighbor segments.
    if (!contains_invalid_data)
    {
        return false;
    }

    size_t total_rows = 0;
    size_t total_bytes = 0;
    for (const auto & file : dt_files)
    {
        total_rows += file->getRows();
        total_bytes += file->getBytes();
    }
    auto valid_rows = snap->stable->getRows();
    auto valid_bytes = snap->stable->getBytes();

    LOG_FMT_TRACE(log, "valid_rows [{}], valid_bytes [{}] total_rows [{}] total_bytes [{}]", valid_rows, valid_bytes, total_rows, total_bytes);
    seg->setValidDataRatioChecked();
    return (valid_rows < total_rows * (1 - invalid_data_ratio_threshold)) || (valid_bytes < total_bytes * (1 - invalid_data_ratio_threshold));
}

} // namespace GC

SegmentPtr DeltaMergeStore::gcTrySegmentMerge(const DMContextPtr & dm_context, const SegmentPtr & segment)
{
    auto segment_rows = segment->getEstimatedRows();
    auto segment_bytes = segment->getEstimatedBytes();
    if (segment_rows >= dm_context->small_segment_rows || segment_bytes >= dm_context->small_segment_bytes)
    {
        LOG_FMT_TRACE(
            log,
            "GC - Merge skipped because current segment is not small, segment={} table={}",
            segment->simpleInfo(),
            table_name);
        return {};
    }

    auto segments_to_merge = getMergeableSegments(dm_context, segment);
    if (segments_to_merge.size() < 2)
    {
        LOG_FMT_TRACE(
            log,
            "GC - Merge skipped because cannot find adjacent segments to merge, segment={} table={}",
            segment->simpleInfo(),
            table_name);
        return {};
    }

    LOG_FMT_INFO(
        log,
        "GC - Trigger Merge, segment={} table={}",
        segment->simpleInfo(),
        table_name);
    auto new_segment = segmentMerge(*dm_context, segments_to_merge, false);
    if (new_segment)
    {
        checkSegmentUpdate(dm_context, segment, ThreadType::BG_GC);
    }

    return new_segment;
}

SegmentPtr DeltaMergeStore::gcTrySegmentMergeDelta(const DMContextPtr & dm_context, const SegmentPtr & segment, const SegmentPtr & prev_segment, const SegmentPtr & next_segment, DB::Timestamp gc_safe_point)
{
    SegmentSnapshotPtr segment_snap;
    {
        std::shared_lock lock(read_write_mutex);

        // The segment we just retrieved may be dropped from the map. Let's verify it again before creating a snapshot.
        if (!isSegmentValid(lock, segment))
        {
            LOG_FMT_TRACE(log, "GC - Skip checking MergeDelta because not valid, segment={} table={}", segment->simpleInfo(), table_name);
            return {};
        }

        segment_snap = segment->createSnapshot(*dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfDeltaMerge);
        if (!segment_snap)
        {
            LOG_FMT_TRACE(
                log,
                "GC - Skip checking MergeDelta because snapshot failed, segment={} table={}",
                segment->simpleInfo(),
                table_name);
            return {};
        }
    }

    RowKeyRange segment_range = segment->getRowKeyRange();

    // Check whether we should apply compact on this segment
    auto invalid_data_ratio_threshold = global_context.getSettingsRef().dt_bg_gc_delta_delete_ratio_to_trigger_gc;
    RUNTIME_ASSERT(invalid_data_ratio_threshold >= 0 && invalid_data_ratio_threshold <= 1);

    bool should_compact = false;
    GC::MergeDeltaReason compact_reason = GC::MergeDeltaReason::Unknown;

    if (GC::shouldCompactDeltaWithStable(
            *dm_context,
            segment_snap,
            segment_range,
            invalid_data_ratio_threshold,
            log))
    {
        should_compact = true;
        compact_reason = GC::MergeDeltaReason::TooManyDeleteRange;
    }

    if (!should_compact && segment->isValidDataRatioChecked())
    {
        if (GC::shouldCompactStableWithTooMuchDataOutOfSegmentRange(
                *dm_context,
                segment,
                segment_snap,
                prev_segment,
                next_segment,
                invalid_data_ratio_threshold,
                log))
        {
            should_compact = true;
            compact_reason = GC::MergeDeltaReason::TooMuchOutOfRange;
        }
    }

    if (!should_compact && (segment->getLastCheckGCSafePoint() < gc_safe_point))
    {
        // Avoid recheck this segment when gc_safe_point doesn't change regardless whether we trigger this segment's DeltaMerge or not.
        // Because after we calculate StableProperty and compare it with this gc_safe_point,
        // there is no need to recheck it again using the same gc_safe_point.
        // On the other hand, if it should do DeltaMerge using this gc_safe_point, and the DeltaMerge is interruptted by other process,
        // it's still worth to wait another gc_safe_point to check this segment again.
        segment->setLastCheckGCSafePoint(gc_safe_point);
        dm_context->min_version = gc_safe_point;

        // calculate StableProperty if needed
        if (!segment->getStable()->isStablePropertyCached())
            segment->getStable()->calculateStableProperty(*dm_context, segment_range, isCommonHandle());

        if (GC::shouldCompactStableWithTooManyInvalidVersion(
                segment,
                gc_safe_point,
                global_context.getSettingsRef().dt_bg_gc_ratio_threhold_to_trigger_gc,
                log))
        {
            should_compact = true;
            compact_reason = GC::MergeDeltaReason::TooManyInvalidVersion;
        }
    }

    if (!should_compact)
    {
        LOG_FMT_TRACE(
            log,
            "GC - MergeDelta skipped, segment={} table={}",
            segment->simpleInfo(),
            table_name);
        return {};
    }

    LOG_FMT_INFO(
        log,
        "GC - Trigger MergeDelta, compact_reason={} segment={} table={}",
        GC::toString(compact_reason),
        segment->simpleInfo(),
        table_name);
    auto new_segment = segmentMergeDelta(*dm_context, segment, MergeDeltaReason::BackgroundGCThread, segment_snap);

    if (!new_segment)
    {
        LOG_FMT_DEBUG(
            log,
            "GC - MergeDelta aborted, compact_reason={} segment={} table={}",
            GC::toString(compact_reason),
            segment->simpleInfo(),
            table_name);
        return {};
    }

    segment_snap = {};
    checkSegmentUpdate(dm_context, segment, ThreadType::BG_GC);

    return new_segment;
}

UInt64 DeltaMergeStore::onSyncGc(Int64 limit)
{
    if (shutdown_called.load(std::memory_order_relaxed))
        return 0;

    bool skip_update_safe_point = false;
    fiu_do_on(FailPoints::gc_skip_update_safe_point, {
        skip_update_safe_point = true;
    });
    if (!skip_update_safe_point)
    {
        if (!updateGCSafePoint())
            return 0;
    }

    {
        std::shared_lock lock(read_write_mutex);
        // avoid gc on empty tables
        if (segments.size() == 1)
        {
            const auto & seg = segments.begin()->second;
            if (seg->getEstimatedRows() == 0)
                return 0;
        }
    }

    DB::Timestamp gc_safe_point = latest_gc_safe_point.load(std::memory_order_acquire);
    LOG_FMT_TRACE(log,
                  "GC on table {} start with key: {}, gc_safe_point: {}, max gc limit: {}",
                  table_name,
                  next_gc_check_key.toDebugString(),
                  gc_safe_point,
                  limit);

    UInt64 check_segments_num = 0;
    Int64 gc_segments_num = 0;
    while (gc_segments_num < limit)
    {
        // If the store is shut down, give up running GC on it.
        if (shutdown_called.load(std::memory_order_relaxed))
            break;

        auto dm_context = newDMContext(global_context, global_context.getSettingsRef(), "onSyncGc");
        SegmentPtr segment;
        SegmentPtr prev_segment = nullptr;
        SegmentPtr next_segment = nullptr;
        {
            std::shared_lock lock(read_write_mutex);

            auto segment_it = segments.upper_bound(next_gc_check_key.toRowKeyValueRef());
            if (segment_it == segments.end())
                segment_it = segments.begin();

            // we have check all segments, stop here
            if (check_segments_num >= segments.size())
                break;
            check_segments_num++;

            segment = segment_it->second;
            next_gc_check_key = segment_it->first.toRowKeyValue();

            auto next_segment_it = next(segment_it, 1);
            if (next_segment_it != segments.end())
            {
                next_segment = next_segment_it->second;
            }
            if (segment_it != segments.begin())
            {
                auto prev_segment_it = prev(segment_it, 1);
                prev_segment = prev_segment_it->second;
            }
        }

        assert(segment != nullptr);
        if (segment->hasAbandoned())
            continue;

        try
        {
            SegmentPtr new_seg = nullptr;
            if (!new_seg)
                new_seg = gcTrySegmentMerge(dm_context, segment);
            if (!new_seg)
                new_seg = gcTrySegmentMergeDelta(dm_context, segment, prev_segment, next_segment, gc_safe_point);

            if (!new_seg)
            {
                LOG_FMT_TRACE(
                    log,
                    "GC - Skipped segment, segment={} table={}",
                    segment->simpleInfo(),
                    table_name);
                continue;
            }

            gc_segments_num++;
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format("Error while GC segment, segment={} table={}", segment->info(), table_name));
            e.rethrow();
        }
    }

    if (gc_segments_num != 0)
        LOG_FMT_DEBUG(log, "Finish GC, gc_segments_num={}", gc_segments_num);

    return gc_segments_num;
}

} // namespace DM
} // namespace DB
