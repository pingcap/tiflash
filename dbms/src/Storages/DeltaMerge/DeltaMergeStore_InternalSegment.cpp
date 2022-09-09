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

#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Segment.h>

namespace CurrentMetrics
{
extern const Metric DT_DeltaMerge;
extern const Metric DT_DeltaMergeTotalBytes;
extern const Metric DT_DeltaMergeTotalRows;
extern const Metric DT_SegmentSplit;
extern const Metric DT_SegmentMerge;
extern const Metric DT_SnapshotOfSegmentSplit;
extern const Metric DT_SnapshotOfSegmentMerge;
extern const Metric DT_SnapshotOfDeltaMerge;
} // namespace CurrentMetrics

namespace DB
{

namespace DM
{

SegmentPair DeltaMergeStore::segmentSplit(DMContext & dm_context, const SegmentPtr & segment, bool is_foreground)
{
    LOG_FMT_INFO(
        log,
        "Split - Begin, is_foreground={} safe_point={} segment={}",
        is_foreground,
        dm_context.min_version,
        segment->info());

    SegmentSnapshotPtr segment_snap;
    ColumnDefinesPtr schema_snap;

    {
        std::shared_lock lock(read_write_mutex);

        if (!isSegmentValid(lock, segment))
        {
            LOG_FMT_DEBUG(log, "Split - Give up segmentSplit because not valid, segment={}", segment->simpleInfo());
            return {};
        }

        segment_snap = segment->createSnapshot(dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfSegmentSplit);
        if (!segment_snap || !segment_snap->getRows())
        {
            LOG_FMT_DEBUG(log, "Split - Give up segmentSplit because snapshot failed or no row, segment={}", segment->simpleInfo());
            return {};
        }
        schema_snap = store_columns;
    }

    // Not counting the early give up action.
    auto delta_bytes = static_cast<Int64>(segment_snap->delta->getBytes());
    auto delta_rows = static_cast<Int64>(segment_snap->delta->getRows());

    size_t duplicated_bytes = 0;
    size_t duplicated_rows = 0;

    CurrentMetrics::Increment cur_dm_segments{CurrentMetrics::DT_SegmentSplit};
    if (is_foreground)
        GET_METRIC(tiflash_storage_subtask_count, type_seg_split_fg).Increment();
    else
        GET_METRIC(tiflash_storage_subtask_count, type_seg_split).Increment();
    Stopwatch watch_seg_split;
    SCOPE_EXIT({
        if (is_foreground)
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_seg_split_fg).Observe(watch_seg_split.elapsedSeconds());
        else
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_seg_split).Observe(watch_seg_split.elapsedSeconds());
    });

    WriteBatches wbs(*storage_pool, dm_context.getWriteLimiter());

    auto range = segment->getRowKeyRange();
    auto split_info_opt = segment->prepareSplit(dm_context, schema_snap, segment_snap, wbs);

    if (!split_info_opt.has_value())
    {
        // Likely we can not find an appropriate split point for this segment later, forbid the split until this segment get updated through applying delta-merge. Or it will slow down the write a lot.
        segment->forbidSplit();
        LOG_FMT_WARNING(log, "Split - Give up segmentSplit and forbid later split because of prepare split failed, segment={}", segment->simpleInfo());
        return {};
    }

    auto & split_info = split_info_opt.value();

    wbs.writeLogAndData();
    split_info.my_stable->enableDMFilesGC();
    split_info.other_stable->enableDMFilesGC();

    SegmentPtr new_left, new_right;
    {
        std::unique_lock lock(read_write_mutex);

        if (!isSegmentValid(lock, segment))
        {
            LOG_FMT_DEBUG(log, "Split - Give up segmentSplit because not valid, segment={}", segment->simpleInfo());
            wbs.setRollback();
            return {};
        }

        auto segment_lock = segment->mustGetUpdateLock();

        std::tie(new_left, new_right) = segment->applySplit(dm_context, segment_snap, wbs, split_info);

        wbs.writeMeta();

        segment->abandon(dm_context);
        segments.erase(range.getEnd());
        id_to_segment.erase(segment->segmentId());

        segments[new_left->getRowKeyRange().getEnd()] = new_left;
        segments[new_right->getRowKeyRange().getEnd()] = new_right;

        id_to_segment.emplace(new_left->segmentId(), new_left);
        id_to_segment.emplace(new_right->segmentId(), new_right);

        if constexpr (DM_RUN_CHECK)
        {
            new_left->check(dm_context, "After split left");
            new_right->check(dm_context, "After split right");
        }

        duplicated_bytes = new_left->getDelta()->getBytes();
        duplicated_rows = new_right->getDelta()->getBytes();

        LOG_FMT_INFO(log, "Split - {} - Finish, segment is split into two, old_segment={} new_left={} new_right={}", split_info.is_logical ? "SplitLogical" : "SplitPhysical", segment->info(), new_left->info(), new_right->info());
    }

    wbs.writeRemoves();

    if (!split_info.is_logical)
    {
        GET_METRIC(tiflash_storage_throughput_bytes, type_split).Increment(delta_bytes);
        GET_METRIC(tiflash_storage_throughput_rows, type_split).Increment(delta_rows);
    }
    else
    {
        // For logical split, delta is duplicated into two segments. And will be merged into stable twice later. So we need to decrease it here.
        // Otherwise the final total delta merge bytes is greater than bytes written into.
        GET_METRIC(tiflash_storage_throughput_bytes, type_split).Decrement(duplicated_bytes);
        GET_METRIC(tiflash_storage_throughput_rows, type_split).Decrement(duplicated_rows);
    }

    if constexpr (DM_RUN_CHECK)
        check(dm_context.db_context);

    return {new_left, new_right};
}

void DeltaMergeStore::segmentMerge(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right, bool is_foreground)
{
    LOG_FMT_INFO(
        log,
        "Merge - Begin, is_foreground={} safe_point={} left={} right={}",
        is_foreground,
        dm_context.min_version,
        left->info(),
        right->info());

    /// This segment may contain some rows that not belong to this segment range which is left by previous split operation.
    /// And only saved data in this segment will be filtered by the segment range in the merge process,
    /// unsaved data will be directly copied to the new segment.
    /// So we flush here to make sure that all potential data left by previous split operation is saved.
    while (!left->flushCache(dm_context))
    {
        // keep flush until success if not abandoned
        if (left->hasAbandoned())
        {
            LOG_FMT_INFO(log, "Merge - Give up segmentMerge because left abandoned, left={} right={}", left->simpleInfo(), right->simpleInfo());
            return;
        }
    }
    while (!right->flushCache(dm_context))
    {
        // keep flush until success if not abandoned
        if (right->hasAbandoned())
        {
            LOG_FMT_DEBUG(log, "Merge - Give up segmentMerge because right abandoned, left={} right={}", left->simpleInfo(), right->simpleInfo());
            return;
        }
    }

    SegmentSnapshotPtr left_snap;
    SegmentSnapshotPtr right_snap;
    ColumnDefinesPtr schema_snap;

    {
        std::shared_lock lock(read_write_mutex);

        if (!isSegmentValid(lock, left))
        {
            LOG_FMT_DEBUG(log, "Merge - Give up segmentMerge because left not valid, left={} right={}", left->simpleInfo(), right->simpleInfo());
            return;
        }
        if (!isSegmentValid(lock, right))
        {
            LOG_FMT_DEBUG(log, "Merge - Give up segmentMerge because right not valid, left={} right={}", left->simpleInfo(), right->simpleInfo());
            return;
        }

        left_snap = left->createSnapshot(dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfSegmentMerge);
        right_snap = right->createSnapshot(dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfSegmentMerge);

        if (!left_snap || !right_snap)
        {
            LOG_FMT_DEBUG(log, "Merge - Give up segmentMerge because snapshot failed, left={} right={}", left->simpleInfo(), right->simpleInfo());
            return;
        }
        schema_snap = store_columns;
    }

    // Not counting the early give up action.
    auto delta_bytes = static_cast<Int64>(left_snap->delta->getBytes()) + right_snap->delta->getBytes();
    auto delta_rows = static_cast<Int64>(left_snap->delta->getRows()) + right_snap->delta->getRows();

    CurrentMetrics::Increment cur_dm_segments{CurrentMetrics::DT_SegmentMerge};
    if (is_foreground)
        GET_METRIC(tiflash_storage_subtask_count, type_seg_merge_fg).Increment();
    else
        GET_METRIC(tiflash_storage_subtask_count, type_seg_merge).Increment();
    Stopwatch watch_seg_merge;
    SCOPE_EXIT({
        if (is_foreground)
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_seg_merge_fg).Observe(watch_seg_merge.elapsedSeconds());
        else
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_seg_merge).Observe(watch_seg_merge.elapsedSeconds());
    });

    auto left_range = left->getRowKeyRange();
    auto right_range = right->getRowKeyRange();

    WriteBatches wbs(*storage_pool, dm_context.getWriteLimiter());
    auto merged_stable = Segment::prepareMerge(dm_context, schema_snap, left, left_snap, right, right_snap, wbs);
    wbs.writeLogAndData();
    merged_stable->enableDMFilesGC();

    {
        std::unique_lock lock(read_write_mutex);

        if (!isSegmentValid(lock, left) || !isSegmentValid(lock, right))
        {
            LOG_FMT_DEBUG(log, "Merge - Give up segmentMerge because left or right not valid, left={} right={}", left->simpleInfo(), right->simpleInfo());
            wbs.setRollback();
            return;
        }

        auto left_lock = left->mustGetUpdateLock();
        auto right_lock = right->mustGetUpdateLock();

        auto merged = Segment::applyMerge(dm_context, left, left_snap, right, right_snap, wbs, merged_stable);

        wbs.writeMeta();

        left->abandon(dm_context);
        right->abandon(dm_context);
        segments.erase(left_range.getEnd());
        segments.erase(right_range.getEnd());
        id_to_segment.erase(left->segmentId());
        id_to_segment.erase(right->segmentId());

        segments.emplace(merged->getRowKeyRange().getEnd(), merged);
        id_to_segment.emplace(merged->segmentId(), merged);

        if constexpr (DM_RUN_CHECK)
            merged->check(dm_context, "After segment merge");

        LOG_FMT_INFO(
            log,
            "Merge - Finish, two segments are merged into one, is_foreground={} left={} right={} merged={}",
            is_foreground,
            dm_context.min_version,
            left->info(),
            right->info(),
            merged->info());
    }

    wbs.writeRemoves();

    GET_METRIC(tiflash_storage_throughput_bytes, type_merge).Increment(delta_bytes);
    GET_METRIC(tiflash_storage_throughput_rows, type_merge).Increment(delta_rows);

    if constexpr (DM_RUN_CHECK)
        check(dm_context.db_context);
}

SegmentPtr DeltaMergeStore::segmentMergeDelta(
    DMContext & dm_context,
    const SegmentPtr & segment,
    const TaskRunThread run_thread,
    SegmentSnapshotPtr segment_snap)
{
    LOG_FMT_INFO(log, "MergeDelta - Begin, thread={} safe_point={} segment={}", toString(run_thread), dm_context.min_version, segment->info());

    ColumnDefinesPtr schema_snap;

    {
        std::shared_lock lock(read_write_mutex);

        if (!isSegmentValid(lock, segment))
        {
            LOG_FMT_DEBUG(log, "MergeDelta - Give up segmentMergeDelta because segment not valid, segment={}", segment->simpleInfo());
            return {};
        }

        // Try to generate a new snapshot if there is no pre-allocated one
        if (!segment_snap)
            segment_snap = segment->createSnapshot(dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfDeltaMerge);

        if (unlikely(!segment_snap))
        {
            LOG_FMT_DEBUG(log, "MergeDelta - Give up segmentMergeDelta because snapshot failed, segment={}", segment->simpleInfo());
            return {};
        }
        schema_snap = store_columns;
    }

    // Not counting the early give up action.
    auto delta_bytes = static_cast<Int64>(segment_snap->delta->getBytes());
    auto delta_rows = static_cast<Int64>(segment_snap->delta->getRows());

    CurrentMetrics::Increment cur_dm_segments{CurrentMetrics::DT_DeltaMerge};
    CurrentMetrics::Increment cur_dm_total_bytes{CurrentMetrics::DT_DeltaMergeTotalBytes, static_cast<Int64>(segment_snap->getBytes())};
    CurrentMetrics::Increment cur_dm_total_rows{CurrentMetrics::DT_DeltaMergeTotalRows, static_cast<Int64>(segment_snap->getRows())};

    switch (run_thread)
    {
    case TaskRunThread::BackgroundThreadPool:
        GET_METRIC(tiflash_storage_subtask_count, type_delta_merge).Increment();
        break;
    case TaskRunThread::Foreground:
        GET_METRIC(tiflash_storage_subtask_count, type_delta_merge_fg).Increment();
        break;
    case TaskRunThread::ForegroundRPC:
        GET_METRIC(tiflash_storage_subtask_count, type_delta_merge_fg_rpc).Increment();
        break;
    case TaskRunThread::BackgroundGCThread:
        GET_METRIC(tiflash_storage_subtask_count, type_delta_merge_bg_gc).Increment();
        break;
    default:
        break;
    }

    Stopwatch watch_delta_merge;
    SCOPE_EXIT({
        switch (run_thread)
        {
        case TaskRunThread::BackgroundThreadPool:
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_delta_merge).Observe(watch_delta_merge.elapsedSeconds());
            break;
        case TaskRunThread::Foreground:
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_delta_merge_fg).Observe(watch_delta_merge.elapsedSeconds());
            break;
        case TaskRunThread::ForegroundRPC:
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_delta_merge_fg_rpc).Observe(watch_delta_merge.elapsedSeconds());
            break;
        case TaskRunThread::BackgroundGCThread:
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_delta_merge_bg_gc).Observe(watch_delta_merge.elapsedSeconds());
            break;
        default:
            break;
        }
    });

    WriteBatches wbs(*storage_pool, dm_context.getWriteLimiter());

    auto new_stable = segment->prepareMergeDelta(dm_context, schema_snap, segment_snap, wbs);
    wbs.writeLogAndData();
    new_stable->enableDMFilesGC();

    SegmentPtr new_segment;
    {
        std::unique_lock read_write_lock(read_write_mutex);

        if (!isSegmentValid(read_write_lock, segment))
        {
            LOG_FMT_DEBUG(log, "MergeDelta - Give up segmentMergeDelta because segment not valid, segment={}", segment->simpleInfo());
            wbs.setRollback();
            return {};
        }

        auto segment_lock = segment->mustGetUpdateLock();

        new_segment = segment->applyMergeDelta(dm_context, segment_snap, wbs, new_stable);

        wbs.writeMeta();


        // The instance of PKRange::End is closely linked to instance of PKRange. So we cannot reuse it.
        // Replace must be done by erase + insert.
        segments.erase(segment->getRowKeyRange().getEnd());
        id_to_segment.erase(segment->segmentId());

        segments[new_segment->getRowKeyRange().getEnd()] = new_segment;
        id_to_segment[new_segment->segmentId()] = new_segment;

        segment->abandon(dm_context);

        if constexpr (DM_RUN_CHECK)
        {
            new_segment->check(dm_context, "After segmentMergeDelta");
        }

        LOG_FMT_INFO(log, "MergeDelta - Finish, delta is merged, old_segment={} new_segment={}", segment->info(), new_segment->info());
    }

    wbs.writeRemoves();

    GET_METRIC(tiflash_storage_throughput_bytes, type_delta_merge).Increment(delta_bytes);
    GET_METRIC(tiflash_storage_throughput_rows, type_delta_merge).Increment(delta_rows);

    if constexpr (DM_RUN_CHECK)
        check(dm_context.db_context);

    return new_segment;
}

bool DeltaMergeStore::doIsSegmentValid(const SegmentPtr & segment)
{
    if (segment->hasAbandoned())
    {
        LOG_FMT_DEBUG(log, "Segment instance is abandoned, segment={}", segment->simpleInfo());
        return false;
    }
    // Segment instance could have been removed or replaced.
    auto it = segments.find(segment->getRowKeyRange().getEnd());
    if (it == segments.end())
    {
        LOG_FMT_DEBUG(log, "Segment not found in segment map, segment={}", segment->simpleInfo());

        auto it2 = id_to_segment.find(segment->segmentId());
        if (it2 != id_to_segment.end())
        {
            LOG_FMT_DEBUG(
                log,
                "Found segment with same id in id_to_segment, found_segment={} my_segment={}",
                it2->second->info(),
                segment->info());
        }
        return false;
    }
    auto & cur_segment = it->second;
    if (cur_segment.get() != segment.get())
    {
        LOG_FMT_DEBUG(log, "Segment instance has been replaced in segment map, segment={}", segment->simpleInfo());
        return false;
    }
    return true;
}

} // namespace DM

} // namespace DB
