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

        std::tie(new_left, new_right) = segment->applySplit(segment_lock, dm_context, segment_snap, wbs, split_info);

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

SegmentPtr DeltaMergeStore::segmentMerge(DMContext & dm_context, const std::vector<SegmentPtr> & ordered_segments, bool is_foreground)
{
    RUNTIME_CHECK(ordered_segments.size() >= 2, ordered_segments.size());

    LOG_FMT_INFO(
        log,
        "Merge - Begin, is_foreground={} safe_point={} segments_to_merge={}",
        is_foreground,
        dm_context.min_version,
        Segment::simpleInfo(ordered_segments));

    /// This segment may contain some rows that not belong to this segment range which is left by previous split operation.
    /// And only saved data in this segment will be filtered by the segment range in the merge process,
    /// unsaved data will be directly copied to the new segment.
    /// So we flush here to make sure that all potential data left by previous split operation is saved.
    for (const auto & seg : ordered_segments)
    {
        size_t sleep_ms = 5;
        while (true)
        {
            if (seg->hasAbandoned())
            {
                LOG_FMT_INFO(log, "Merge - Give up segmentMerge because segment abandoned, segment={}", seg->simpleInfo());
                return {};
            }

            if (seg->flushCache(dm_context))
                break;

            // Else: retry. Flush could fail. Typical cases:
            // #1. The segment is abandoned (due to an update is finished)
            // #2. There is another flush in progress, for example, triggered in background
            // Let's sleep 5ms ~ 100ms and then retry flush again.
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
            sleep_ms = std::min(sleep_ms * 2, 100);
        }
    }

    std::vector<SegmentSnapshotPtr> ordered_snapshots;
    ordered_snapshots.reserve(ordered_segments.size());
    ColumnDefinesPtr schema_snap;

    {
        std::shared_lock lock(read_write_mutex);

        for (const auto & seg : ordered_segments)
        {
            if (!isSegmentValid(lock, seg))
            {
                LOG_FMT_DEBUG(log, "Merge - Give up segmentMerge because not valid, segment={}", seg->simpleInfo());
                return {};
            }
        }

        for (const auto & seg : ordered_segments)
        {
            auto snap = seg->createSnapshot(dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfSegmentMerge);
            if (!snap)
            {
                LOG_FMT_DEBUG(log, "Merge - Give up segmentMerge because snapshot failed, segment={}", seg->simpleInfo());
                return {};
            }

            ordered_snapshots.emplace_back(snap);
        }

        schema_snap = store_columns;
    }

    // Not counting the early give up action.
    Int64 delta_bytes = 0;
    Int64 delta_rows = 0;
    for (const auto & snap : ordered_snapshots)
    {
        delta_bytes += static_cast<Int64>(snap->delta->getBytes());
        delta_rows += static_cast<Int64>(snap->delta->getRows());
    }

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

    WriteBatches wbs(*storage_pool, dm_context.getWriteLimiter());
    auto merged_stable = Segment::prepareMerge(dm_context, schema_snap, ordered_segments, ordered_snapshots, wbs);
    wbs.writeLogAndData();
    merged_stable->enableDMFilesGC();

    SegmentPtr merged;
    {
        std::unique_lock lock(read_write_mutex);

        for (const auto & seg : ordered_segments)
        {
            if (!isSegmentValid(lock, seg))
            {
                LOG_FMT_DEBUG(log, "Merge - Give up segmentMerge because not valid, segment={}", seg->simpleInfo());
                wbs.setRollback();
                return {};
            }
        }

        std::vector<Segment::Lock> locks;
        locks.reserve(ordered_segments.size());
        for (const auto & seg : ordered_segments)
            locks.emplace_back(seg->mustGetUpdateLock());

        merged = Segment::applyMerge(locks, dm_context, ordered_segments, ordered_snapshots, wbs, merged_stable);

        wbs.writeMeta();

        for (const auto & seg : ordered_segments)
        {
            seg->abandon(dm_context);
            segments.erase(seg->getRowKeyRange().getEnd());
            id_to_segment.erase(seg->segmentId());
        }

        segments.emplace(merged->getRowKeyRange().getEnd(), merged);
        id_to_segment.emplace(merged->segmentId(), merged);

        if constexpr (DM_RUN_CHECK)
            merged->check(dm_context, "After segment merge");

        LOG_FMT_INFO(
            log,
            "Merge - Finish, {} segments are merged into one, is_foreground={} merged={} segments_to_merge={}",
            ordered_segments.size(),
            is_foreground,
            merged->info(),
            Segment::info(ordered_segments));
    }

    wbs.writeRemoves();

    GET_METRIC(tiflash_storage_throughput_bytes, type_merge).Increment(delta_bytes);
    GET_METRIC(tiflash_storage_throughput_rows, type_merge).Increment(delta_rows);

    if constexpr (DM_RUN_CHECK)
        check(dm_context.db_context);

    return merged;
}

SegmentPtr DeltaMergeStore::segmentMergeDelta(
    DMContext & dm_context,
    const SegmentPtr & segment,
    const MergeDeltaReason reason,
    SegmentSnapshotPtr segment_snap)
{
    LOG_FMT_INFO(log, "MergeDelta - Begin, reason={} safe_point={} segment={}", toString(reason), dm_context.min_version, segment->info());

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

    switch (reason)
    {
    case MergeDeltaReason::BackgroundThreadPool:
        GET_METRIC(tiflash_storage_subtask_count, type_delta_merge_bg).Increment();
        break;
    case MergeDeltaReason::BackgroundGCThread:
        GET_METRIC(tiflash_storage_subtask_count, type_delta_merge_bg_gc).Increment();
        break;
    case MergeDeltaReason::ForegroundWrite:
        GET_METRIC(tiflash_storage_subtask_count, type_delta_merge_fg).Increment();
        break;
    case MergeDeltaReason::Manual:
        GET_METRIC(tiflash_storage_subtask_count, type_delta_merge_manual).Increment();
        break;
    default:
        break;
    }

    Stopwatch watch_delta_merge;
    SCOPE_EXIT({
        switch (reason)
        {
        case MergeDeltaReason::BackgroundThreadPool:
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_delta_merge_bg).Observe(watch_delta_merge.elapsedSeconds());
            break;
        case MergeDeltaReason::BackgroundGCThread:
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_delta_merge_bg_gc).Observe(watch_delta_merge.elapsedSeconds());
            break;
        case MergeDeltaReason::ForegroundWrite:
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_delta_merge_fg).Observe(watch_delta_merge.elapsedSeconds());
            break;
        case MergeDeltaReason::Manual:
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_delta_merge_manual).Observe(watch_delta_merge.elapsedSeconds());
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

        new_segment = segment->applyMergeDelta(segment_lock, dm_context, segment_snap, wbs, new_stable);

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
