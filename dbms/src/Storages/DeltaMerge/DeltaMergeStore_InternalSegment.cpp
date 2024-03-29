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

#include <Common/SyncPoint/SyncPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/WriteBatchesImpl.h>

#include <magic_enum.hpp>

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
extern const Metric DT_SnapshotOfSegmentIngest;
} // namespace CurrentMetrics

namespace DB
{
namespace DM
{
SegmentPair DeltaMergeStore::segmentSplit(
    DMContext & dm_context,
    const SegmentPtr & segment,
    SegmentSplitReason reason,
    std::optional<RowKeyValue> opt_split_at,
    SegmentSplitMode opt_split_mode)
{
    LOG_INFO(
        log,
        "Split - Begin, mode={} reason={}{} safe_point={} segment={}",
        magic_enum::enum_name(opt_split_mode),
        magic_enum::enum_name(reason),
        (opt_split_at.has_value() ? fmt::format(" force_split_at={}", opt_split_at->toDebugString()) : ""),
        dm_context.min_version,
        segment->info());

    SegmentSnapshotPtr segment_snap;
    ColumnDefinesPtr schema_snap;

    {
        std::shared_lock lock(read_write_mutex);

        if (!isSegmentValid(lock, segment))
        {
            LOG_DEBUG(log, "Split - Give up segmentSplit because not valid, segment={}", segment->simpleInfo());
            return {};
        }

        segment_snap
            = segment->createSnapshot(dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfSegmentSplit);
        if (!segment_snap)
        {
            LOG_DEBUG(log, "Split - Give up segmentSplit because snapshot failed, segment={}", segment->simpleInfo());
            return {};
        }
        if (!opt_split_at.has_value() && !segment_snap->getRows())
        {
            // When opt_split_at is not specified, we skip split for empty segments.
            LOG_DEBUG(log, "Split - Give up auto segmentSplit because no row, segment={}", segment->simpleInfo());
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
    switch (reason)
    {
    case SegmentSplitReason::ForegroundWrite:
        GET_METRIC(tiflash_storage_subtask_count, type_seg_split_fg).Increment();
        break;
    case SegmentSplitReason::Background:
        GET_METRIC(tiflash_storage_subtask_count, type_seg_split_bg).Increment();
        break;
    case SegmentSplitReason::ForIngest:
        GET_METRIC(tiflash_storage_subtask_count, type_seg_split_ingest).Increment();
        break;
    }

    Stopwatch watch_seg_split;
    SCOPE_EXIT({
        switch (reason)
        {
        case SegmentSplitReason::ForegroundWrite:
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_seg_split_fg)
                .Observe(watch_seg_split.elapsedSeconds());
            break;
        case SegmentSplitReason::Background:
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_seg_split_bg)
                .Observe(watch_seg_split.elapsedSeconds());
            break;
        case SegmentSplitReason::ForIngest:
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_seg_split_ingest)
                .Observe(watch_seg_split.elapsedSeconds());
            break;
        }
    });

    WriteBatches wbs(*storage_pool, dm_context.getWriteLimiter());

    Segment::SplitMode seg_split_mode;
    switch (opt_split_mode)
    {
    case SegmentSplitMode::Auto:
        seg_split_mode = Segment::SplitMode::Auto;
        break;
    case SegmentSplitMode::Logical:
        seg_split_mode = Segment::SplitMode::Logical;
        break;
    case SegmentSplitMode::Physical:
        seg_split_mode = Segment::SplitMode::Physical;
        break;
    default:
        seg_split_mode = Segment::SplitMode::Auto;
        break;
    }

    auto range = segment->getRowKeyRange();
    auto split_info_opt
        = segment->prepareSplit(dm_context, schema_snap, segment_snap, opt_split_at, seg_split_mode, wbs);

    if (!split_info_opt.has_value())
    {
        // Likely we can not find an appropriate split point for this segment later, forbid the split until this segment get updated through applying delta-merge. Or it will slow down the write a lot.
        segment->forbidSplit();
        LOG_WARNING(
            log,
            "Split - Give up segmentSplit and forbid later auto split because prepare split failed, segment={}",
            segment->simpleInfo());
        return {};
    }

    auto & split_info = split_info_opt.value();

    wbs.writeLogAndData();
    split_info.my_stable->enableDMFilesGC(dm_context);
    split_info.other_stable->enableDMFilesGC(dm_context);

    SegmentPtr new_left, new_right;
    {
        std::unique_lock lock(read_write_mutex);

        if (!isSegmentValid(lock, segment))
        {
            LOG_DEBUG(log, "Split - Give up segmentSplit because not valid, segment={}", segment->simpleInfo());
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

        LOG_INFO(
            log,
            "Split - {} - Finish, segment is split into two, old_segment={} new_left={} new_right={}",
            split_info.is_logical ? "SplitLogical" : "SplitPhysical",
            segment->info(),
            new_left->info(),
            new_right->info());
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
        check(dm_context.global_context);

    return {new_left, new_right};
}

SegmentPtr DeltaMergeStore::segmentMerge(
    DMContext & dm_context,
    const std::vector<SegmentPtr> & ordered_segments,
    SegmentMergeReason reason)
{
    RUNTIME_CHECK(ordered_segments.size() >= 2, ordered_segments.size());

    LOG_INFO(
        log,
        "Merge - Begin, reason={} safe_point={} segments_to_merge={}",
        magic_enum::enum_name(reason),
        dm_context.min_version,
        Segment::simpleInfo(ordered_segments));

    std::vector<SegmentSnapshotPtr> ordered_snapshots;
    ordered_snapshots.reserve(ordered_segments.size());
    ColumnDefinesPtr schema_snap;

    {
        std::shared_lock lock(read_write_mutex);

        for (const auto & seg : ordered_segments)
        {
            if (!isSegmentValid(lock, seg))
            {
                LOG_DEBUG(log, "Merge - Give up segmentMerge because not valid, segment={}", seg->simpleInfo());
                return {};
            }
        }

        for (const auto & seg : ordered_segments)
        {
            auto snap
                = seg->createSnapshot(dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfSegmentMerge);
            if (!snap)
            {
                LOG_DEBUG(log, "Merge - Give up segmentMerge because snapshot failed, segment={}", seg->simpleInfo());
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
    switch (reason)
    {
    case SegmentMergeReason::BackgroundGCThread:
        GET_METRIC(tiflash_storage_subtask_count, type_seg_merge_bg_gc).Increment();
        break;
    default:
        break;
    }
    Stopwatch watch_seg_merge;
    SCOPE_EXIT({
        switch (reason)
        {
        case SegmentMergeReason::BackgroundGCThread:
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_seg_merge_bg_gc)
                .Observe(watch_seg_merge.elapsedSeconds());
            break;
        default:
            break;
        }
    });

    WriteBatches wbs(*storage_pool, dm_context.getWriteLimiter());
    auto merged_stable = Segment::prepareMerge(dm_context, schema_snap, ordered_segments, ordered_snapshots, wbs);
    wbs.writeLogAndData();
    merged_stable->enableDMFilesGC(dm_context);

    SYNC_FOR("after_DeltaMergeStore::segmentMerge|prepare_merge");

    SegmentPtr merged;
    {
        std::unique_lock lock(read_write_mutex);

        for (const auto & seg : ordered_segments)
        {
            if (!isSegmentValid(lock, seg))
            {
                LOG_DEBUG(log, "Merge - Give up segmentMerge because not valid, segment={}", seg->simpleInfo());
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

        LOG_INFO(
            log,
            "Merge - Finish, {} segments are merged into one, reason={} merged={} segments_to_merge={}",
            ordered_segments.size(),
            magic_enum::enum_name(reason),
            merged->info(),
            Segment::info(ordered_segments));
    }

    wbs.writeRemoves();

    GET_METRIC(tiflash_storage_throughput_bytes, type_merge).Increment(delta_bytes);
    GET_METRIC(tiflash_storage_throughput_rows, type_merge).Increment(delta_rows);

    if constexpr (DM_RUN_CHECK)
        check(dm_context.global_context);

    return merged;
}

SegmentPtr DeltaMergeStore::segmentMergeDelta(
    DMContext & dm_context,
    const SegmentPtr & segment,
    const MergeDeltaReason reason,
    SegmentSnapshotPtr segment_snap)
{
    LOG_INFO(
        log,
        "MergeDelta - Begin, reason={} safe_point={} segment={}",
        magic_enum::enum_name(reason),
        dm_context.min_version,
        segment->info());

    ColumnDefinesPtr schema_snap;

    {
        std::shared_lock lock(read_write_mutex);

        if (!isSegmentValid(lock, segment))
        {
            LOG_DEBUG(
                log,
                "MergeDelta - Give up segmentMergeDelta because segment not valid, segment={}",
                segment->simpleInfo());
            return {};
        }

        // Try to generate a new snapshot if there is no pre-allocated one
        if (!segment_snap)
            segment_snap
                = segment->createSnapshot(dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfDeltaMerge);

        if (unlikely(!segment_snap))
        {
            LOG_DEBUG(
                log,
                "MergeDelta - Give up segmentMergeDelta because snapshot failed, segment={}",
                segment->simpleInfo());
            return {};
        }
        schema_snap = store_columns;
    }

    // Not counting the early give up action.
    auto delta_bytes = static_cast<Int64>(segment_snap->delta->getBytes());
    auto delta_rows = static_cast<Int64>(segment_snap->delta->getRows());

    CurrentMetrics::Increment cur_dm_segments{CurrentMetrics::DT_DeltaMerge};
    CurrentMetrics::Increment cur_dm_total_bytes{
        CurrentMetrics::DT_DeltaMergeTotalBytes,
        static_cast<Int64>(segment_snap->getBytes())};
    CurrentMetrics::Increment cur_dm_total_rows{
        CurrentMetrics::DT_DeltaMergeTotalRows,
        static_cast<Int64>(segment_snap->getRows())};

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
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_delta_merge_bg)
                .Observe(watch_delta_merge.elapsedSeconds());
            break;
        case MergeDeltaReason::BackgroundGCThread:
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_delta_merge_bg_gc)
                .Observe(watch_delta_merge.elapsedSeconds());
            break;
        case MergeDeltaReason::ForegroundWrite:
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_delta_merge_fg)
                .Observe(watch_delta_merge.elapsedSeconds());
            break;
        case MergeDeltaReason::Manual:
            GET_METRIC(tiflash_storage_subtask_duration_seconds, type_delta_merge_manual)
                .Observe(watch_delta_merge.elapsedSeconds());
            break;
        default:
            break;
        }
    });

    WriteBatches wbs(*storage_pool, dm_context.getWriteLimiter());

    auto new_stable = segment->prepareMergeDelta(dm_context, schema_snap, segment_snap, wbs);
    wbs.writeLogAndData();
    new_stable->enableDMFilesGC(dm_context);

    SegmentPtr new_segment;
    {
        std::unique_lock read_write_lock(read_write_mutex);

        if (!isSegmentValid(read_write_lock, segment))
        {
            LOG_DEBUG(
                log,
                "MergeDelta - Give up segmentMergeDelta because segment not valid, segment={}",
                segment->simpleInfo());
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

        LOG_INFO(
            log,
            "MergeDelta - Finish, delta is merged, old_segment={} new_segment={}",
            segment->info(),
            new_segment->info());
    }

    wbs.writeRemoves();

    GET_METRIC(tiflash_storage_throughput_bytes, type_delta_merge).Increment(delta_bytes);
    GET_METRIC(tiflash_storage_throughput_rows, type_delta_merge).Increment(delta_rows);

    if constexpr (DM_RUN_CHECK)
        check(dm_context.global_context);

    return new_segment;
}

SegmentPtr DeltaMergeStore::segmentIngestData(
    DMContext & dm_context,
    const SegmentPtr & segment,
    const DMFilePtr & data_file,
    bool clear_all_data_in_segment)
{
    LOG_INFO(
        log,
        "IngestData - Begin, data_file=dmf_{} clear_all_data_in_seg={} segment={}",
        data_file->fileId(),
        clear_all_data_in_segment,
        segment->info());

    SegmentSnapshotPtr snapshot;
    {
        std::shared_lock lock(read_write_mutex);
        if (!isSegmentValid(lock, segment))
        {
            LOG_DEBUG(
                log,
                "IngestData - Give up segmentIngestData because segment not valid, segment={}",
                segment->simpleInfo());
            return {};
        }

        if (!clear_all_data_in_segment)
        {
            // When clear_data == false, we need a snapshot to decide which column files to be replaced
            snapshot = segment->createSnapshot(
                dm_context,
                /* for_update */ true,
                CurrentMetrics::DT_SnapshotOfSegmentIngest);
            if (!snapshot)
            {
                LOG_DEBUG(
                    log,
                    "IngestData - Give up segmentIngestData because snapshot failed, segment={}",
                    segment->simpleInfo());
                return {};
            }
        }
    }

    Segment::IngestDataInfo ingest_info;
    if (clear_all_data_in_segment)
        ingest_info = segment->prepareIngestDataWithClearData();
    else
        ingest_info = segment->prepareIngestDataWithPreserveData(dm_context, snapshot);

    SegmentPtr new_segment{};
    {
        std::unique_lock lock(read_write_mutex);
        if (!isSegmentValid(lock, segment))
        {
            LOG_DEBUG(
                log,
                "IngestData - Give up segmentIngestData because segment not valid, segment={}",
                segment->simpleInfo());
            return {};
        }

        auto segment_lock = segment->mustGetUpdateLock();
        // Note: applyIngestData itself writes the wbs, and we don't need to pass a wbs by ourselves.
        auto apply_result = segment->applyIngestData(segment_lock, dm_context, data_file, ingest_info);

        if (apply_result.get() != segment.get())
        {
            // A new segment is created, we should abandon the current one.

            new_segment = apply_result;

            RUNTIME_CHECK(
                segment->getRowKeyRange().getEnd() == new_segment->getRowKeyRange().getEnd(),
                segment->info(),
                new_segment->info());
            RUNTIME_CHECK(segment->segmentId() == new_segment->segmentId(), segment->info(), new_segment->info());

            segment->abandon(dm_context);
            segments[segment->getRowKeyRange().getEnd()] = new_segment;
            id_to_segment[segment->segmentId()] = new_segment;

            LOG_INFO(
                log,
                "IngestData - Finish, new segment is created, old_segment={} new_segment={}",
                segment->info(),
                new_segment->info());
        }
        else if (apply_result.get() == segment.get())
        {
            LOG_INFO(log, "IngestData - Finish, ingested to existing segment's delta, segment={}", segment->info());

            return segment;
        }
        else if (apply_result == nullptr)
        {
            // This should not happen, because we have verified segment is not abandoned.
            RUNTIME_CHECK_MSG(false, "applyIngestData should not fail");
        }
        else
        {
            RUNTIME_CHECK_MSG(false, "applyIngestData returns unexpected result");
        }
    }

    if constexpr (DM_RUN_CHECK)
        check(dm_context.global_context);

    return new_segment;
}

SegmentPtr DeltaMergeStore::segmentDangerouslyReplaceDataFromCheckpoint(
    DMContext & dm_context,
    const SegmentPtr & segment,
    const DMFilePtr & data_file,
    const ColumnFilePersisteds & column_file_persisteds)
{
    LOG_INFO(
        log,
        "ReplaceData - Begin, segment={} data_file={} column_files_num={}",
        segment->info(),
        data_file->path(),
        column_file_persisteds.size());

    WriteBatches wbs(*storage_pool, dm_context.getWriteLimiter());

    SegmentPtr new_segment;
    {
        std::unique_lock lock(read_write_mutex);
        if (!isSegmentValid(lock, segment))
        {
            LOG_DEBUG(
                log,
                "ReplaceData - Give up segment replace data because segment not valid, segment={} data_file={}",
                segment->simpleInfo(),
                data_file->path());
            return {};
        }

        auto segment_lock = segment->mustGetUpdateLock();
        new_segment = segment->dangerouslyReplaceDataFromCheckpoint(
            segment_lock,
            dm_context,
            data_file,
            wbs,
            column_file_persisteds);

        RUNTIME_CHECK(
            segment->getRowKeyRange().getEnd() == new_segment->getRowKeyRange().getEnd(),
            segment->info(),
            new_segment->info());
        RUNTIME_CHECK(segment->segmentId() == new_segment->segmentId(), segment->info(), new_segment->info());

        wbs.writeLogAndData();
        wbs.writeMeta();

        segment->abandon(dm_context);
        segments[segment->getRowKeyRange().getEnd()] = new_segment;
        id_to_segment[segment->segmentId()] = new_segment;

        LOG_INFO(log, "ReplaceData - Finish, old_segment={} new_segment={}", segment->info(), new_segment->info());
    }

    wbs.writeRemoves();

    if constexpr (DM_RUN_CHECK)
        check(dm_context.global_context);

    return new_segment;
}

bool DeltaMergeStore::doIsSegmentValid(const SegmentPtr & segment)
{
    if (segment->hasAbandoned())
    {
        LOG_DEBUG(log, "Segment instance is abandoned, segment={}", segment->simpleInfo());
        return false;
    }
    // Segment instance could have been removed or replaced.
    auto it = segments.find(segment->getRowKeyRange().getEnd());
    if (it == segments.end())
    {
        LOG_DEBUG(log, "Segment not found in segment map, segment={}", segment->simpleInfo());

        auto it2 = id_to_segment.find(segment->segmentId());
        if (it2 != id_to_segment.end())
        {
            LOG_DEBUG(
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
        LOG_DEBUG(log, "Segment instance has been replaced in segment map, segment={}", segment->simpleInfo());
        return false;
    }
    return true;
}

} // namespace DM

} // namespace DB
