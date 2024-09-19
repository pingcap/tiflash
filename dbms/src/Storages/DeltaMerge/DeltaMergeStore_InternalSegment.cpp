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
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileIndexWriter.h>
#include <Storages/DeltaMerge/LocalIndexerScheduler.h>
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
extern const Metric DT_SnapshotOfSegmentIngestIndex;
} // namespace CurrentMetrics

namespace DB::DM
{

void DeltaMergeStore::DMFileIDToSegmentIDs::remove(const SegmentPtr & segment)
{
    RUNTIME_CHECK(segment != nullptr);
    for (const auto & dmfile : segment->getStable()->getDMFiles())
    {
        if (auto it = u_map.find(dmfile->fileId()); it != u_map.end())
        {
            it->second.erase(segment->segmentId());
        }
    }
}

void DeltaMergeStore::DMFileIDToSegmentIDs::add(const SegmentPtr & segment)
{
    RUNTIME_CHECK(segment != nullptr);
    for (const auto & dmfile : segment->getStable()->getDMFiles())
    {
        u_map[dmfile->fileId()].insert(segment->segmentId());
    }
}

const DeltaMergeStore::DMFileIDToSegmentIDs::Value & DeltaMergeStore::DMFileIDToSegmentIDs::get(
    PageIdU64 dmfile_id) const
{
    static const Value empty;
    if (auto it = u_map.find(dmfile_id); it != u_map.end())
    {
        return it->second;
    }
    return empty;
}

void DeltaMergeStore::removeSegment(std::unique_lock<std::shared_mutex> &, const SegmentPtr & segment)
{
    segments.erase(segment->getRowKeyRange().getEnd());
    id_to_segment.erase(segment->segmentId());
    dmfile_id_to_segment_ids.remove(segment);
}

void DeltaMergeStore::addSegment(std::unique_lock<std::shared_mutex> &, const SegmentPtr & segment)
{
    RUNTIME_CHECK_MSG(
        !segments.contains(segment->getRowKeyRange().getEnd()),
        "Trying to add segment {} but there is a segment with the same key exists. Old segment must be removed "
        "before adding new.",
        segment->simpleInfo());
    segments[segment->getRowKeyRange().getEnd()] = segment;
    id_to_segment[segment->segmentId()] = segment;
    dmfile_id_to_segment_ids.add(segment);
}

void DeltaMergeStore::replaceSegment(
    std::unique_lock<std::shared_mutex> &,
    const SegmentPtr & old_segment,
    const SegmentPtr & new_segment)
{
    RUNTIME_CHECK(
        old_segment->segmentId() == new_segment->segmentId(),
        old_segment->segmentId(),
        new_segment->segmentId());
    segments.erase(old_segment->getRowKeyRange().getEnd());
    dmfile_id_to_segment_ids.remove(old_segment);

    segments[new_segment->getRowKeyRange().getEnd()] = new_segment;
    id_to_segment[new_segment->segmentId()] = new_segment;
    dmfile_id_to_segment_ids.add(new_segment);
}

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

        removeSegment(lock, segment);
        addSegment(lock, new_left);
        addSegment(lock, new_right);

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

    // For logical split, no new DMFile is created, new_left and new_right share the same DMFile with the old segment.
    // Even if the index build process of the old segment is not finished, after it is finished,
    // it will also trigger the new_left and new_right to bump the meta version.
    // So there is no need to check the local index update for logical split.
    if (!split_info.is_logical)
    {
        segmentEnsureStableIndexAsync(new_left);
        segmentEnsureStableIndexAsync(new_right);
    }

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
            removeSegment(lock, seg);
        }

        addSegment(lock, merged);

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

    segmentEnsureStableIndexAsync(merged);
    return merged;
}

void DeltaMergeStore::checkAllSegmentsLocalIndex()
{
    if (!local_index_infos || local_index_infos->empty())
        return;

    LOG_INFO(log, "CheckAllSegmentsLocalIndex - Begin");

    size_t segments_updated_meta = 0;
    auto dm_context = newDMContext(global_context, global_context.getSettingsRef(), "checkAllSegmentsLocalIndex");

    // 1. Make all segments referencing latest meta version.
    {
        Stopwatch watch;
        std::unique_lock lock(read_write_mutex);

        std::map<PageIdU64, DMFilePtr> latest_dmf_by_id;
        for (const auto & [end, segment] : segments)
        {
            UNUSED(end);
            for (const auto & dm_file : segment->getStable()->getDMFiles())
            {
                auto & latest_dmf = latest_dmf_by_id[dm_file->fileId()];
                if (!latest_dmf || dm_file->metaVersion() > latest_dmf->metaVersion())
                    // Note: pageId could be different. It is fine.
                    latest_dmf = dm_file;
            }
        }
        for (const auto & [end, segment] : segments)
        {
            UNUSED(end);
            for (const auto & dm_file : segment->getStable()->getDMFiles())
            {
                auto & latest_dmf = latest_dmf_by_id.at(dm_file->fileId());
                if (dm_file->metaVersion() < latest_dmf->metaVersion())
                {
                    // Note: pageId could be different. It is fine, replaceStableMetaVersion will fix it.
                    auto update_result = segmentUpdateMeta(lock, *dm_context, segment, {latest_dmf});
                    RUNTIME_CHECK(update_result != nullptr, segment->simpleInfo());
                    ++segments_updated_meta;
                }
            }
        }
        LOG_INFO(
            log,
            "CheckAllSegmentsLocalIndex - Finish, updated_meta={}, elapsed={:.3f}s",
            segments_updated_meta,
            watch.elapsedSeconds());
    }

    size_t segments_missing_indexes = 0;

    // 2. Trigger ensureStableIndex for all segments.
    // There could be new segments between 1 and 2, which is fine. New segments
    // will invoke ensureStableIndex at creation time.
    {
        // There must be a lock, because segments[] may be mutated.
        // And one lock for all is fine, because segmentEnsureStableIndexAsync is non-blocking, it
        // simply put tasks in the background.
        std::shared_lock lock(read_write_mutex);
        for (const auto & [end, segment] : segments)
        {
            UNUSED(end);
            if (segmentEnsureStableIndexAsync(segment))
                ++segments_missing_indexes;
        }
    }

    LOG_INFO(
        log,
        "CheckAllSegmentsLocalIndex - Finish, segments_[updated_meta/missing_index]={}/{}",
        segments_updated_meta,
        segments_missing_indexes);
}

bool DeltaMergeStore::segmentEnsureStableIndexAsync(const SegmentPtr & segment)
{
    RUNTIME_CHECK(segment != nullptr);

    // TODO(local index): There could be some indexes are built while some indexes is not yet built after DDL
    if (!local_index_infos || local_index_infos->empty())
        return false;

    // No lock is needed, stable meta is immutable.
    auto dm_files = segment->getStable()->getDMFiles();
    auto build_info = DMFileIndexWriter::getLocalIndexBuildInfo(local_index_infos, dm_files);
    if (!build_info.indexes_to_build || build_info.indexes_to_build->empty())
        return false;

    auto store_weak_ptr = weak_from_this();
    auto tracing_id = fmt::format("segmentEnsureStableIndex<{}>", log->identifier());
    auto workload = [store_weak_ptr, build_info, dm_files, segment, tracing_id]() -> void {
        auto store = store_weak_ptr.lock();
        if (store == nullptr) // Store is destroyed before the task is executed.
            return;
        auto dm_context = store->newDMContext( //
            store->global_context,
            store->global_context.getSettingsRef(),
            tracing_id);
        const auto source_segment_info = segment->simpleInfo();
        store->segmentEnsureStableIndex(*dm_context, build_info.indexes_to_build, dm_files, source_segment_info);
    };

    auto indexer_scheduler = global_context.getGlobalLocalIndexerScheduler();
    RUNTIME_CHECK(indexer_scheduler != nullptr);
    indexer_scheduler->pushTask(LocalIndexerScheduler::Task{
        .keyspace_id = keyspace_id,
        .table_id = physical_table_id,
        .file_ids = build_info.file_ids,
        .request_memory = build_info.estimated_memory_bytes,
        .workload = workload,
    });
    return true;
}

bool DeltaMergeStore::segmentWaitStableIndexReady(const SegmentPtr & segment) const
{
    RUNTIME_CHECK(segment != nullptr);

    // TODO(local index): There could be some indexes are built while some indexes is not yet built after DDL
    if (!local_index_infos || local_index_infos->empty())
        return true;

    // No lock is needed, stable meta is immutable.
    auto segment_id = segment->segmentId();
    auto dm_files = segment->getStable()->getDMFiles();
    auto build_info = DMFileIndexWriter::getLocalIndexBuildInfo(local_index_infos, dm_files);
    if (!build_info.indexes_to_build || build_info.indexes_to_build->empty())
        return true;

    static constexpr size_t MAX_CHECK_TIME_SECONDS = 60; // 60s
    Stopwatch watch;
    while (watch.elapsedSeconds() < MAX_CHECK_TIME_SECONDS)
    {
        DMFilePtr dmfile;
        {
            std::shared_lock lock(read_write_mutex);
            auto seg = id_to_segment.at(segment_id);
            assert(!seg->getStable()->getDMFiles().empty());
            dmfile = seg->getStable()->getDMFiles()[0];
        }
        if (!dmfile)
            return false; // DMFile is not exist, return false
        bool all_indexes_built = true;
        for (const auto & index : *build_info.indexes_to_build)
        {
            auto col_id = index.column_id;
            // The dmfile may be built before col_id is added. Skip build indexes for it
            if (!dmfile->isColumnExist(col_id))
                continue;

            all_indexes_built = all_indexes_built && dmfile->getColumnStat(col_id).index_bytes > 0;
        }
        if (all_indexes_built)
            return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(100)); // 0.1s
    }

    return false;
}

SegmentPtr DeltaMergeStore::segmentUpdateMeta(
    std::unique_lock<std::shared_mutex> & read_write_lock,
    DMContext & dm_context,
    const SegmentPtr & segment,
    const DMFiles & new_dm_files)
{
    if (!isSegmentValid(read_write_lock, segment))
    {
        LOG_WARNING(log, "SegmentUpdateMeta - Give up because segment not valid, segment={}", segment->simpleInfo());
        return {};
    }

    auto lock = segment->mustGetUpdateLock();
    auto new_segment = segment->replaceStableMetaVersion(lock, dm_context, new_dm_files);
    if (new_segment == nullptr)
    {
        LOG_WARNING(
            log,
            "SegmentUpdateMeta - Failed due to replace stableMeta failed, segment={}",
            segment->simpleInfo());
        return {};
    }

    replaceSegment(read_write_lock, segment, new_segment);

    // Must not abandon old segment, because they share the same delta.
    // segment->abandon(dm_context);

    if constexpr (DM_RUN_CHECK)
    {
        new_segment->check(dm_context, "After SegmentUpdateMeta");
    }

    LOG_INFO(
        log,
        "SegmentUpdateMeta - Finish, old_segment={} new_segment={}",
        segment->simpleInfo(),
        new_segment->simpleInfo());
    return new_segment;
}

void DeltaMergeStore::segmentEnsureStableIndex(
    DMContext & dm_context,
    const IndexInfosPtr & index_info,
    const DMFiles & dm_files,
    const String & source_segment_info)
{
    // 1. Acquire a snapshot for PageStorage, and keep the snapshot until index is built.
    // This helps keep DMFile valid during the index build process.
    // We don't acquire a snapshot from the source_segment, because the source_segment
    // may be abandoned at this moment.
    //
    // Note that we cannot simply skip the index building when seg is not valid any more,
    // because segL and segR is still referencing them, consider this case:
    //   1. seg=PhysicalSplit
    //   2. Add CreateStableIndex(seg) to ThreadPool
    //   3. segL, segR=LogicalSplit(seg)
    //   4. CreateStableIndex(seg)

    auto storage_snapshot = std::make_shared<StorageSnapshot>(
        *dm_context.storage_pool,
        dm_context.getReadLimiter(),
        dm_context.tracing_id,
        /*snapshot_read*/ true);

    RUNTIME_CHECK(dm_files.size() == 1); // size > 1 is currently not supported.
    const auto & dm_file = dm_files[0];

    // 2. Check whether the DMFile has been referenced by any valid segment.
    {
        std::shared_lock lock(read_write_mutex);
        auto segment_ids = dmfile_id_to_segment_ids.get(dm_file->fileId());
        if (segment_ids.empty())
        {
            LOG_DEBUG(
                log,
                "EnsureStableIndex - Give up because no segment to update, source_segment={}",
                source_segment_info);
            return;
        }
    }

    LOG_INFO(
        log,
        "EnsureStableIndex - Begin building index, dm_files={} source_segment={}",
        DMFile::info(dm_files),
        source_segment_info);

    // 2. Build the index.
    DMFileIndexWriter iw(DMFileIndexWriter::Options{
        .path_pool = path_pool,
        .index_infos = index_info,
        .dm_files = dm_files,
        .dm_context = dm_context,
    });
    auto new_dmfiles = iw.build();
    RUNTIME_CHECK(!new_dmfiles.empty());

    LOG_INFO(
        log,
        "EnsureStableIndex - Finish building index, dm_files={} source_segment={}",
        DMFile::info(dm_files),
        source_segment_info);

    // 3. Update the meta version of the segments to the latest one.
    // To avoid logical split between step 2 and 3, get lastest segments to update again.
    // If TiFlash crashes during updating the meta version, some segments' meta are updated and some are not.
    // So after TiFlash restarts, we will update meta versions to latest versions again.
    {
        // We must acquire a single lock when updating multiple segments.
        // Otherwise we may miss new segments.
        std::unique_lock lock(read_write_mutex);
        auto segment_ids = dmfile_id_to_segment_ids.get(dm_file->fileId());
        for (const auto & seg_id : segment_ids)
        {
            auto segment = id_to_segment[seg_id];
            auto new_segment = segmentUpdateMeta(lock, dm_context, segment, new_dmfiles);
            // Expect update meta always success, because the segment must be valid and bump meta should succeed.
            RUNTIME_CHECK_MSG(new_segment != nullptr, "Update meta failed for segment {}", segment->simpleInfo());
        }
    }
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
        std::unique_lock lock(read_write_mutex);

        if (!isSegmentValid(lock, segment))
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
        replaceSegment(lock, segment, new_segment);

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

    segmentEnsureStableIndexAsync(new_segment);
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
            replaceSegment(lock, segment, new_segment);

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

    segmentEnsureStableIndexAsync(new_segment);
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
        replaceSegment(lock, segment, new_segment);

        LOG_INFO(log, "ReplaceData - Finish, old_segment={} new_segment={}", segment->info(), new_segment->info());
    }

    wbs.writeRemoves();

    if constexpr (DM_RUN_CHECK)
        check(dm_context.global_context);

    segmentEnsureStableIndexAsync(new_segment);
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

} // namespace DB::DM
