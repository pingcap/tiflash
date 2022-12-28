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

#include <Poco/Path.h>
#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/ExternalDTFileInfo.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/Page/V3/Remote/CheckpointManifestFileReader.h>
#include <Storages/Page/universal/UniversalPageStorage.h>

namespace ProfileEvents
{
extern const Event DMWriteFile;
extern const Event DMWriteFileNS;

} // namespace ProfileEvents

namespace DB
{

namespace FailPoints
{
extern const char pause_when_ingesting_to_dt_store[];
extern const char force_set_segment_ingest_packs_fail[];
extern const char segment_merge_after_ingest_packs[];
} // namespace FailPoints

namespace DM
{

std::tuple<String, PageId> DeltaMergeStore::preAllocateIngestFile()
{
    if (shutdown_called.load(std::memory_order_relaxed))
        return {};

    auto delegator = path_pool->getStableDiskDelegator();
    auto parent_path = delegator.choosePath();
    auto new_id = storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
    return {parent_path, new_id};
}

void DeltaMergeStore::preIngestFile(const String & parent_path, const PageId file_id, size_t file_size)
{
    if (shutdown_called.load(std::memory_order_relaxed))
        return;

    auto delegator = path_pool->getStableDiskDelegator();
    delegator.addDTFile(file_id, file_size, parent_path);
}

Segments DeltaMergeStore::ingestDTFilesUsingColumnFile(
    const DMContextPtr & dm_context,
    const RowKeyRange & range,
    const std::vector<DMFilePtr> & files,
    bool clear_data_in_range)
{
    auto delegate = dm_context->path_pool.getStableDiskDelegator();
    auto file_provider = dm_context->db_context.getFileProvider();

    Segments updated_segments;
    RowKeyRange cur_range = range;

    while (!cur_range.none())
    {
        RowKeyRange segment_range;

        // Keep trying until succeeded.
        while (true)
        {
            SegmentPtr segment;
            {
                std::shared_lock lock(read_write_mutex);

                auto segment_it = segments.upper_bound(cur_range.getStart());
                RUNTIME_CHECK(segment_it != segments.end(), cur_range.toDebugString());
                segment = segment_it->second;
            }

            FAIL_POINT_PAUSE(FailPoints::pause_when_ingesting_to_dt_store);
            waitForWrite(dm_context, segment);
            if (segment->hasAbandoned())
                continue;

            segment_range = segment->getRowKeyRange();

            // Write could fail, because other threads could already updated the instance. Like split/merge, merge delta.
            ColumnFiles column_files;
            WriteBatches wbs(*storage_pool, dm_context->getWriteLimiter());

            for (const auto & file : files)
            {
                /// Generate DMFile instance with a new ref_id pointed to the file_id.
                auto file_id = file->fileId();
                const auto & file_parent_path = file->parentPath();
                auto page_id = storage_pool->newDataPageIdForDTFile(delegate, __PRETTY_FUNCTION__);

                auto ref_file = DMFile::restore(file_provider, file_id, page_id, file_parent_path, DMFile::ReadMetaMode::all());
                auto column_file = std::make_shared<ColumnFileBig>(*dm_context, ref_file, segment_range);
                if (column_file->getRows() != 0)
                {
                    column_files.emplace_back(std::move(column_file));
                    wbs.data.putRefPage(page_id, file->pageId());
                }
            }

            // We have to commit those file_ids to PageStorage, because as soon as packs are written into segments,
            // they are visible for readers who require file_ids to be found in PageStorage.
            wbs.writeLogAndData();

            bool ingest_success = segment->ingestColumnFiles(*dm_context, range.shrink(segment_range), column_files, clear_data_in_range);
            fiu_do_on(FailPoints::force_set_segment_ingest_packs_fail, { ingest_success = false; });
            if (ingest_success)
            {
                updated_segments.push_back(segment);
                fiu_do_on(FailPoints::segment_merge_after_ingest_packs, {
                    segment->flushCache(*dm_context);
                    segmentMergeDelta(*dm_context, segment, MergeDeltaReason::ForegroundWrite);
                    storage_pool->gc(global_context.getSettingsRef(), StoragePool::Seconds(0));
                });
                break;
            }
            else
            {
                wbs.rollbackWrittenLogAndData();
            }
        }

        cur_range.setStart(segment_range.end);
        cur_range.setEnd(range.end);
    }

    return updated_segments;
}

/**
 * Accept a target ingest range and a vector of DTFiles, ingest these DTFiles (clipped by the target ingest range)
 * using logical split. All data in the target ingest range will be cleared, and replaced by the specified DTFiles.
 *
 * `clear_data_in_range` must be true. Otherwise exceptions will be thrown.
 *
 * You must ensure DTFiles do not overlap. Otherwise you will lose data.
 * // TODO: How to check this? Maybe we can enforce external_files to be ordered.
 *
 * WARNING: This function does not guarantee isolation. You may observe partial results when
 * querying related segments when this function is running.
 */
Segments DeltaMergeStore::ingestDTFilesUsingSplit(
    const DMContextPtr & dm_context,
    const RowKeyRange & ingest_range,
    const std::vector<ExternalDTFileInfo> & external_files,
    const DMFiles & files,
    bool clear_data_in_range)
{
    RUNTIME_CHECK(clear_data_in_range == true);
    RUNTIME_CHECK(
        files.size() == external_files.size(),
        files.size(),
        external_files.size());
    for (size_t i = 0; i < files.size(); ++i)
    {
        RUNTIME_CHECK(files[i]->pageId() == external_files[i].id);
    }

    std::set<SegmentPtr> updated_segments;

    // First phase (DeleteRange Phase):
    // Write DeleteRange to the covered segments to ensure that all data in the `ingest_range` is cleared.
    {
        RowKeyRange remaining_delete_range = ingest_range;
        LOG_INFO(
            log,
            "Table ingest using split - delete range phase - begin, remaining_delete_range={}",
            remaining_delete_range.toDebugString());

        while (!remaining_delete_range.none())
        {
            SegmentPtr segment;
            {
                std::shared_lock lock(read_write_mutex);

                auto segment_it = segments.upper_bound(remaining_delete_range.getStart());
                RUNTIME_CHECK(segment_it != segments.end(), remaining_delete_range.toDebugString());
                segment = segment_it->second;
            }

            const auto delete_range = remaining_delete_range.shrink(segment->getRowKeyRange());
            RUNTIME_CHECK(
                !delete_range.none(), // as remaining_delete_range is not none, we expect the shrinked range to be not none.
                delete_range.toDebugString(),
                segment->simpleInfo(),
                remaining_delete_range.toDebugString());
            LOG_DEBUG(
                log,
                "Table ingest using split - delete range phase - Try to delete range in segment, delete_range={} segment={} remaining_delete_range={} updated_segments_n={}",
                delete_range.toDebugString(),
                segment->simpleInfo(),
                remaining_delete_range.toDebugString(),
                updated_segments.size());

            const bool succeeded = segment->write(*dm_context, delete_range);
            if (succeeded)
            {
                updated_segments.insert(segment);
                RUNTIME_CHECK(compare(delete_range.getEnd(), remaining_delete_range.getStart()) >= 0);
                remaining_delete_range.setStart(delete_range.end); // We always move forward
            }
            else
            {
                // segment may be abandoned, retry current range by finding the segment again.
            }
        }
    }

    LOG_DEBUG(
        log,
        "Table ingest using split - delete range phase - finished, updated_segments_n={}",
        updated_segments.size());

    /*
     * In second phase (SplitIngest Phase),
     * we will try to ingest DMFile one by one into the segments in order.
     *
     * Consider the following case:
     * -Inf                                                                         +Inf
     *  │        │--------------- Ingest Range ---------------│                       │
     *  │               │-- DMFile --│- DMFile --│---- DMFile ----│- DMFile --│       │
     *  │- Segment --│-- Seg --│------- Segment -----│- Seg -│------- Segment --------│
     *
     * This is what we will ingest:
     * Iterate 0:
     * -Inf                                                                         +Inf
     *  │        │--------------- Ingest Range ---------------│                       │
     *  │               │-- DMFile --│                                                │
     *  │            │-- Seg --│------- Segment -----│                                │
     *                   ↑              ↑ The segment we ingest DMFile into
     *
     * Iterate 1:
     * -Inf                                                                         +Inf
     *  │        │--------------- Ingest Range ---------------│                       │
     *  │               │************│- DMFile --│                                    │
     *  │                      │------- Segment -----│                                │
     *                                  ↑ The segment we ingest DMFile into
     */

    LOG_DEBUG(
        log,
        "Table ingest using split - split ingest phase - begin, ingest_range={}, files_n={}",
        ingest_range.toDebugString(),
        files.size());

    for (size_t file_idx = 0; file_idx < files.size(); file_idx++)
    {
        // This should not happen. Just check to be confident.
        // Even if it happened, we could handle it gracefully here. (but it really means somewhere else is broken)
        if (files[file_idx]->getRows() == 0)
        {
            LOG_WARNING(
                log,
                "Table ingest using split - split ingest phase - Unexpected empty DMFile, skipped. ingest_range={} file_idx={} file={}",
                ingest_range.toDebugString(),
                file_idx,
                files[file_idx]->path());
            continue;
        }

        /**
         * Each DMFile (bounded by the ingest range) may overlap with multiple segments, like:
         * -Inf                                                                         +Inf
         *  │        │--------------- Ingest Range ---------------│                       │
         *  │               │-- DMFile --│                                                │
         *  │            │-- Seg --│------- Segment -----│                                │
         * We will try to ingest it into all overlapped segments.
         */
        auto file_ingest_range = external_files[file_idx].range.shrink(ingest_range);
        while (!file_ingest_range.none()) // This DMFile has remaining data to ingest
        {
            SegmentPtr segment;
            {
                std::shared_lock lock(read_write_mutex);
                auto segment_it = segments.upper_bound(file_ingest_range.getStart());
                RUNTIME_CHECK(segment_it != segments.end(),
                              file_ingest_range.toDebugString(),
                              file_idx,
                              files[file_idx]->path());
                segment = segment_it->second;
            }

            if (segment->hasAbandoned())
                continue; // retry with current range and file

            /**
             * -Inf                                                                         +Inf
             *  │        │--------------- Ingest Range ---------------│                       │
             *  │               │-- DMFile --│                                                │
             *  │            │-- Seg --│------- Segment -----│                                │
             *                  ^^^^^^^^ segment_ingest_range
             */
            const auto segment_ingest_range = file_ingest_range.shrink(segment->getRowKeyRange());
            RUNTIME_CHECK(
                !segment_ingest_range.none(),
                segment_ingest_range.toDebugString(),
                file_idx,
                files[file_idx]->path(),
                segment->simpleInfo());

            LOG_INFO(
                log,
                "Table ingest using split - split ingest phase - Try to ingest file into segment, file_idx={} file_id=dmf_{} file_ingest_range={} segment={} segment_ingest_range={}",
                file_idx,
                files[file_idx]->fileId(),
                file_ingest_range.toDebugString(),
                segment->simpleInfo(),
                segment_ingest_range.toDebugString());

            const bool succeeded = ingestDTFileIntoSegmentUsingSplit(*dm_context, segment, segment_ingest_range, files[file_idx]);
            if (succeeded)
            {
                updated_segments.insert(segment);
                // We have ingested (DTFileRange ∪ ThisSegmentRange), let's try with next overlapped segment.
                RUNTIME_CHECK(compare(segment_ingest_range.getEnd(), file_ingest_range.getStart()) > 0);
                file_ingest_range.setStart(segment_ingest_range.end);
            }
            else
            {
                // this segment is abandoned, or may be split into multiples.
                // retry with current range and file and find segment again.
            }
        }
    }

    LOG_DEBUG(
        log,
        "Table ingest using split - split ingest phase - finished, updated_segments_n={}",
        updated_segments.size());

    return std::vector<SegmentPtr>(
        updated_segments.begin(),
        updated_segments.end());
}

/**
 * Ingest one DTFile into the target segment by using logical split.
 */
bool DeltaMergeStore::ingestDTFileIntoSegmentUsingSplit(
    DMContext & dm_context,
    const SegmentPtr & segment,
    const RowKeyRange & ingest_range,
    const DMFilePtr & file)
{
    const auto & segment_range = segment->getRowKeyRange();

    // The ingest_range must fall in segment's range.
    RUNTIME_CHECK(
        !ingest_range.none(),
        ingest_range.toDebugString());
    RUNTIME_CHECK(
        compare(segment_range.getStart(), ingest_range.getStart()) <= 0,
        segment_range.toDebugString(),
        ingest_range.toDebugString());
    RUNTIME_CHECK(
        compare(segment_range.getEnd(), ingest_range.getEnd()) >= 0,
        segment_range.toDebugString(),
        ingest_range.toDebugString());

    const bool is_start_matching = (compare(segment_range.getStart(), ingest_range.getStart()) == 0);
    const bool is_end_matching = (compare(segment_range.getEnd(), ingest_range.getEnd()) == 0);

    if (is_start_matching && is_end_matching)
    {
        /*
         * The segment and the ingest range is perfectly matched. We can
         * simply replace all of the data from this segment.
         *
         * Example:
         *    │----------- Segment ----------│
         *    │-------- Ingest Range --------│
         */
        const auto new_segment_or_null = segmentDangerouslyReplaceData(dm_context, segment, file);
        const bool succeeded = new_segment_or_null != nullptr;
        return succeeded;
    }
    else if (is_start_matching)
    {
        /*
         * Example:
         *    │--------------- Segment ---------------│
         *    │-------- Ingest Range --------│
         *
         * We will logical split the segment to form a perfect matching segment:
         *    │--------------- Segment ------│--------│
         *    │-------- Ingest Range --------│
         */
        const auto [left, right] = segmentSplit(dm_context, segment, SegmentSplitReason::IngestBySplit, ingest_range.end, SegmentSplitMode::Logical);
        if (left == nullptr || right == nullptr)
        {
            // Split failed, likely caused by snapshot failed.
            // Sleep awhile and retry.
            std::this_thread::sleep_for(std::chrono::milliseconds(15));
        }
        // Always returning false, because we need to retry to get a new segment (as the old segment is abandoned)
        // even when split succeeded.
        return false;
    }
    else if (is_end_matching)
    {
        /*
         * Example:
         *    │--------------- Segment ---------------│
         *             │-------- Ingest Range --------│
         *
         * We will logical split the segment to form a perfect matching segment:
         *    │--------│------ Segment ---------------│
         *             │-------- Ingest Range --------│
         */
        const auto [left, right] = segmentSplit(dm_context, segment, SegmentSplitReason::IngestBySplit, ingest_range.start, SegmentSplitMode::Logical);
        if (left == nullptr || right == nullptr)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(15));
        }
        return false;
    }
    else
    {
        /*
         * Example:
         *    │--------------- Segment ---------------│
         *        │-------- Ingest Range --------│
         *
         * We invoke a logical split first:
         *    │---│----------- Segment ---------------│
         *        │-------- Ingest Range --------│
         */
        const auto [left, right] = segmentSplit(dm_context, segment, SegmentSplitReason::IngestBySplit, ingest_range.start, SegmentSplitMode::Logical);
        if (left == nullptr || right == nullptr)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(15));
        }
        return false;
    }
}

void DeltaMergeStore::ingestFiles(
    const DMContextPtr & dm_context,
    const RowKeyRange & range,
    const std::vector<DM::ExternalDTFileInfo> & external_files,
    bool clear_data_in_range)
{
    if (unlikely(shutdown_called.load(std::memory_order_relaxed)))
    {
        const auto msg = fmt::format("Try to ingest files into a shutdown table, store={}", log->identifier());
        LOG_WARNING(log, "{}", msg);
        throw Exception(msg);
    }

    EventRecorder write_block_recorder(ProfileEvents::DMWriteFile, ProfileEvents::DMWriteFileNS);

    auto delegate = dm_context->path_pool.getStableDiskDelegator();
    auto file_provider = dm_context->db_context.getFileProvider();

    size_t rows = 0;
    size_t bytes = 0;
    size_t bytes_on_disk = 0;

    DMFiles files;
    for (const auto & external_file : external_files)
    {
        auto file_parent_path = delegate.getDTFilePath(external_file.id);

        // we always create a ref file to this DMFile with all meta info restored later, so here we just restore meta info to calculate its' memory and disk size
        auto file = DMFile::restore(file_provider, external_file.id, external_file.id, file_parent_path, DMFile::ReadMetaMode::memoryAndDiskSize());
        rows += file->getRows();
        bytes += file->getBytes();
        bytes_on_disk += file->getBytesOnDisk();

        // Do some simple verification for the file range.
        if (file->getRows() > 0)
            RUNTIME_CHECK(!external_file.range.none());

        files.emplace_back(std::move(file));
    }

    bool ingest_using_split = false;
    if (clear_data_in_range && bytes >= dm_context->delta_small_column_file_bytes)
    {
        // We still write small ssts directly into the delta layer.
        ingest_using_split = true;
    }

    {
        auto get_ingest_files = [&] {
            FmtBuffer fmt_buf;
            fmt_buf.append("[");
            fmt_buf.joinStr(
                external_files.begin(),
                external_files.end(),
                [](const ExternalDTFileInfo & external_file, FmtBuffer & fb) { fb.append(external_file.toString()); },
                ",");
            fmt_buf.append("]");
            return fmt_buf.toString();
        };
        LOG_INFO(
            log,
            "Table ingest files - begin, ingest_by_split={} files={} rows={} bytes={} bytes_on_disk={} range={} clear={}",
            ingest_using_split,
            get_ingest_files(),
            rows,
            bytes,
            bytes_on_disk,
            range.toDebugString(),
            clear_data_in_range);
    }

    // Put the ingest file ids into `storage_pool` and use ref id in each segments to ensure the atomic
    // of ingesting.
    // Check https://github.com/pingcap/tics/issues/2040 for more details.
    // TODO: If tiflash crash during the middle of ingesting, we may leave some DTFiles on disk and
    // they can not be deleted. We should find a way to cleanup those files.
    WriteBatches ingest_wbs(*storage_pool, dm_context->getWriteLimiter());
    if (!files.empty())
    {
        for (const auto & file : files)
        {
            ingest_wbs.data.putExternal(file->fileId(), 0);
        }
        ingest_wbs.writeLogAndData();
        ingest_wbs.setRollback(); // rollback if exception thrown
    }

    Segments updated_segments;
    if (!range.none())
    {
        if (ingest_using_split)
            updated_segments = ingestDTFilesUsingSplit(dm_context, range, external_files, files, clear_data_in_range);
        else
            updated_segments = ingestDTFilesUsingColumnFile(dm_context, range, files, clear_data_in_range);
    }

    // Enable gc for DTFile after all segment applied.
    // Note that we can not enable gc for them once they have applied to any segments.
    // Assume that one segment get compacted after file ingested, `gc_handle` gc the
    // DTFiles before they get applied to all segments. Then we will apply some
    // deleted DTFiles to other segments.
    for (const auto & file : files)
        file->enableGC();
    // After the ingest DTFiles applied, remove the original page
    ingest_wbs.rollbackWrittenLogAndData();

    {
        // Add some logging about the ingested file ids and updated segments
        // Example: "ingested_files=[<file=dmf_1001 range=..>,<file=dmf_1002 range=..>] updated_segments=[<segment_id=1 ...>,<segment_id=3 ...>]"
        //          "ingested_files=[] updated_segments=[<segment_id=1 ...>,<segment_id=3 ...>]"
        auto get_ingest_info = [&] {
            FmtBuffer fmt_buf;
            fmt_buf.append("ingested_files=[");
            fmt_buf.joinStr(
                external_files.begin(),
                external_files.end(),
                [](const ExternalDTFileInfo & external_file, FmtBuffer & fb) { fb.append(external_file.toString()); },
                ",");
            fmt_buf.append("] updated_segments=[");
            fmt_buf.joinStr(
                updated_segments.begin(),
                updated_segments.end(),
                [](const auto & segment, FmtBuffer & fb) { fb.append(segment->simpleInfo()); },
                ",");
            fmt_buf.append("]");
            return fmt_buf.toString();
        };

        LOG_INFO(
            log,
            "Table ingest files - finished ingested files into segments, {} clear={}",
            get_ingest_info(),
            clear_data_in_range);
    }

    GET_METRIC(tiflash_storage_throughput_bytes, type_ingest).Increment(bytes);
    GET_METRIC(tiflash_storage_throughput_rows, type_ingest).Increment(rows);

    if (!range.none())
        flushCache(dm_context, range);

    // TODO: Update the tracing_id before checkSegmentUpdate?
    for (auto & segment : updated_segments)
        checkSegmentUpdate(dm_context, segment, ThreadType::Write);
}

std::vector<SegmentPtr> DeltaMergeStore::ingestSegmentsUsingSplit(
    const DMContextPtr & dm_context,
    const RowKeyRange & ingest_range,
    const std::vector<SegmentPtr> & target_segments)
{
    std::set<SegmentPtr> updated_segments;

    // First phase (DeleteRange Phase):
    // Write DeleteRange to the covered segments to ensure that all data in the `ingest_range` is cleared.
    {
        RowKeyRange remaining_delete_range = ingest_range;
        LOG_INFO(
            log,
            "Table ingest using split - delete range phase - begin, remaining_delete_range={}",
            remaining_delete_range.toDebugString());

        while (!remaining_delete_range.none())
        {
            SegmentPtr segment;
            {
                std::shared_lock lock(read_write_mutex);

                auto segment_it = segments.upper_bound(remaining_delete_range.getStart());
                RUNTIME_CHECK(segment_it != segments.end(), remaining_delete_range.toDebugString());
                segment = segment_it->second;
            }

            const auto delete_range = remaining_delete_range.shrink(segment->getRowKeyRange());
            RUNTIME_CHECK(
                !delete_range.none(), // as remaining_delete_range is not none, we expect the shrinked range to be not none.
                delete_range.toDebugString(),
                segment->simpleInfo(),
                remaining_delete_range.toDebugString());
            LOG_DEBUG(
                log,
                "Table ingest using split - delete range phase - Try to delete range in segment, delete_range={} segment={} remaining_delete_range={} updated_segments_n={}",
                delete_range.toDebugString(),
                segment->simpleInfo(),
                remaining_delete_range.toDebugString(),
                updated_segments.size());

            const bool succeeded = segment->write(*dm_context, delete_range);
            if (succeeded)
            {
                updated_segments.insert(segment);
                RUNTIME_CHECK(compare(delete_range.getEnd(), remaining_delete_range.getStart()) >= 0);
                remaining_delete_range.setStart(delete_range.end); // We always move forward
            }
            else
            {
                // segment may be abandoned, retry current range by finding the segment again.
            }
        }
    }

    LOG_DEBUG(
        log,
        "Table ingest using split - delete range phase - finished, updated_segments_n={}",
        updated_segments.size());

    /*
     * In second phase (SplitIngest Phase),
     * we will try to ingest DMFile one by one into the segments in order.
     *
     * Consider the following case:
     * -Inf                                                                         +Inf
     *  │        │--------------- Ingest Range ---------------│                       │
     *  │               │-- DMFile --│- DMFile --│---- DMFile ----│- DMFile --│       │
     *  │- Segment --│-- Seg --│------- Segment -----│- Seg -│------- Segment --------│
     *
     * This is what we will ingest:
     * Iterate 0:
     * -Inf                                                                         +Inf
     *  │        │--------------- Ingest Range ---------------│                       │
     *  │               │-- DMFile --│                                                │
     *  │            │-- Seg --│------- Segment -----│                                │
     *                   ↑              ↑ The segment we ingest DMFile into
     *
     * Iterate 1:
     * -Inf                                                                         +Inf
     *  │        │--------------- Ingest Range ---------------│                       │
     *  │               │************│- DMFile --│                                    │
     *  │                      │------- Segment -----│                                │
     *                                  ↑ The segment we ingest DMFile into
     */

    LOG_DEBUG(
        log,
        "Table ingest using split - split ingest phase - begin, ingest_range={}, files_n={}",
        ingest_range.toDebugString(),
        target_segments.size());

    for (size_t file_idx = 0; file_idx < target_segments.size(); file_idx++)
    {
        // This should not happen. Just check to be confident.
        // Even if it happened, we could handle it gracefully here. (but it really means somewhere else is broken)
        if (target_segments[file_idx]->getEstimatedRows() == 0)
        {
            LOG_WARNING(
                log,
                "Table ingest using split - split ingest phase - Unexpected empty DMFile, skipped. ingest_range={} file_idx={}",
                ingest_range.toDebugString(),
                file_idx);
            continue;
        }

        /**
         * Each DMFile (bounded by the ingest range) may overlap with multiple segments, like:
         * -Inf                                                                         +Inf
         *  │        │--------------- Ingest Range ---------------│                       │
         *  │               │-- DMFile --│                                                │
         *  │            │-- Seg --│------- Segment -----│                                │
         * We will try to ingest it into all overlapped segments.
         */
        auto file_ingest_range = target_segments[file_idx]->getRowKeyRange();
        while (!file_ingest_range.none()) // This DMFile has remaining data to ingest
        {
            SegmentPtr segment;
            {
                std::shared_lock lock(read_write_mutex);
                auto segment_it = segments.upper_bound(file_ingest_range.getStart());
                RUNTIME_CHECK(segment_it != segments.end());
                segment = segment_it->second;
            }

            if (segment->hasAbandoned())
                continue; // retry with current range and file

            /**
             * -Inf                                                                         +Inf
             *  │        │--------------- Ingest Range ---------------│                       │
             *  │               │-- DMFile --│                                                │
             *  │            │-- Seg --│------- Segment -----│                                │
             *                  ^^^^^^^^ segment_ingest_range
             */
            const auto segment_ingest_range = file_ingest_range.shrink(segment->getRowKeyRange());
            RUNTIME_CHECK(!segment_ingest_range.none());

            LOG_INFO(
                log,
                "Table ingest using split - split ingest phase - Try to ingest file into segment, file_idx={} file_id=dmf_{} file_ingest_range={} segment={} segment_ingest_range={}",
                file_idx,
                target_segments[file_idx]->segmentId(),
                file_ingest_range.toDebugString(),
                segment->simpleInfo(),
                segment_ingest_range.toDebugString());

            const bool succeeded = ingestSegmentIntoSegmentUsingSplit(*dm_context, segment, target_segments[file_idx]);
            if (succeeded)
            {
                updated_segments.insert(segment);
                // We have ingested (DTFileRange ∪ ThisSegmentRange), let's try with next overlapped segment.
                RUNTIME_CHECK(compare(segment_ingest_range.getEnd(), file_ingest_range.getStart()) > 0);
                file_ingest_range.setStart(segment_ingest_range.end);
            }
            else
            {
                // this segment is abandoned, or may be split into multiples.
                // retry with current range and file and find segment again.
            }
        }
    }

    LOG_DEBUG(
        log,
        "Table ingest using split - split ingest phase - finished, updated_segments_n={}",
        updated_segments.size());

    return std::vector<SegmentPtr>(
        updated_segments.begin(),
        updated_segments.end());
}

bool DeltaMergeStore::ingestSegmentIntoSegmentUsingSplit(
    DMContext & dm_context,
    const SegmentPtr & segment,
    const SegmentPtr & target_segment)
{
    const auto & segment_range = segment->getRowKeyRange();
    const auto & ingest_range = target_segment->getRowKeyRange();

    // The ingest_range must fall in segment's range.
    RUNTIME_CHECK(
        !ingest_range.none(),
        ingest_range.toDebugString());
    RUNTIME_CHECK(
        compare(segment_range.getStart(), ingest_range.getStart()) <= 0,
        segment_range.toDebugString(),
        ingest_range.toDebugString());
    RUNTIME_CHECK(
        compare(segment_range.getEnd(), ingest_range.getEnd()) >= 0,
        segment_range.toDebugString(),
        ingest_range.toDebugString());

    const bool is_start_matching = (compare(segment_range.getStart(), ingest_range.getStart()) == 0);
    const bool is_end_matching = (compare(segment_range.getEnd(), ingest_range.getEnd()) == 0);

    if (is_start_matching && is_end_matching)
    {
        /*
         * The segment and the ingest range is perfectly matched. We can
         * simply replace all of the data from this segment.
         *
         * Example:
         *    │----------- Segment ----------│
         *    │-------- Ingest Range --------│
         */
        WriteBatches wbs{dm_context.storage_pool};
        auto [in_memory_files, column_file_persisteds] = target_segment->getDelta()->cloneAllColumnFiles(
            target_segment->mustGetUpdateLock(),
            dm_context,
            segment_range,
            wbs);
        RUNTIME_CHECK(in_memory_files.empty());
        wbs.writeLogAndData();
        auto dm_files = target_segment->getStable()->getDMFiles();
        RUNTIME_CHECK(dm_files.size() == 1);
        const auto new_segment_or_null = segmentDangerouslyReplaceData(dm_context, segment, dm_files[0], column_file_persisteds);
        const bool succeeded = new_segment_or_null != nullptr;
        if (!succeeded)
        {
            wbs.rollbackWrittenLogAndData();
        }
        return succeeded;
    }
    else if (is_start_matching)
    {
        /*
         * Example:
         *    │--------------- Segment ---------------│
         *    │-------- Ingest Range --------│
         *
         * We will logical split the segment to form a perfect matching segment:
         *    │--------------- Segment ------│--------│
         *    │-------- Ingest Range --------│
         */
        const auto [left, right] = segmentSplit(dm_context, segment, SegmentSplitReason::IngestBySplit, ingest_range.end, SegmentSplitMode::Logical);
        if (left == nullptr || right == nullptr)
        {
            // Split failed, likely caused by snapshot failed.
            // Sleep awhile and retry.
            std::this_thread::sleep_for(std::chrono::milliseconds(15));
        }
        // Always returning false, because we need to retry to get a new segment (as the old segment is abandoned)
        // even when split succeeded.
        return false;
    }
    else if (is_end_matching)
    {
        /*
         * Example:
         *    │--------------- Segment ---------------│
         *             │-------- Ingest Range --------│
         *
         * We will logical split the segment to form a perfect matching segment:
         *    │--------│------ Segment ---------------│
         *             │-------- Ingest Range --------│
         */
        const auto [left, right] = segmentSplit(dm_context, segment, SegmentSplitReason::IngestBySplit, ingest_range.start, SegmentSplitMode::Logical);
        if (left == nullptr || right == nullptr)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(15));
        }
        return false;
    }
    else
    {
        /*
         * Example:
         *    │--------------- Segment ---------------│
         *        │-------- Ingest Range --------│
         *
         * We invoke a logical split first:
         *    │---│----------- Segment ---------------│
         *        │-------- Ingest Range --------│
         */
        const auto [left, right] = segmentSplit(dm_context, segment, SegmentSplitReason::IngestBySplit, ingest_range.start, SegmentSplitMode::Logical);
        if (left == nullptr || right == nullptr)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(15));
        }
        return false;
    }
}

void DeltaMergeStore::ingestSegmentFromCheckpointPath(const DMContextPtr & dm_context, //
                                     const DM::RowKeyRange & range,
                                     const String & checkpoint_path)
{
    if (unlikely(shutdown_called.load(std::memory_order_relaxed)))
    {
        const auto msg = fmt::format("Try to ingest files into a shutdown table, store={}", log->identifier());
        LOG_WARNING(log, "{}", msg);
        throw Exception(msg);
    }
    auto reader = PS::V3::CheckpointManifestFileReader<PageDirectoryTrait>::create(//
        PS::V3::CheckpointManifestFileReader<PageDirectoryTrait>::Options{
            .file_path = checkpoint_path
        });
    auto checkpoint_dir = Poco::Path(checkpoint_path).parent().toString();
    PS::V3::CheckpointPageManager manager(*reader, checkpoint_dir);
    auto segment_meta_infos = Segment::restoreAllSegmentsMetaInfo(physical_table_id, range, manager);
    WriteBatches wbs{dm_context->storage_pool};
    auto restored_segments = Segment::restoreSegmentsFromCheckpoint( //
        log,
        *dm_context,
        physical_table_id,
        segment_meta_infos,
        range,
        manager,
        wbs);
    wbs.writeAll();

    RUNTIME_CHECK_MSG(!restored_segments.empty(), "Failed to restore any segment");

    auto updated_segments = ingestSegmentsUsingSplit(dm_context, range, restored_segments);

    for (auto & segment : updated_segments)
        checkSegmentUpdate(dm_context, segment, ThreadType::Write);

//    std::vector<SegmentPtr> DeltaMergeStore::ingestSegmentsUsingSplit(
//        const DMContextPtr & dm_context,
//        const RowKeyRange & ingest_range,
//        const std::vector<SegmentPtr> & target_segments)

//    // begin ingest segments
//    RowKeyRange cur_range = range;
//    while (true)
//    {
//        SegmentPtr segment_to_split;
//        {
//            std::shared_lock lock(read_write_mutex);
//            /*
//             *  we want a perfect match which ingest range start and segment range start to avoid hard case like the following:
//             *     │---------- Range ------------│
//             *  │-------- Segment Range ------------│
//             */
//            auto first_segment_it = segments.upper_bound(cur_range.getStart());
//            auto first_segment = first_segment_it->second;
//            auto first_segment_range = first_segment->getRowKeyRange();
//            if (compare(range.getStart(), first_segment_range.getStart()) == 0)
//            {
//                bool found_first_segment;
//                PageId last_segment_id;
//                while (!cur_range.none())
//                {
//                    auto segment_it = segments.upper_bound(cur_range.getStart());
//                    auto segment = segment_it->second;
//                    auto segment_range = segment->getRowKeyRange();
//                    segments.erase(segment_range.getEnd());
//                    id_to_segment.erase(segment->segmentId());
//                    RUNTIME_CHECK(compare(segment_range.getStart(), cur_range.getStart()) == 0);
//
//                    // FIXME: handle first segment is deleted.
//                    if (compare(segment_range.getEnd(), cur_range.getEnd()) <= 0)
//                    {
//                        /*
//                         *        │------------- Range ---------------│
//                         *        │-------- Segment Range --------│
//                         *
//                         *        │------------- Range -----------│
//                         *        │-------- Segment Range --------│
//                         */
//                        if (!found_first_segment)
//                        {
//                            auto prev_segment_it = segment_it;
//                            prev_segment_it--;
//                            auto prev_segment = prev_segment_it->second;
//                            auto new_prev_segment = std::make_shared<Segment>(//
//                                log,
//                                prev_segment->segmentEpoch() + 1,
//                                prev_segment->getRowKeyRange(),
//                                prev_segment->segmentId(),
//                                restored_segments[0]->segmentId(),
//                                prev_segment->getDelta(),
//                                prev_segment->getStable()
//                            );
//                            prev_segment->abandon(*dm_context);
//                            segments[prev_segment->getRowKeyRange().getEnd()] = prev_segment;
//                            id_to_segment.emplace(prev_segment->segmentId(), prev_segment);
//                            new_prev_segment->serialize(wbs.meta);
//                            found_first_segment = true;
//                        }
//                        auto next_segment_it = segment_it;
//                        next_segment_it++;
//                        last_segment_id = next_segment_it != segments.end() ? next_segment_it->second->segmentId() : 0;
//                    }
//                    else if (compare(cur_range.getEnd(), segment_range.getEnd()) <= 0)
//                    {
//                        /*
//                         *        │------------- Range -----------│
//                         *        │-------- Segment Range --------------│
//                         */
//                        if (!found_first_segment)
//                        {
//                            auto prev_segment_it = segment_it;
//                            prev_segment_it--;
//                            auto prev_segment = prev_segment_it->second;
//                            auto new_prev_segment = std::make_shared<Segment>(//
//                                log,
//                                prev_segment->segmentEpoch() + 1,
//                                prev_segment->getRowKeyRange(),
//                                prev_segment->segmentId(),
//                                restored_segments[0]->segmentId(),
//                                prev_segment->getDelta(),
//                                prev_segment->getStable()
//                            );
//                            prev_segment->abandon(*dm_context);
//                            segments[prev_segment->getRowKeyRange().getEnd()] = prev_segment;
//                            id_to_segment.emplace(prev_segment->segmentId(), prev_segment);
//                            new_prev_segment->serialize(wbs.meta);
//                            found_first_segment = true;
//                        }
//                        if (compare(cur_range.getEnd(), segment_range.getEnd()) < 0)
//                        {
//                            RowKeyRange new_segment_range = segment_range;
//                            new_segment_range.setStart(cur_range.end);
//                            auto new_segment = std::make_shared<Segment>(//
//                                log,
//                                segment->segmentEpoch() + 1,
//                                new_segment_range,
//                                segment->segmentId(),
//                                segment->nextSegmentId(),
//                                segment->getDelta(),
//                                segment->getStable()
//                            );
//                            segments[new_segment->getRowKeyRange().getEnd()] = new_segment;
//                            id_to_segment.emplace(new_segment->segmentId(), new_segment);
//                            new_segment->serialize(wbs.meta);
//                            last_segment_id = segment->segmentId();
//                        }
//                        else
//                        {
//                            auto next_segment_it = segment_it;
//                            next_segment_it++;
//                            last_segment_id = next_segment_it != segments.end() ? next_segment_it->second->segmentId() : 0;
//                        }
//                    }
//                    else
//                    {
//                        RUNTIME_CHECK_MSG(false, "Shouldn't reach here");
//                    }
//
//                    segment->abandon(*dm_context);
//                    cur_range.setStart(segment_range.end);
//                    cur_range.setEnd(range.end);
//                }
//
//                for (size_t i = 0; i < restored_segments.size(); i++)
//                {
//                    auto target_segment = restored_segments[i];
//                    PageId next_segment_id = (i == restored_segments.size() - 1) ? last_segment_id : restored_segments[i + 1]->segmentId();
//                    auto segment = std::make_shared<Segment>(//
//                        log,
//                        target_segment->segmentEpoch() + 1,
//                        target_segment->getRowKeyRange(),
//                        target_segment->segmentId(),
//                        next_segment_id,
//                        target_segment->getDelta(),
//                        target_segment->getStable()
//                    );
//                    segments[segment->getRowKeyRange().getEnd()] = segment;
//                    id_to_segment.emplace(segment->segmentId(), segment);
//                    segment->serialize(wbs.meta);
//                }
//
//                wbs.writeMeta();
//                return;
//            }
//            else
//            {
//                RUNTIME_CHECK(compare(range.getStart(), segment_range.getStart()) > 0);
//                RUNTIME_CHECK(cur_range == range);
//                segment_to_split = segment;
//            }
//        }
//        if (segment_to_split)
//        {
//            const auto [left, right] = segmentSplit(*dm_context, segment_to_split, SegmentSplitReason::IngestBySplit, range.start, SegmentSplitMode::Logical);
//            if (left == nullptr || right == nullptr)
//            {
//                std::this_thread::sleep_for(std::chrono::milliseconds(15));
//            }
//        }
//    }
}
} // namespace DM
} // namespace DB
