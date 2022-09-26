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

#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/ExternalDTFileInfo.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Segment.h>

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
        LOG_FMT_INFO(
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
            LOG_FMT_DEBUG(
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

    LOG_FMT_DEBUG(
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

    LOG_FMT_DEBUG(
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

            LOG_FMT_INFO(
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

    LOG_FMT_DEBUG(
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
        // We don't care whether it is succeeded or not, because the caller always need to retry
        // from the beginning in this case.
        UNUSED(left, right);
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
        UNUSED(left, right);
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
        UNUSED(left, right);
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
        LOG_FMT_INFO(
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

        LOG_FMT_INFO(
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

} // namespace DM
} // namespace DB
