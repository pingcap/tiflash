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

#include <Common/EventRecorder.h>
#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/ExternalDTFileInfo.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/DeltaMerge/Remote/ObjectId.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/WriteBatchesImpl.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/Disagg/CheckpointInfo.h>
#include <Storages/KVStore/MultiRaft/Disagg/FastAddPeerContext.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/PathPool.h>

#include <magic_enum.hpp>

namespace CurrentMetrics
{
extern const Metric DT_SnapshotOfSegmentIngest;
}

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
extern const char force_ingest_via_delta[];
extern const char force_ingest_via_replace[];
} // namespace FailPoints

namespace DM
{
std::tuple<String, PageIdU64> DeltaMergeStore::preAllocateIngestFile()
{
    if (shutdown_called.load(std::memory_order_relaxed))
        return {};

    auto delegator = path_pool->getStableDiskDelegator();
    auto parent_path = delegator.choosePath();
    auto new_id = storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
    return {parent_path, new_id};
}

void DeltaMergeStore::preIngestFile(const String & parent_path, const PageIdU64 file_id, size_t file_size)
{
    if (shutdown_called.load(std::memory_order_relaxed))
        return;

    auto delegator = path_pool->getStableDiskDelegator();
    if (auto remote_data_store = global_context.getSharedContextDisagg()->remote_data_store; !remote_data_store)
    {
        delegator.addDTFile(file_id, file_size, parent_path);
    }
    else
    {
        delegator.addRemoteDTFileWithGCDisabled(file_id, file_size);
    }
}

void DeltaMergeStore::removePreIngestFile(PageIdU64 file_id, bool throw_on_not_exist)
{
    if (shutdown_called.load(std::memory_order_relaxed))
        return;

    auto delegator = path_pool->getStableDiskDelegator();
    if (auto remote_data_store = global_context.getSharedContextDisagg()->remote_data_store; !remote_data_store)
    {
        delegator.removeDTFile(file_id, throw_on_not_exist);
    }
    else
    {
        delegator.removeRemoteDTFile(file_id, throw_on_not_exist);
    }
}


void DeltaMergeStore::cleanPreIngestFiles(
    const Context & db_context,
    const DB::Settings & db_settings,
    const std::vector<DM::ExternalDTFileInfo> & external_files)
{
    auto dm_context = newDMContext(db_context, db_settings);
    auto delegate = dm_context->path_pool->getStableDiskDelegator();
    auto file_provider = dm_context->global_context.getFileProvider();

    for (const auto & f : external_files)
    {
        if (auto remote_data_store = global_context.getSharedContextDisagg()->remote_data_store; !remote_data_store)
        {
            auto file_parent_path = delegate.getDTFilePath(f.id);
            auto file = DM::DMFile::restore(
                file_provider,
                f.id,
                f.id,
                file_parent_path,
                DM::DMFileMeta::ReadMode::memoryAndDiskSize(),
                0 /* a meta version that must exists */,
                keyspace_id);
            removePreIngestFile(f.id, false);
            file->remove(file_provider);
        }
        else
        {
            // For disagg mode
            // - if the job has been finished, it means the local files is likely all uploaded to S3
            // - if the job is intrrupted, it means the `SSTFilesToDTFilesOutputStream::cancel` is called
            //   and local files are also removed.
            // So we ignore the files on disk.
            removePreIngestFile(f.id, false);
        }
    }
}


Segments DeltaMergeStore::ingestDTFilesUsingColumnFile(
    const DMContextPtr & dm_context,
    const RowKeyRange & range,
    const std::vector<DMFilePtr> & files,
    bool clear_data_in_range)
{
    auto delegate = dm_context->path_pool->getStableDiskDelegator();
    auto file_provider = dm_context->global_context.getFileProvider();

    Segments updated_segments;
    RowKeyRange cur_range = range;

    while (!cur_range.none())
    {
        RowKeyRange segment_range;

        // Keep trying until succeeded.
        while (true)
        {
            auto [segment, is_empty]
                = getSegmentByStartKey(cur_range.getStart(), /*create_if_empty*/ true, /*throw_if_notfound*/ true);

            FAIL_POINT_PAUSE(FailPoints::pause_when_ingesting_to_dt_store);
            waitForWrite(dm_context, segment);
            if (segment->hasAbandoned())
                continue;

            segment_range = segment->getRowKeyRange();

            // Write could fail, because we do not lock the segment here.
            // Thus, other threads may update the instance at any time, like split, merge, merge delta,
            // causing the segment to be abandoned.
            WriteBatches wbs(*storage_pool, dm_context->getWriteLimiter());

            DMFiles data_files;
            data_files.reserve(files.size());

            for (const auto & file : files)
            {
                /// Generate DMFile instance with a new ref_id pointed to the file_id.
                auto file_id = file->fileId();
                const auto & file_parent_path = file->parentPath();
                auto page_id = storage_pool->newDataPageIdForDTFile(delegate, __PRETTY_FUNCTION__);

                auto ref_file = DMFile::restore(
                    file_provider,
                    file_id,
                    page_id,
                    file_parent_path,
                    DMFileMeta::ReadMode::all(),
                    file->metaVersion(),
                    keyspace_id);
                data_files.emplace_back(std::move(ref_file));
                wbs.data.putRefPage(page_id, file->pageId());
            }

            // We have to commit those file_ids to PageStorage before applying the ingest, because after the write
            // they are visible for readers immediately, who require file_ids to be found in PageStorage.
            wbs.writeLogAndData();

            bool ingest_success
                = segment->ingestDataToDelta(*dm_context, range.shrink(segment_range), data_files, clear_data_in_range);
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
 * using logical split.
 *
 * You must ensure DTFiles do not overlap. Otherwise this function will not work properly when clear_data_in_range == true.
 * The check is performed in `ingestFiles`.
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
    {
        RUNTIME_CHECK(files.size() == external_files.size(), files.size(), external_files.size());
        for (size_t i = 0; i < files.size(); ++i)
            RUNTIME_CHECK(files[i]->pageId() == external_files[i].id, files[i]->pageId(), external_files[i].toString());
    }

    std::set<SegmentPtr> updated_segments;

    // First phase (DeleteRange Phase):
    // Write DeleteRange to the covered segments to ensure that all data in the `ingest_range` is cleared.
    if (clear_data_in_range)
    {
        RowKeyRange remaining_delete_range = ingest_range;
        LOG_INFO(
            log,
            "Table ingest using split - delete range phase - begin, remaining_delete_range={}",
            remaining_delete_range.toDebugString());

        while (!remaining_delete_range.none())
        {
            auto [segment, is_empty] = getSegmentByStartKey(
                remaining_delete_range.getStart(),
                /*create_if_empty*/ true,
                /*throw_if_notfound*/ true);

            const auto delete_range = remaining_delete_range.shrink(segment->getRowKeyRange());
            RUNTIME_CHECK(
                !delete_range
                     .none(), // as remaining_delete_range is not none, we expect the shrinked range to be not none.
                delete_range.toDebugString(),
                segment->simpleInfo(),
                remaining_delete_range.toDebugString());
            LOG_DEBUG(
                log,
                "Table ingest using split - delete range phase - Try to delete range in segment, delete_range={} "
                "segment={} remaining_delete_range={} updated_segments_n={}",
                delete_range.toDebugString(),
                segment->simpleInfo(),
                remaining_delete_range.toDebugString(),
                updated_segments.size());

            const bool succeeded = segment->write(*dm_context, delete_range);
            if (succeeded)
            {
                updated_segments.insert(segment);
                RUNTIME_CHECK(delete_range.getEnd() >= remaining_delete_range.getStart());
                remaining_delete_range.setStart(delete_range.end); // We always move forward
            }
            else
            {
                // segment may be abandoned, retry current range by finding the segment again.
            }
        }

        LOG_DEBUG(
            log,
            "Table ingest using split - delete range phase - finished, updated_segments_n={}",
            updated_segments.size());
    }

    /*
     * In second phase (SplitIngest Phase),
     * we will try to ingest DMFile one by one into the segments.
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
                "Table ingest using split - split ingest phase - Unexpected empty DMFile, skipped. ingest_range={} "
                "file_idx={} file={}",
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
            auto [segment, is_empty] = getSegmentByStartKey(
                file_ingest_range.getStart(),
                /*create_if_empty*/ true,
                /*throw_if_notfound*/ true);

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
                "Table ingest using split - split ingest phase - Try to ingest file into segment, file_idx={} "
                "file_id=dmf_{} file_ingest_range={} segment={} segment_ingest_range={}",
                file_idx,
                files[file_idx]->fileId(),
                file_ingest_range.toDebugString(),
                segment->simpleInfo(),
                segment_ingest_range.toDebugString());

            const bool succeeded = ingestDTFileIntoSegmentUsingSplit(
                *dm_context,
                segment,
                segment_ingest_range,
                files[file_idx],
                clear_data_in_range);
            if (succeeded)
            {
                updated_segments.insert(segment);
                // We have ingested (DTFileRange ∪ ThisSegmentRange), let's try with next overlapped segment.
                RUNTIME_CHECK(segment_ingest_range.getEnd() > file_ingest_range.getStart());
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

    return std::vector<SegmentPtr>(updated_segments.begin(), updated_segments.end());
}

/**
 * Ingest one DTFile into the target segment by using logical split.
 */
bool DeltaMergeStore::ingestDTFileIntoSegmentUsingSplit(
    DMContext & dm_context,
    const SegmentPtr & segment,
    const RowKeyRange & ingest_range,
    const DMFilePtr & file,
    bool clear_data_in_range)
{
    const auto & segment_range = segment->getRowKeyRange();

    // The ingest_range must fall in segment's range.
    RUNTIME_CHECK(!ingest_range.none(), ingest_range.toDebugString());
    RUNTIME_CHECK(
        segment_range.getStart() <= ingest_range.getStart(),
        segment_range.toDebugString(),
        ingest_range.toDebugString());
    RUNTIME_CHECK(
        segment_range.getEnd() >= ingest_range.getEnd(),
        segment_range.toDebugString(),
        ingest_range.toDebugString());

    const bool is_start_matching = segment_range.getStart() == ingest_range.getStart();
    const bool is_end_matching = segment_range.getEnd() == ingest_range.getEnd();

    if (is_start_matching && is_end_matching)
    {
        /*
         * The segment and the ingest range is perfectly matched.
         *
         * Example:
         *    │----------- Segment ----------│
         *    │-------- Ingest Range --------│
         */

        auto delegate = dm_context.path_pool->getStableDiskDelegator();
        auto file_provider = dm_context.global_context.getFileProvider();

        WriteBatches wbs(*storage_pool, dm_context.getWriteLimiter());

        // Generate DMFile instance with a new ref_id pointed to the file_id,
        // because we may use the same DMFile to ingest into multiple segments.
        auto new_page_id = storage_pool->newDataPageIdForDTFile(delegate, __PRETTY_FUNCTION__);
        auto ref_file = DMFile::restore(
            file_provider,
            file->fileId(),
            new_page_id,
            file->parentPath(),
            DMFileMeta::ReadMode::all(),
            file->metaVersion(),
            keyspace_id);
        wbs.data.putRefPage(new_page_id, file->pageId());

        // We have to commit those file_ids to PageStorage before applying the ingest, because after the write
        // they are visible for readers immediately, who require file_ids to be found in PageStorage.
        wbs.writeLogAndData();

        // clear_all_data_in_segment == clear_data_in_range is safe, because we have verified
        // that current segment range is identical to the ingest range.
        auto new_segment = segmentIngestData(dm_context, segment, ref_file, clear_data_in_range);
        if (new_segment == nullptr)
        {
            // When ingest failed, just discard this ref file.
            wbs.rollbackWrittenLogAndData();

            // likely caused by snapshot failed.
            // Sleep awhile and retry.
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            return false;
        }

        return true;
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
        const auto [left, right] = segmentSplit(
            dm_context,
            segment,
            SegmentSplitReason::ForIngest,
            ingest_range.end,
            SegmentSplitMode::Logical);
        if (left == nullptr || right == nullptr)
        {
            // Split failed, likely caused by snapshot failed.
            // Sleep awhile and retry.
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
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
        const auto [left, right] = segmentSplit(
            dm_context,
            segment,
            SegmentSplitReason::ForIngest,
            ingest_range.start,
            SegmentSplitMode::Logical);
        if (left == nullptr || right == nullptr)
        {
            // Split failed, likely caused by snapshot failed.
            // Sleep awhile and retry.
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
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
        const auto [left, right] = segmentSplit(
            dm_context,
            segment,
            SegmentSplitReason::ForIngest,
            ingest_range.start,
            SegmentSplitMode::Logical);
        if (left == nullptr || right == nullptr)
        {
            // Split failed, likely caused by snapshot failed.
            // Sleep awhile and retry.
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return false;
    }
}

UInt64 DeltaMergeStore::ingestFiles(
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

    {
        // `ingestDTFilesUsingSplit` requires external_files to be not overlapped. Otherwise the results will be incorrect.
        // Here we verify the external_files are ordered and not overlapped.
        // "Ordered" is actually not a hard requirement by `ingestDTFilesUsingSplit`. However "ordered" makes us easy to check overlap efficiently.
        RowKeyValue last_end;
        if (is_common_handle)
            last_end = RowKeyValue::COMMON_HANDLE_MIN_KEY;
        else
            last_end = RowKeyValue::INT_HANDLE_MIN_KEY;

        // Suppose we have keys: 1, 2, | 3, 4, 5, | 6, | 7, 8
        // Our file ranges will be: [1, 3), [3, 6), [6, 7), [7, 9)
        //                              ↑    ↑
        //                              A    B
        //                         We require A <= B.
        for (const auto & ext_file : external_files)
        {
            RUNTIME_CHECK(!ext_file.range.none(), ext_file.toString());
            RUNTIME_CHECK(
                last_end.toRowKeyValueRef() <= ext_file.range.getStart(),
                last_end.toDebugString(),
                ext_file.toString());
            last_end = ext_file.range.end;
        }

        // Check whether all external files are contained by the range.
        if (dm_context->global_context.getSettingsRef().dt_enable_ingest_check)
        {
            for (const auto & ext_file : external_files)
            {
                RUNTIME_CHECK_MSG(
                    range.getStart() <= ext_file.range.getStart() && range.getEnd() >= ext_file.range.getEnd(),
                    "Detected illegal region boundary: range={} file_range={} keyspace={} table_id={}. "
                    "TiFlash will exit to prevent data inconsistency. "
                    "If you accept data inconsistency and want to continue the service, "
                    "set profiles.default.dt_enable_ingest_check=false .",
                    keyspace_id,
                    physical_table_id,
                    range.toDebugString(),
                    ext_file.range.toDebugString());
            }
        }
    }

    EventRecorder write_block_recorder(ProfileEvents::DMWriteFile, ProfileEvents::DMWriteFileNS);

    auto delegate = dm_context->path_pool->getStableDiskDelegator();
    auto file_provider = dm_context->global_context.getFileProvider();

    size_t rows = 0;
    size_t bytes = 0;
    size_t bytes_on_disk = 0;

    auto remote_data_store = dm_context->global_context.getSharedContextDisagg()->remote_data_store;
    StoreID store_id = InvalidStoreID;
    if (remote_data_store)
    {
        store_id = dm_context->global_context.getTMTContext().getKVStore()->getStoreID();
    }

    DMFiles files;
    for (const auto & external_file : external_files)
    {
        // we always create a ref file to this DMFile with all meta info restored later, so here we just restore meta info to calculate its' memory and disk size
        DMFilePtr file;
        if (!remote_data_store)
        {
            auto file_parent_path = delegate.getDTFilePath(external_file.id);
            file = DMFile::restore(
                file_provider,
                external_file.id,
                external_file.id,
                file_parent_path,
                DMFileMeta::ReadMode::memoryAndDiskSize(),
                0 /* FIXME: Support other meta version */,
                keyspace_id);
        }
        else
        {
            Remote::DMFileOID oid{
                .store_id = store_id,
                .keyspace_id = dm_context->keyspace_id,
                .table_id = dm_context->physical_table_id,
                .file_id = external_file.id};
            file = remote_data_store->prepareDMFile(oid, external_file.id)
                       ->restore(DMFileMeta::ReadMode::memoryAndDiskSize(), 0 /* FIXME: Support other meta version */);
        }
        rows += file->getRows();
        bytes += file->getBytes();
        bytes_on_disk += file->getBytesOnDisk();

        // Do some simple verification for the file range.
        if (file->getRows() > 0)
            RUNTIME_CHECK(!external_file.range.none());

        files.emplace_back(std::move(file));
    }

    bool use_split_replace = false;
    if (bytes >= dm_context->delta_small_column_file_bytes)
    {
        // We still write small ssts directly into the delta layer.
        use_split_replace = true;
    }

    fiu_do_on(FailPoints::force_ingest_via_delta, { use_split_replace = false; });
    fiu_do_on(FailPoints::force_ingest_via_replace, { use_split_replace = true; });

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
            "Table ingest files - begin, use_split_replace={} files={} rows={} bytes={} bytes_on_disk={} range={} "
            "clear={}",
            use_split_replace,
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
            if (!remote_data_store)
            {
                ingest_wbs.data.putExternal(file->fileId(), 0);
            }
            else
            {
                Remote::DMFileOID oid{
                    .store_id = store_id,
                    .keyspace_id = dm_context->keyspace_id,
                    .table_id = dm_context->physical_table_id,
                    .file_id = file->fileId()};
                PS::V3::CheckpointLocation loc{
                    .data_file_id = std::make_shared<String>(S3::S3Filename::fromDMFileOID(oid).toFullKey()),
                    .offset_in_file = 0,
                    .size_in_file = 0,
                };
                ingest_wbs.data.putRemoteExternal(file->fileId(), loc);
            }
        }
        ingest_wbs.writeLogAndData();
        ingest_wbs.setRollback(); // rollback if exception thrown
    }

    Segments updated_segments;
    if (!range.none())
    {
        bool has_segments = true;
        {
            std::shared_lock lock(read_write_mutex);
            if (segments.empty())
            {
                has_segments = false;
            }
        }
        if (has_segments || !external_files.empty())
        {
            if (use_split_replace)
                updated_segments
                    = ingestDTFilesUsingSplit(dm_context, range, external_files, files, clear_data_in_range);
            else
                updated_segments = ingestDTFilesUsingColumnFile(dm_context, range, files, clear_data_in_range);
        }
    }

    // Enable gc for DTFile after all segment applied.
    // Note that we can not enable gc for them once they have applied to any segments.
    // Assume that one segment get compacted after file ingested, `gc_handle` gc the
    // DTFiles before they get applied to all segments. Then we will apply some
    // deleted DTFiles to other segments.
    if (auto data_store = dm_context->global_context.getSharedContextDisagg()->remote_data_store; !data_store)
    {
        for (auto & file : files)
            file->enableGC();
    }
    else
    {
        auto delegator = dm_context->path_pool->getStableDiskDelegator();
        for (auto & file : files)
            delegator.enableGCForRemoteDTFile(file->fileId());
    }
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
        checkSegmentUpdate(dm_context, segment, ThreadType::Write, InputType::RaftSSTAndSnap);

    return bytes;
}

std::vector<SegmentPtr> DeltaMergeStore::ingestSegmentsUsingSplit(
    const DMContextPtr & dm_context,
    const RowKeyRange & ingest_range,
    const std::vector<SegmentPtr> & segments_to_ingest)
{
    std::set<SegmentPtr> updated_segments;

    // First phase (DeleteRange Phase):
    // Write DeleteRange to the covered segments to ensure that all data in the `ingest_range` is cleared.
    {
        RowKeyRange remaining_delete_range = ingest_range;
        LOG_INFO(
            log,
            "Table ingest checkpoint using split - delete range phase - begin, remaining_delete_range={}",
            remaining_delete_range.toDebugString());

        while (!remaining_delete_range.none())
        {
            auto [segment, is_empty] = getSegmentByStartKey(
                remaining_delete_range.getStart(),
                /*create_if_empty*/ true,
                /*throw_if_notfound*/ true);

            const auto delete_range = remaining_delete_range.shrink(segment->getRowKeyRange());
            RUNTIME_CHECK(
                !delete_range
                     .none(), // as remaining_delete_range is not none, we expect the shrinked range to be not none.
                delete_range.toDebugString(),
                segment->simpleInfo(),
                remaining_delete_range.toDebugString());
            LOG_DEBUG(
                log,
                "Table ingest checkpoint using split - delete range phase - Try to delete range in segment, "
                "delete_range={} segment={} remaining_delete_range={} updated_segments_n={}",
                delete_range.toDebugString(),
                segment->simpleInfo(),
                remaining_delete_range.toDebugString(),
                updated_segments.size());

            const bool succeeded = segment->write(*dm_context, delete_range);
            if (succeeded)
            {
                updated_segments.insert(segment);
                RUNTIME_CHECK(delete_range.getEnd() >= remaining_delete_range.getStart());
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
        "Table ingest checkpoint using split - delete range phase - finished, updated_segments_n={}",
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
        "Table ingest checkpoint using split - split ingest phase - begin, ingest_range={}, files_n={}",
        ingest_range.toDebugString(),
        segments_to_ingest.size());

    for (size_t remote_segment_idx = 0; remote_segment_idx < segments_to_ingest.size(); remote_segment_idx++)
    {
        // We may meet empty segment, just ignore it
        if (segments_to_ingest[remote_segment_idx]->getEstimatedRows() == 0)
        {
            LOG_INFO(
                log,
                "Table ingest checkpoint using split - split ingest phase - Meet empty Segment, skipped. "
                "ingest_range={} segment_idx={}",
                ingest_range.toDebugString(),
                remote_segment_idx);
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
        auto file_ingest_range = segments_to_ingest[remote_segment_idx]->getRowKeyRange();
        while (!file_ingest_range.none()) // This DMFile has remaining data to ingest
        {
            auto [segment, is_empty] = getSegmentByStartKey(
                file_ingest_range.getStart(),
                /*create_if_empty*/ true,
                /*throw_if_notfound*/ true);

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
                "Table ingest checkpoint using split - split ingest phase - Try to ingest file into segment, "
                "remote_segment_idx={} remote_segment_id={} remote_ingest_range={} segment={} segment_ingest_range={}",
                remote_segment_idx,
                segments_to_ingest[remote_segment_idx]->segmentId(),
                file_ingest_range.toDebugString(),
                segment->simpleInfo(),
                segment_ingest_range.toDebugString());

            const bool succeeded = ingestSegmentDataIntoSegmentUsingSplit(
                *dm_context,
                segment,
                segment_ingest_range,
                segments_to_ingest[remote_segment_idx]);
            if (succeeded)
            {
                updated_segments.insert(segment);
                // We have ingested (DTFileRange ∪ ThisSegmentRange), let's try with next overlapped segment.
                RUNTIME_CHECK(segment_ingest_range.getEnd() > file_ingest_range.getStart());
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
        "Table ingest checkpoint using split - split ingest phase - finished, updated_segments_n={}",
        updated_segments.size());

    return std::vector<SegmentPtr>(updated_segments.begin(), updated_segments.end());
}

bool DeltaMergeStore::ingestSegmentDataIntoSegmentUsingSplit(
    DMContext & dm_context,
    const SegmentPtr & segment,
    const RowKeyRange & ingest_range,
    const SegmentPtr & segment_to_ingest)
{
    const auto & segment_range = segment->getRowKeyRange();

    // The ingest_range must fall in segment's range.
    RUNTIME_CHECK(!ingest_range.none(), ingest_range.toDebugString());
    RUNTIME_CHECK(
        segment_range.getStart() <= ingest_range.getStart(),
        segment_range.toDebugString(),
        ingest_range.toDebugString());
    RUNTIME_CHECK(
        segment_range.getEnd() >= ingest_range.getEnd(),
        segment_range.toDebugString(),
        ingest_range.toDebugString());

    const bool is_start_matching = segment_range.getStart() == ingest_range.getStart();
    const bool is_end_matching = segment_range.getEnd() == ingest_range.getEnd();

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
        WriteBatches wbs{*dm_context.storage_pool};
        auto dm_files = segment_to_ingest->getStable()->getDMFiles();
        auto [in_memory_files, column_file_persisteds] = segment_to_ingest->getDelta()->cloneAllColumnFiles(
            segment_to_ingest->mustGetUpdateLock(),
            dm_context,
            ingest_range,
            wbs);
        wbs.writeLogAndData();
        RUNTIME_CHECK(in_memory_files.empty());
        RUNTIME_CHECK(dm_files.size() == 1);
        const auto new_segment_or_null
            = segmentDangerouslyReplaceDataFromCheckpoint(dm_context, segment, dm_files[0], column_file_persisteds);
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
        const auto [left, right] = segmentSplit(
            dm_context,
            segment,
            SegmentSplitReason::ForIngest,
            ingest_range.end,
            SegmentSplitMode::Logical);
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
        const auto [left, right] = segmentSplit(
            dm_context,
            segment,
            SegmentSplitReason::ForIngest,
            ingest_range.start,
            SegmentSplitMode::Logical);
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
        const auto [left, right] = segmentSplit(
            dm_context,
            segment,
            SegmentSplitReason::ForIngest,
            ingest_range.start,
            SegmentSplitMode::Logical);
        if (left == nullptr || right == nullptr)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(15));
        }
        return false;
    }
}

Segments DeltaMergeStore::buildSegmentsFromCheckpointInfo(
    const DMContextPtr & dm_context,
    const std::shared_ptr<GeneralCancelHandle> & cancel_handle,
    const DM::RowKeyRange & range,
    const CheckpointInfoPtr & checkpoint_info) const
{
    if (unlikely(range.none()))
    {
        return {};
    }
    LOG_INFO(
        log,
        "Build checkpoint from remote, store_id={} region_id={} range={}",
        checkpoint_info->remote_store_id,
        checkpoint_info->region_id,
        range.toDebugString());
    WriteBatches wbs{*dm_context->storage_pool};
    try
    {
        auto segment_meta_infos
            = Segment::readAllSegmentsMetaInfoInRange(*dm_context, cancel_handle, range, checkpoint_info);
        if (cancel_handle->isCanceled())
        {
            // Will be cleared in `FastAddPeerWrite`.
            return {};
        }
        LOG_INFO(
            log,
            "Finish read all segments meta info in range, region_id={} segments_num={}",
            checkpoint_info->region_id,
            segment_meta_infos.size());
        auto restored_segments = Segment::createTargetSegmentsFromCheckpoint( //
            log,
            checkpoint_info->region_id,
            *dm_context,
            checkpoint_info->remote_store_id,
            segment_meta_infos,
            range,
            checkpoint_info->temp_ps,
            wbs);
        if (restored_segments.empty())
        {
            return {};
        }
        wbs.writeLogAndData();
        LOG_INFO(
            log,
            "Finish write fap checkpoint, region_id={} segments_num={}",
            checkpoint_info->region_id,
            segment_meta_infos.size());
        return restored_segments;
    }
    catch (const Exception & e)
    {
        LOG_INFO(
            log,
            "Build checkpoint from remote failed for {}, region_id={} remote_store_id={}",
            e.message(),
            checkpoint_info->region_id,
            checkpoint_info->remote_store_id);
        wbs.setRollback();
        e.rethrow();
    }
    return {};
}

UInt64 DeltaMergeStore::ingestSegmentsFromCheckpointInfo(
    const DMContextPtr & dm_context,
    const DM::RowKeyRange & range,
    const CheckpointIngestInfoPtr & checkpoint_info)
{
    if (unlikely(shutdown_called.load(std::memory_order_relaxed)))
    {
        const auto msg = fmt::format("Try to ingest files into a shutdown table, store_id={}", log->identifier());
        LOG_WARNING(log, "{}", msg);
        throw Exception(msg);
    }

    if (unlikely(range.none()))
    {
        LOG_INFO(
            log,
            "Ingest checkpoint from remote meet empty range, ignore, store_id={} region_id={}",
            checkpoint_info->getRemoteStoreId(),
            checkpoint_info->regionId());
        return 0;
    }

    auto restored_segments = checkpoint_info->getRestoredSegments();
    auto updated_segments = ingestSegmentsUsingSplit(dm_context, range, restored_segments);
    auto estimated_bytes = 0;

    for (const auto & segment : restored_segments)
    {
        estimated_bytes += segment->getEstimatedBytes();
    }

    LOG_INFO(
        log,
        "Ingest checkpoint from remote done, store_id={} region_id={} n_segments={} est_bytes={}",
        checkpoint_info->getRemoteStoreId(),
        checkpoint_info->regionId(),
        restored_segments.size(),
        estimated_bytes);

    WriteBatches wbs{*dm_context->storage_pool};
    for (auto & segment : restored_segments)
    {
        auto delta = segment->getDelta();
        auto stable = segment->getStable();
        delta->recordRemoveColumnFilesPages(wbs);
        stable->recordRemovePacksPages(wbs);
        wbs.writeRemoves();
    }

    // TODO(fap) This could be executed in a dedicated thread if it consumes too much time.
    for (auto & segment : updated_segments)
        checkSegmentUpdate(dm_context, segment, ThreadType::Write, InputType::RaftSSTAndSnap);

    return estimated_bytes;
}

} // namespace DM
} // namespace DB
