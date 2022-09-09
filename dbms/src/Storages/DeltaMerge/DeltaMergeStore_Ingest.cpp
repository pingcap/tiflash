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
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/File/DMFile.h>

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

void DeltaMergeStore::ingestFiles(
    const DMContextPtr & dm_context,
    const RowKeyRange & range,
    const PageIds & file_ids,
    bool clear_data_in_range)
{
    if (unlikely(shutdown_called.load(std::memory_order_relaxed)))
    {
        const auto msg = fmt::format("Try to ingest files into a shutdown table, store={}", log->identifier());
        LOG_FMT_WARNING(log, "{}", msg);
        throw Exception(msg);
    }

    EventRecorder write_block_recorder(ProfileEvents::DMWriteFile, ProfileEvents::DMWriteFileNS);

    auto delegate = dm_context->path_pool.getStableDiskDelegator();
    auto file_provider = dm_context->db_context.getFileProvider();

    size_t rows = 0;
    size_t bytes = 0;
    size_t bytes_on_disk = 0;

    DMFiles files;
    for (auto file_id : file_ids)
    {
        auto file_parent_path = delegate.getDTFilePath(file_id);

        // we always create a ref file to this DMFile with all meta info restored later, so here we just restore meta info to calculate its' memory and disk size
        auto file = DMFile::restore(file_provider, file_id, file_id, file_parent_path, DMFile::ReadMetaMode::memoryAndDiskSize());
        rows += file->getRows();
        bytes += file->getBytes();
        bytes_on_disk += file->getBytesOnDisk();

        files.emplace_back(std::move(file));
    }

    {
        auto get_ingest_files = [&] {
            FmtBuffer fmt_buf;
            fmt_buf.append("[");
            fmt_buf.joinStr(
                file_ids.begin(),
                file_ids.end(),
                [](const PageId id, FmtBuffer & fb) { fb.fmtAppend("dmf_{}", id); },
                ",");
            fmt_buf.append("]");
            return fmt_buf.toString();
        };
        LOG_FMT_INFO(
            log,
            "Begin table ingest files, files={} rows={} bytes={} bytes_on_disk={} range={} clear={}",
            get_ingest_files(),
            rows,
            bytes,
            bytes_on_disk,
            range.toDebugString(),
            clear_data_in_range);
    }

    Segments updated_segments;
    RowKeyRange cur_range = range;

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
                if (segment_it == segments.end())
                {
                    throw Exception(
                        fmt::format("Failed to locate segment begin with start in range: {}", cur_range.toDebugString()),
                        ErrorCodes::LOGICAL_ERROR);
                }
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
                    segmentMergeDelta(*dm_context, segment, TaskRunThread::BackgroundThreadPool);
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
        // Example: "ingested_files=[dmf_1001,dmf_1002,dmf_1003] updated_segments=[<segment_id=1 ...>,<segment_id=3 ...>]"
        //          "ingested_files=[] updated_segments=[<segment_id=1 ...>,<segment_id=3 ...>]"
        auto get_ingest_info = [&] {
            FmtBuffer fmt_buf;
            fmt_buf.append("ingested_files=[");
            fmt_buf.joinStr(
                file_ids.begin(),
                file_ids.end(),
                [](const PageId id, FmtBuffer & fb) { fb.fmtAppend("dmf_{}", id); },
                ",");
            fmt_buf.append("] updated_segments=[");
            fmt_buf.joinStr(
                updated_segments.begin(),
                updated_segments.end(),
                [](const auto & segment, FmtBuffer & fb) { fb.fmtAppend("{}", segment->simpleInfo()); },
                ",");
            fmt_buf.append("]");
            return fmt_buf.toString();
        };

        LOG_FMT_INFO(
            log,
            "Finish table ingest files, ingested files into segments, {} clear={}",
            get_ingest_info(),
            clear_data_in_range);
    }

    GET_METRIC(tiflash_storage_throughput_bytes, type_ingest).Increment(bytes);
    GET_METRIC(tiflash_storage_throughput_rows, type_ingest).Increment(rows);

    flushCache(dm_context, range);

    // TODO: Update the tracing_id before checkSegmentUpdate?
    for (auto & segment : updated_segments)
        checkSegmentUpdate(dm_context, segment, ThreadType::Write);
}

}
}
