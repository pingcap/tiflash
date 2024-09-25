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

#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/SyncPoint/SyncPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/Decode/SSTFilesToDTFilesOutputStream.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/KVStore/Decode/PartitionStreams.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/FFI/SSTReader.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/StorageDeltaMerge.h>
#include <common/logger_useful.h>

#include <magic_enum.hpp>

namespace DB
{
namespace DM
{

template <typename ChildStream>
SSTFilesToDTFilesOutputStream<ChildStream>::SSTFilesToDTFilesOutputStream( //
    const std::string & log_prefix_,
    ChildStream child_,
    StorageDeltaMergePtr storage_,
    DecodingStorageSchemaSnapshotConstPtr schema_snap_,
    FileConvertJobType job_type_,
    UInt64 split_after_rows_,
    UInt64 split_after_size_,
    UInt64 region_id_,
    std::shared_ptr<PreHandlingTrace::Item> prehandle_task_,
    Context & context_)
    : child(std::move(child_))
    , storage(std::move(storage_))
    , schema_snap(std::move(schema_snap_))
    , job_type(job_type_)
    , split_after_rows(split_after_rows_)
    , split_after_size(split_after_size_)
    , region_id(region_id_)
    , prehandle_task(prehandle_task_)
    , context(context_)
    , log(Logger::get(log_prefix_))
{}

template <typename ChildStream>
SSTFilesToDTFilesOutputStream<ChildStream>::~SSTFilesToDTFilesOutputStream() = default;

template <typename ChildStream>
void SSTFilesToDTFilesOutputStream<ChildStream>::writePrefix()
{
    child->readPrefix();
    total_committed_rows = 0;
    total_committed_bytes = 0;
    watch.start();
}

template <typename ChildStream>
void SSTFilesToDTFilesOutputStream<ChildStream>::writeSuffix()
{
    child->readSuffix();

    finalizeDTFileStream();

    const auto process_keys = child->getProcessKeys();
    switch (job_type)
    {
    case FileConvertJobType::ApplySnapshot:
    {
        GET_METRIC(tiflash_raft_command_duration_seconds, type_apply_snapshot_predecode_sst2dt)
            .Observe(watch.elapsedSeconds());
        // Note that number of keys in different cf will be aggregated into one metrics
        GET_METRIC(tiflash_raft_process_keys, type_apply_snapshot).Increment(process_keys.total());
        GET_METRIC(tiflash_raft_process_keys, type_apply_snapshot_default).Increment(process_keys.default_cf);
        GET_METRIC(tiflash_raft_process_keys, type_apply_snapshot_write).Increment(process_keys.write_cf);
        break;
    }
    case FileConvertJobType::IngestSST:
    {
        GET_METRIC(tiflash_raft_command_duration_seconds, type_ingest_sst_sst2dt).Observe(watch.elapsedSeconds());
        // Note that number of keys in different cf will be aggregated into one metrics
        GET_METRIC(tiflash_raft_process_keys, type_ingest_sst).Increment(process_keys.total());
        break;
    }
    }

    LOG_INFO(
        log,
        "Transformed snapshot in SSTFile to DTFiles,"
        " region={} job_type={} cost_ms={} rows={} bytes={} bytes_on_disk={}"
        " write_cf_keys={} default_cf_keys={} lock_cf_keys={} splid_id={} dt_files=[{}]",
        child->getRegion()->toString(true),
        magic_enum::enum_name(job_type),
        watch.elapsedMilliseconds(),
        total_committed_rows,
        total_committed_bytes,
        total_bytes_on_disk,
        process_keys.write_cf,
        process_keys.default_cf,
        process_keys.lock_cf,
        child->getSplitId(),
        [&] {
            FmtBuffer fmt_buf;
            fmt_buf.fmtAppend("files_num={} ", ingest_files.size());
            fmt_buf.joinStr(
                ingest_files.begin(),
                ingest_files.end(),
                [](const DMFilePtr & file, FmtBuffer & fb) { fb.fmtAppend("dmf_{}", file->fileId()); },
                ",");
            return fmt_buf.toString();
        }());

    // We could create an async task once a DMFile is generated in `finalizeDTFileStream`
    // to take more resources to shorten the time of IngestSST/ApplySnapshot.
    auto remote_data_store = context.getSharedContextDisagg()->remote_data_store;
    if (remote_data_store)
    {
        Stopwatch upload_watch;
        const StoreID store_id = context.getTMTContext().getKVStore()->getStoreID();
        const auto table_info = storage->getTableInfo();
        for (const auto & file : ingest_files)
        {
            Remote::DMFileOID oid{
                .store_id = store_id,
                .keyspace_id = table_info.keyspace_id,
                .table_id = table_info.id,
                .file_id = file->fileId()};
            remote_data_store->putDMFile(file, oid, /*remove_local*/ true);
        }
        const auto elapsed_seconds = upload_watch.elapsedSeconds();
        LOG_INFO(
            log,
            "Upload snapshot DTFiles done, region={} store_id={} n_dt_files={} cost={:.3f}s",
            child->getRegion()->toString(true),
            store_id,
            ingest_files.size(),
            elapsed_seconds);
        switch (job_type)
        {
        case FileConvertJobType::ApplySnapshot:
            GET_METRIC(tiflash_raft_command_duration_seconds, type_apply_snapshot_predecode_upload)
                .Observe(elapsed_seconds);
            break;
        case FileConvertJobType::IngestSST:
            GET_METRIC(tiflash_raft_command_duration_seconds, type_ingest_sst_upload).Observe(elapsed_seconds);
            break;
        }
    }
}

template <typename ChildStream>
bool SSTFilesToDTFilesOutputStream<ChildStream>::newDTFileStream()
{
    RUNTIME_CHECK(dt_stream == nullptr);

    // The parent_path and file_id are generated by the storage.
    auto [parent_path, file_id] = storage->getAndMaybeInitStore()->preAllocateIngestFile();
    if (parent_path.empty())
    {
        // Can not allocate path and id for storing DTFiles (the storage may be dropped / shutdown)
        return false;
    }

    auto dt_file = DMFile::create(
        file_id,
        parent_path,
        storage->createChecksumConfig(),
        context.getGlobalContext().getSettingsRef().dt_small_file_size_threshold,
        context.getGlobalContext().getSettingsRef().dt_merged_file_max_size,
        storage->getKeyspaceID());
    dt_stream = std::make_unique<DMFileBlockOutputStream>(context, dt_file, *(schema_snap->column_defines));
    dt_stream->writePrefix();
    ingest_files.emplace_back(dt_file);
    ingest_files_range.emplace_back(std::nullopt);
    committed_rows_this_dt_file = 0;
    committed_bytes_this_dt_file = 0;

    LOG_DEBUG(
        log,
        "Create new DTFile for snapshot data, region_id={} file_idx={} file={}",
        child->getRegion()->id(),
        ingest_files.size() - 1,
        dt_file->path());

    return true;
}

template <typename ChildStream>
bool SSTFilesToDTFilesOutputStream<ChildStream>::finalizeDTFileStream()
{
    if (unlikely(dt_stream == nullptr))
    {
        // Maybe error happened in `newDTFileStream`, or no data has been written since last finalize.
        return false;
    }

    dt_stream->writeSuffix();
    auto dt_file = dt_stream->getFile();
    assert(!dt_file->canGC()); // The DTFile should not be able to gc until it is ingested.
    const auto bytes_written = dt_file->getBytesOnDisk();
    total_bytes_on_disk += bytes_written;

    // If remote data store is not enabled, add the DTFile to StoragePathPool so that we can restore it later
    // Else just add it's size to disk delegator
    storage->getAndMaybeInitStore()->preIngestFile(dt_file->parentPath(), dt_file->fileId(), bytes_written);

    dt_stream.reset();

    LOG_INFO(
        log,
        "Finished writing DTFile from snapshot data, region={} file_idx={} file_rows={} file_bytes={} data_range={} "
        "file_bytes_on_disk={} file={}",
        child->getRegion()->toString(true),
        ingest_files.size() - 1,
        committed_rows_this_dt_file,
        committed_bytes_this_dt_file,
        ingest_files_range.back().has_value() ? ingest_files_range.back()->toDebugString() : "(null)",
        bytes_written,
        dt_file->path());

    return true;
}

template <typename ChildStream>
void SSTFilesToDTFilesOutputStream<ChildStream>::write()
{
    size_t last_effective_num_rows = 0;
    size_t last_not_clean_rows = 0;
    size_t last_deleted_rows = 0;
    size_t cur_effective_num_rows = 0;
    size_t cur_not_clean_rows = 0;
    size_t cur_deleted_rows = 0;
    while (true)
    {
        if (prehandle_task->isAbort())
        {
            break;
        }
        SYNC_FOR("before_SSTFilesToDTFilesOutputStream::handle_one");
        Block block = child->read();
        if (!block)
            break;
        if (unlikely(block.rows() == 0))
            continue;

        if (dt_stream == nullptr)
        {
            // If can not create DTFile stream (the storage may be dropped / shutdown),
            // break the writing loop.
            if (bool ok = newDTFileStream(); !ok)
                break;
        }

        {
            // Check whether rows are sorted by handle & version in ascending order.
            SortDescription sort;
            sort.emplace_back(MutableSupport::tidb_pk_column_name, 1, 0);
            sort.emplace_back(MutableSupport::version_column_name, 1, 0);

            if (unlikely(block.rows() > 1 && !isAlreadySorted(block, sort)))
            {
                const String error_msg = fmt::format(
                    "The block decoded from SSTFile is not sorted by primary key and version {}",
                    child->getRegion()->toString(true));
                LOG_ERROR(log, error_msg);
                FieldVisitorToString visitor;
                const size_t nrows = block.rows();
                for (size_t i = 0; i < nrows; ++i)
                {
                    const auto & pk_col = block.getByName(MutableSupport::tidb_pk_column_name);
                    const auto & ver_col = block.getByName(MutableSupport::version_column_name);
                    LOG_ERROR(
                        log,
                        "[Row={}/{}] [pk={}] [ver={}]",
                        i,
                        nrows,
                        applyVisitor(visitor, (*pk_col.column)[i]),
                        applyVisitor(visitor, (*ver_col.column)[i]));
                }
                throw Exception(error_msg);
            }
        }

        updateRangeFromNonEmptyBlock(block); // We have checked block is not empty previously.

        // Write block to the output stream
        DMFileBlockOutputStream::BlockProperty property;
        std::tie(cur_effective_num_rows, cur_not_clean_rows, cur_deleted_rows, property.gc_hint_version) //
            = child->getMvccStatistics();
        property.effective_num_rows = cur_effective_num_rows - last_effective_num_rows;
        property.not_clean_rows = cur_not_clean_rows - last_not_clean_rows;
        property.deleted_rows = cur_deleted_rows - last_deleted_rows;
        last_effective_num_rows = cur_effective_num_rows;
        last_not_clean_rows = cur_not_clean_rows;
        last_deleted_rows = cur_deleted_rows;
        dt_stream->write(block, property);

        auto rows = block.rows();
        auto bytes = block.bytes();
        total_committed_rows += rows;
        total_committed_bytes += bytes;
        committed_rows_this_dt_file += rows;
        committed_bytes_this_dt_file += bytes;
        auto should_split_dt_file
            = ((split_after_rows > 0 && committed_rows_this_dt_file >= split_after_rows) || //
               (split_after_size > 0 && committed_bytes_this_dt_file >= split_after_size));
        if (should_split_dt_file)
            finalizeDTFileStream();
    }
}

template <typename ChildStream>
std::vector<ExternalDTFileInfo> SSTFilesToDTFilesOutputStream<ChildStream>::outputFiles() const
{
    RUNTIME_CHECK(ingest_files.size() == ingest_files_range.size(), ingest_files.size(), ingest_files_range.size());

    auto files = std::vector<ExternalDTFileInfo>{};
    files.reserve(ingest_files.size());
    for (size_t i = 0; i < ingest_files.size(); i++)
    {
        // We should never have empty DTFile and empty DTFile ranges.
        RUNTIME_CHECK(ingest_files_range[i].has_value());

        auto external_file = ExternalDTFileInfo{
            .id = ingest_files[i]->fileId(),
            .range = ingest_files_range[i].value(),
        };
        files.emplace_back(external_file);
    }

    return files;
}

template <typename ChildStream>
void SSTFilesToDTFilesOutputStream<ChildStream>::cancel()
{
    for (auto & file : ingest_files)
    {
        try
        {
            // If DMFile has pre-ingested, remove it.
            storage->getAndMaybeInitStore()->removePreIngestFile(file->fileId(), /*throw_on_not_exist*/ false);
            // Remove local DMFile.
            file->remove(context.getFileProvider());
        }
        catch (...)
        {
            tryLogCurrentException(
                log,
                fmt::format(
                    "ignore exception while canceling SST files to DeltaTree files stream [file={}]",
                    file->path()));
        }
    }
    ingest_files.clear();
    ingest_files_range.clear();
}

template <typename ChildStream>
void SSTFilesToDTFilesOutputStream<ChildStream>::updateRangeFromNonEmptyBlock(Block & block)
{
    // TODO: Now we update the rangeEnd for every block. We can update the rangeEnd only once,
    //  just before a new DTFile is going to be created.

    RUNTIME_CHECK(block.rows() > 0);

    const auto & pk_col = block.getByName(MutableSupport::tidb_pk_column_name);
    const auto rowkey_column = RowKeyColumnContainer(pk_col.column, schema_snap->is_common_handle);
    auto & current_file_range = ingest_files_range.back();

    auto const block_start = rowkey_column.getRowKeyValue(0);
    auto const block_end = rowkey_column
                               .getRowKeyValue(pk_col.column->size() - 1) //
                               .toRowKeyValue()
                               .toNext(); // because range is right-open.

    // Note: The underlying stream ensures that one row key will not fall into two blocks (when there are multiple versions).
    // So we will never have overlapped range.
    RUNTIME_CHECK(block_start < block_end.toRowKeyValueRef());

    if (!current_file_range.has_value())
    {
        current_file_range = RowKeyRange( //
            block_start.toRowKeyValue(),
            block_end,
            schema_snap->is_common_handle,
            schema_snap->rowkey_column_size);
    }
    else
    {
        RUNTIME_CHECK(block_start > current_file_range->getStart());
        current_file_range->setEnd(block_end);
    }
    RUNTIME_CHECK(!current_file_range->none());
}

template class SSTFilesToDTFilesOutputStream<BoundedSSTFilesToBlockInputStreamPtr>;
template class SSTFilesToDTFilesOutputStream<MockSSTFilesToDTFilesOutputStreamChildPtr>;

} // namespace DM
} // namespace DB
