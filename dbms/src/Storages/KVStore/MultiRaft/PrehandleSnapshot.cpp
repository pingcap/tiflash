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
#include <Common/TiFlashMetrics.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/SSTFilesToBlockInputStream.h>
#include <Storages/DeltaMerge/SSTFilesToDTFilesOutputStream.h>
#include <Storages/KVStore/Decode/PartitionStreams.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/FFI/SSTReader.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Types.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>
#include <TiDB/Schema/SchemaSyncer.h>
#include <TiDB/Schema/TiDBSchemaManager.h>

namespace DB
{
namespace FailPoints
{
extern const char force_set_sst_to_dtfile_block_size[];
} // namespace FailPoints

namespace ErrorCodes
{
extern const int TABLE_IS_DROPPED;
extern const int REGION_DATA_SCHEMA_UPDATED;
} // namespace ErrorCodes

enum class ReadFromStreamError
{
    Ok,
    Aborted,
    ErrUpdateSchema,
    ErrTableDropped,
};

struct ReadFromStreamResult
{
    ReadFromStreamError error = ReadFromStreamError::Ok;
    std::string extra_msg;
};

static inline std::tuple<ReadFromStreamResult, PrehandleResult> executeTransform(
    const RegionPtr & new_region,
    const std::shared_ptr<std::atomic_bool> & prehandle_task,
    DM::FileConvertJobType job_type,
    const std::shared_ptr<StorageDeltaMerge> & storage,
    const std::shared_ptr<DM::SSTFilesToBlockInputStream> & sst_stream,
    const DM::SSTFilesToBlockInputStreamOpts & opts,
    TMTContext & tmt)
{
    auto region_id = new_region->id();
    std::shared_ptr<DM::SSTFilesToDTFilesOutputStream<DM::BoundedSSTFilesToBlockInputStreamPtr>> stream;
    // If any schema changes is detected during decoding SSTs to DTFiles, we need to cancel and recreate DTFiles with
    // the latest schema. Or we will get trouble in `BoundedSSTFilesToBlockInputStream`.
    try
    {
        auto & context = tmt.getContext();
        auto & global_settings = context.getGlobalContext().getSettingsRef();
        // Read from SSTs and refine the boundary of blocks output to DTFiles
        auto bounded_stream = std::make_shared<DM::BoundedSSTFilesToBlockInputStream>(
            sst_stream,
            ::DB::TiDBPkColumnID,
            opts.schema_snap);

        stream = std::make_shared<DM::SSTFilesToDTFilesOutputStream<DM::BoundedSSTFilesToBlockInputStreamPtr>>(
            opts.log_prefix,
            bounded_stream,
            storage,
            opts.schema_snap,
            job_type,
            /* split_after_rows */ global_settings.dt_segment_limit_rows,
            /* split_after_size */ global_settings.dt_segment_limit_size,
            region_id,
            prehandle_task,
            context);

        stream->writePrefix();
        stream->write();
        stream->writeSuffix();
        auto res = ReadFromStreamResult{.error = ReadFromStreamError::Ok, .extra_msg = ""};
        if (stream->isAbort())
        {
            stream->cancel();
            res = ReadFromStreamResult{.error = ReadFromStreamError::Aborted, .extra_msg = ""};
        }
        return std::make_pair(
            std::move(res),
            PrehandleResult{
                .ingest_ids = stream->outputFiles(),
                .stats = PrehandleResult::Stats{
                    .raft_snapshot_bytes = sst_stream->getProcessKeys().total_bytes(),
                    .dt_disk_bytes = stream->getTotalBytesOnDisk(),
                    .dt_total_bytes = stream->getTotalCommittedBytes()}});
    }
    catch (DB::Exception & e)
    {
        if (stream != nullptr)
        {
            // Remove all DMFiles.
            stream->cancel();
        }
        if (e.code() == ErrorCodes::REGION_DATA_SCHEMA_UPDATED)
        {
            return std::make_pair(
                ReadFromStreamResult{.error = ReadFromStreamError::ErrUpdateSchema, .extra_msg = e.displayText()},
                PrehandleResult{});
        }
        else if (e.code() == ErrorCodes::TABLE_IS_DROPPED)
        {
            return std::make_pair(
                ReadFromStreamResult{.error = ReadFromStreamError::ErrTableDropped, .extra_msg = e.displayText()},
                PrehandleResult{});
        }
        throw;
    }
}

// It is currently a wrapper for preHandleSSTsToDTFiles.
PrehandleResult KVStore::preHandleSnapshotToFiles(
    RegionPtr new_region,
    const SSTViewVec snaps,
    uint64_t index,
    uint64_t term,
    std::optional<uint64_t> deadline_index,
    TMTContext & tmt)
{
    new_region->beforePrehandleSnapshot(new_region->id(), deadline_index);
    PrehandleResult result;
    try
    {
        SCOPE_EXIT({ new_region->afterPrehandleSnapshot(); });
        result = preHandleSSTsToDTFiles( //
            new_region,
            snaps,
            index,
            term,
            DM::FileConvertJobType::ApplySnapshot,
            tmt);
    }
    catch (DB::Exception & e)
    {
        e.addMessage(
            fmt::format("(while preHandleSnapshot region_id={}, index={}, term={})", new_region->id(), index, term));
        e.rethrow();
    }
    return result;
}

/// `preHandleSSTsToDTFiles` read data from SSTFiles and generate DTFile(s) for commited data
/// return the ids of DTFile(s), the uncommitted data will be inserted to `new_region`
PrehandleResult KVStore::preHandleSSTsToDTFiles(
    RegionPtr new_region,
    const SSTViewVec snaps,
    uint64_t index,
    uint64_t term,
    DM::FileConvertJobType job_type,
    TMTContext & tmt)
{
    // if it's only a empty snapshot, we don't create the Storage object, but return directly.
    if (snaps.len == 0)
    {
        return {};
    }
    auto context = tmt.getContext();
    auto keyspace_id = new_region->getKeyspaceID();
    bool force_decode = false;
    size_t expected_block_size = DEFAULT_MERGE_BLOCK_SIZE;

    // Use failpoint to change the expected_block_size for some test cases
    fiu_do_on(FailPoints::force_set_sst_to_dtfile_block_size, {
        if (auto v = FailPointHelper::getFailPointVal(FailPoints::force_set_sst_to_dtfile_block_size); v)
            expected_block_size = std::any_cast<size_t>(v.value());
    });

    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_raft_command_duration_seconds, type_apply_snapshot_predecode)
            .Observe(watch.elapsedSeconds());
    });

    PrehandleResult prehandle_result;
    TableID physical_table_id = InvalidTableID;

    auto region_id = new_region->id();
    auto prehandle_task = prehandling_trace.registerTask(region_id);
    while (true)
    {
        // If any schema changes is detected during decoding SSTs to DTFiles, we need to cancel and recreate DTFiles with
        // the latest schema. Or we will get trouble in `BoundedSSTFilesToBlockInputStream`.
        try
        {
            // Get storage schema atomically, will do schema sync if the storage does not exists.
            // Will return the storage even if it is tombstone.
            const auto [table_drop_lock, storage, schema_snap] = AtomicGetStorageSchema(new_region, tmt);
            if (unlikely(storage == nullptr))
            {
                // The storage must be physically dropped, throw exception and do cleanup.
                throw Exception("", ErrorCodes::TABLE_IS_DROPPED);
            }

            // Get a gc safe point for compacting
            Timestamp gc_safepoint = 0;
            if (auto pd_client = tmt.getPDClient(); !pd_client->isMock())
            {
                gc_safepoint = PDClientHelper::getGCSafePointWithRetry(
                    pd_client,
                    keyspace_id,
                    /* ignore_cache= */ false,
                    context.getSettingsRef().safe_point_update_interval_seconds);
            }
            physical_table_id = storage->getTableInfo().id;

            auto opt = DM::SSTFilesToBlockInputStreamOpts{
                .log_prefix = fmt::format("keyspace={} table_id={}", keyspace_id, physical_table_id),
                .schema_snap = schema_snap,
                .gc_safepoint = gc_safepoint,
                .force_decode = force_decode,
                .expected_size = expected_block_size};

            auto sst_stream = std::make_shared<DM::SSTFilesToBlockInputStream>(
                new_region,
                index,
                snaps,
                proxy_helper,
                tmt,
                DM::SSTFilesToBlockInputStreamOpts(opt));

            ReadFromStreamResult result;
            std::tie(result, prehandle_result)
                = executeTransform(new_region, prehandle_task, job_type, storage, sst_stream, opt, tmt);

            if (result.error == ReadFromStreamError::ErrUpdateSchema)
            {
                // The schema of decoding region data has been updated, need to clear and recreate another stream for writing DTFile(s)
                new_region->clearAllData();

                if (force_decode)
                {
                    // Can not decode data with `force_decode == true`, must be something wrong
                    throw Exception(result.extra_msg, ErrorCodes::REGION_DATA_SCHEMA_UPDATED);
                }

                // Update schema and try to decode again
                LOG_INFO(
                    log,
                    "Decoding Region snapshot data meet error, sync schema and try to decode again {} [error={}]",
                    new_region->toString(true),
                    result.extra_msg);
                GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Increment();
                tmt.getSchemaSyncerManager()->syncTableSchema(context, keyspace_id, physical_table_id);
                // Next time should force_decode
                force_decode = true;

                continue;
            }
            else if (result.error == ReadFromStreamError::ErrTableDropped)
            {
                // We can ignore if storage is dropped.
                LOG_INFO(
                    log,
                    "Pre-handle snapshot to DTFiles is ignored because the table is dropped {}",
                    new_region->toString(true));
                break;
            }
            else if (result.error == ReadFromStreamError::Aborted)
            {
                LOG_INFO(
                    log,
                    "Apply snapshot is aborted, cancelling. region_id={} term={} index={}",
                    region_id,
                    term,
                    index);
            }

            (void)table_drop_lock; // the table should not be dropped during ingesting file
            break;
        }
        catch (DB::Exception & e)
        {
            if (e.code() == ErrorCodes::TABLE_IS_DROPPED)
            {
                // It will be thrown in many places that will lock a table.
                // We can ignore if storage is dropped.
                LOG_INFO(
                    log,
                    "Pre-handle snapshot to DTFiles is ignored because the table is dropped {}",
                    new_region->toString(true));
                break;
            }
            else
            {
                // Other unrecoverable error, throw
                e.addMessage(fmt::format("keyspace={} physical_table_id={}", keyspace_id, physical_table_id));
                throw;
            }
        }
    } // while

    return prehandle_result;
}

void KVStore::abortPreHandleSnapshot(UInt64 region_id, TMTContext & tmt)
{
    UNUSED(tmt);
    auto cancel_flag = prehandling_trace.deregisterTask(region_id);
    if (cancel_flag)
    {
        // The task is registered, set the cancel flag to true and the generated files
        // will be clear later by `releasePreHandleSnapshot`
        LOG_INFO(log, "Try cancel pre-handling from upper layer, region_id={}", region_id);
        cancel_flag->store(true, std::memory_order_seq_cst);
    }
    else
    {
        // the task is not registered, continue
        LOG_INFO(log, "Start cancel pre-handling from upper layer, region_id={}", region_id);
    }
}

template <>
void KVStore::releasePreHandledSnapshot<RegionPtrWithSnapshotFiles>(
    const RegionPtrWithSnapshotFiles & s,
    TMTContext & tmt)
{
    auto & storages = tmt.getStorages();
    auto keyspace_id = s.base->getKeyspaceID();
    auto table_id = s.base->getMappedTableID();
    auto storage = storages.get(keyspace_id, table_id);
    if (storage->engineType() != TiDB::StorageEngine::DT)
    {
        return;
    }
    auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
    LOG_INFO(
        log,
        "Release prehandled snapshot, clean {} dmfiles, region_id={} keyspace={} table_id={}",
        s.external_files.size(),
        s.base->id(),
        keyspace_id,
        table_id);
    auto & context = tmt.getContext();
    dm_storage->cleanPreIngestFiles(s.external_files, context.getSettingsRef());
}

void Region::beforePrehandleSnapshot(uint64_t region_id, std::optional<uint64_t> deadline_index)
{
    if (getClusterRaftstoreVer() == RaftstoreVer::V2)
    {
        data.orphan_keys_info.snapshot_index = appliedIndex();
        data.orphan_keys_info.pre_handling = true;
        data.orphan_keys_info.deadline_index = deadline_index;
        data.orphan_keys_info.region_id = region_id;
    }
}

void Region::afterPrehandleSnapshot()
{
    if (getClusterRaftstoreVer() == RaftstoreVer::V2)
    {
        data.orphan_keys_info.pre_handling = false;
        LOG_INFO(
            log,
            "After prehandle, remains orphan keys {} removed orphan keys {} [region_id={}]",
            data.orphan_keys_info.remainedKeyCount(),
            data.orphan_keys_info.removed_remained_keys.size(),
            id());
    }
}

} // namespace DB