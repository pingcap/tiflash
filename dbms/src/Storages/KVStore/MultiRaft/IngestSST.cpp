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
#include <Common/setThreadName.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/Decode/SSTFilesToDTFilesOutputStream.h>
#include <Storages/KVStore/Decode/RegionTable.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/FFI/SSTReader.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>

namespace DB
{
namespace ErrorCodes
{
extern const int TABLE_IS_DROPPED;
} // namespace ErrorCodes

EngineStoreApplyRes KVStore::handleIngestSST(
    UInt64 region_id,
    const SSTViewVec snaps,
    UInt64 index,
    UInt64 term,
    TMTContext & tmt)
{
    auto region_task_lock = region_manager.genRegionTaskLock(region_id);

    Stopwatch watch;
    SCOPE_EXIT({ GET_METRIC(tiflash_raft_command_duration_seconds, type_ingest_sst).Observe(watch.elapsedSeconds()); });

    const RegionPtr region = getRegion(region_id);
    if (region == nullptr)
    {
        LOG_WARNING(
            log,
            "region not found, might be removed already, region_id={} term={} index={}",
            region_id,
            term,
            index);
        return EngineStoreApplyRes::NotFound;
    }

    const auto func_try_flush = [&]() {
        if (region->writeCFCount() == 0)
            return;
        try
        {
            tmt.getRegionTable().tryWriteBlockByRegion(region);
            tryFlushRegionCacheInStorage(tmt, *region, log);
        }
        catch (Exception & e)
        {
            // sst of write cf may be ingested first, exception may be raised because there is no matched data in default cf.
            // ignore it.
            LOG_DEBUG(log, "catch but ignore exception: {}", e.message());
        }
    };

    {
        // try to flush remain data in memory.
        func_try_flush();
        auto tmp_region = handleIngestSSTByDTFile(region, snaps, index, term, tmt);
        // Merge data from tmp_region.
        region->finishIngestSSTByDTFile(std::move(tmp_region), index, term);
        // after `finishIngestSSTByDTFile`, try to flush committed data into storage
        func_try_flush();
    }

    if (region->dataSize())
    {
        LOG_INFO(log, "{} with data {}, skip persist", region->toString(true), region->dataInfo());
        return EngineStoreApplyRes::None;
    }
    else
    {
        // We always try to flush dm cache and region if possible for every IngestSST,
        // in order to have the raft log truncated and sst deleted.
        persistRegion(*region, region_task_lock, PersistRegionReason::IngestSst, "");
        return EngineStoreApplyRes::Persist;
    }
}

RegionPtr KVStore::handleIngestSSTByDTFile(
    const RegionPtr & region,
    const SSTViewVec snaps,
    UInt64 index,
    UInt64 term,
    TMTContext & tmt)
{
    if (index <= region->appliedIndex())
        return nullptr;

    // Create a tmp region to store uncommitted data
    RegionPtr tmp_region;
    {
        auto meta_region = region->cloneMetaRegion();
        auto meta_snap = region->dumpRegionMetaSnapshot();
        auto peer_id = meta_snap.peer.id();
        tmp_region = genRegionPtr(std::move(meta_region), peer_id, index, term);
    }

    // Decode the KV pairs in ingesting SST into DTFiles
    PrehandleResult prehandle_result;
    try
    {
        prehandle_result
            = preHandleSSTsToDTFiles(tmp_region, snaps, index, term, DM::FileConvertJobType::IngestSST, tmt);
    }
    catch (DB::Exception & e)
    {
        e.addMessage(
            fmt::format("(while handleIngestSST region_id={} index={} term={})", tmp_region->id(), index, term));
        e.rethrow();
    }

    // If `external_files` is empty, ingest SST won't write delete_range for ingest region, it is safe to
    // ignore the step of calling `ingestFiles`
    if (!prehandle_result.ingest_ids.empty())
    {
        auto keyspace_id = region->getKeyspaceID();
        auto table_id = region->getMappedTableID();
        if (auto storage = tmt.getStorages().get(keyspace_id, table_id); storage)
        {
            // Ingest DTFiles into DeltaMerge storage
            auto & context = tmt.getContext();
            try
            {
                // Acquire `drop_lock` so that no other threads can drop the storage. `alter_lock` is not required.
                auto table_lock = storage->lockForShare(getThreadNameAndID());
                auto key_range = DM::RowKeyRange::fromRegionRange(
                    region->getRange(),
                    table_id,
                    storage->isCommonHandle(),
                    storage->getRowKeyColumnSize());
                // Call `ingestFiles` to ingest external DTFiles.
                // Note that ingest sst won't remove the data in the key range
                auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
                dm_storage->ingestFiles( //
                    key_range,
                    prehandle_result.ingest_ids,
                    /*clear_data_in_range=*/false,
                    context.getSettingsRef());
            }
            catch (DB::Exception & e)
            {
                // We can ignore if storage is dropped.
                if (e.code() == ErrorCodes::TABLE_IS_DROPPED)
                    return nullptr;
                else
                    throw;
            }
        }
    }

    return tmp_region;
}

void Region::finishIngestSSTByDTFile(RegionPtr && temp_region, UInt64 index, UInt64 term)
{
    if (index <= appliedIndex())
        return;

    {
        std::unique_lock<std::shared_mutex> lock(mutex);

        auto uncommitted_ingest = temp_region->dataSize();
        GET_METRIC(tiflash_raft_write_flow_bytes, type_ingest_uncommitted).Observe(uncommitted_ingest);
        if (temp_region)
        {
            // Merge the uncommitted data from `temp_region`.
            // As we have taken the ownership of `temp_region`, so don't need to acquire lock on `temp_region.mutex`
            data.mergeFrom(temp_region->data);
        }

        meta.setApplied(index, term);
    }
    LOG_INFO(
        log,
        "{} finish ingest sst by DTFile, write_cf_keys={} default_cf_keys={} lock_cf_keys={}",
        this->toString(false),
        data.write_cf.getSize(),
        data.default_cf.getSize(),
        data.lock_cf.getSize());
    meta.notifyAll();
}
} // namespace DB
