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
#include <Storages/DeltaMerge/SSTFilesToBlockInputStream.h>
#include <Storages/DeltaMerge/SSTFilesToDTFilesOutputStream.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>
#include <Storages/Transaction/CHTableHandle.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/PDTiKVClient.h>
#include <Storages/Transaction/PartitionStreams.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/SSTReader.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/Types.h>

#include <ext/scope_guard.h>

namespace DB
{
namespace FailPoints
{
extern const char force_set_sst_to_dtfile_block_size[];
extern const char force_set_sst_decode_rand[];
extern const char pause_until_apply_raft_snapshot[];
} // namespace FailPoints

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int TABLE_IS_DROPPED;
extern const int REGION_DATA_SCHEMA_UPDATED;
} // namespace ErrorCodes

template <typename RegionPtrWrap>
void KVStore::checkAndApplySnapshot(const RegionPtrWrap & new_region, TMTContext & tmt)
{
    auto region_id = new_region->id();
    auto old_region = getRegion(region_id);
    UInt64 old_applied_index = 0;

    /**
     * When applying snapshot of a region, its range must not be overlapped with any other(different id) region's.
     */
    if (old_region)
    {
        old_applied_index = old_region->appliedIndex();
        if (auto new_index = new_region->appliedIndex(); old_applied_index > new_index)
        {
            auto s = fmt::format("[region {}] already has newer apply-index {} than {}, should not happen",
                                 region_id,
                                 old_applied_index,
                                 new_index);
            throw Exception(s, ErrorCodes::LOGICAL_ERROR);
        }
        else if (old_applied_index == new_index)
        {
            LOG_FMT_WARNING(log,
                            "{} already has same applied index, just ignore next process. Please check log whether server crashed after successfully applied snapshot.",
                            old_region->getDebugString());
            return;
        }

        {
            LOG_FMT_INFO(log, "{} set state to `Applying`", old_region->toString());
            // Set original region state to `Applying` and any read request toward this region should be rejected because
            // engine may delete data unsafely.
            auto region_lock = region_manager.genRegionTaskLock(old_region->id());
            old_region->setStateApplying();
            tmt.getRegionTable().tryFlushRegion(old_region, false);
            tryFlushRegionCacheInStorage(tmt, *old_region, log);
            persistRegion(*old_region, region_lock, "save previous region before apply");
        }
    }

    {
        const auto & new_range = new_region->getRange();
        auto task_lock = genTaskLock();
        auto region_map = getRegionsByRangeOverlap(new_range->comparableKeys());
        for (const auto & overlapped_region : region_map)
        {
            if (overlapped_region.first != region_id)
            {
                auto state = getProxyHelper()->getRegionLocalState(overlapped_region.first);
                if (state.state() != raft_serverpb::PeerState::Tombstone)
                {
                    throw Exception(fmt::format(
                                        "range of region {} is overlapped with {}, state: {}",
                                        region_id,
                                        overlapped_region.first,
                                        state.ShortDebugString()),
                                    ErrorCodes::LOGICAL_ERROR);
                }
                else
                {
                    LOG_FMT_INFO(log, "range of region {} is overlapped with `Tombstone` region {}", region_id, overlapped_region.first);
                    handleDestroy(overlapped_region.first, tmt, task_lock);
                }
            }
        }
    }

    {
        auto table_id = new_region->getMappedTableID();
        if (auto storage = tmt.getStorages().get(table_id); storage)
        {
            switch (storage->engineType())
            {
            case TiDB::StorageEngine::DT:
            {
                break;
            }
            default:
                throw Exception(
                    "Unknown StorageEngine: " + toString(static_cast<Int32>(storage->engineType())),
                    ErrorCodes::LOGICAL_ERROR);
            }
        }
    }

    onSnapshot(new_region, old_region, old_applied_index, tmt);
}

template <typename RegionPtrWrap>
void KVStore::onSnapshot(const RegionPtrWrap & new_region_wrap, RegionPtr old_region, UInt64 old_region_index, TMTContext & tmt)
{
    RegionID region_id = new_region_wrap->id();

    {
        auto table_id = new_region_wrap->getMappedTableID();
        if (auto storage = tmt.getStorages().get(table_id); storage && storage->engineType() == TiDB::StorageEngine::DT)
        {
            try
            {
                auto & context = tmt.getContext();
                // Acquire `drop_lock` so that no other threads can drop the storage. `alter_lock` is not required.
                auto table_lock = storage->lockForShare(getThreadName());
                auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
                auto new_key_range = DM::RowKeyRange::fromRegionRange(
                    new_region_wrap->getRange(),
                    table_id,
                    storage->isCommonHandle(),
                    storage->getRowKeyColumnSize());
                if (old_region)
                {
                    auto old_key_range = DM::RowKeyRange::fromRegionRange(
                        old_region->getRange(),
                        table_id,
                        storage->isCommonHandle(),
                        storage->getRowKeyColumnSize());
                    if (old_key_range != new_key_range)
                    {
                        LOG_FMT_INFO(log, "clear region {} old range {} before apply snapshot of new range {}", region_id, old_key_range.toDebugString(), new_key_range.toDebugString());
                        dm_storage->deleteRange(old_key_range, context.getSettingsRef());
                    }
                }
                if constexpr (std::is_same_v<RegionPtrWrap, RegionPtrWithSnapshotFiles>)
                {
                    // Call `ingestFiles` to delete data for range and ingest external DTFiles.
                    dm_storage->ingestFiles(new_key_range, new_region_wrap.ingest_ids, /*clear_data_in_range=*/true, context.getSettingsRef());
                }
                else
                {
                    // Call `deleteRange` to delete data for range
                    dm_storage->deleteRange(new_key_range, context.getSettingsRef());
                }
            }
            catch (DB::Exception & e)
            {
                // We can ignore if storage is dropped.
                if (e.code() != ErrorCodes::TABLE_IS_DROPPED)
                    throw;
            }
        }
    }

    {
        const auto range = new_region_wrap->getRange();
        auto & region_table = tmt.getRegionTable();
        // extend region to make sure data won't be removed.
        region_table.extendRegionRange(region_id, *range);
        // For `RegionPtrWithBlock`, try to flush data into storage first.
        if constexpr (std::is_same_v<RegionPtrWrap, RegionPtrWithBlock>)
        {
            try
            {
                auto tmp = region_table.tryFlushRegion(new_region_wrap, false);
                {
                    std::lock_guard lock(bg_gc_region_data_mutex);
                    bg_gc_region_data.push_back(std::move(tmp));
                }
                tryFlushRegionCacheInStorage(tmt, *new_region_wrap, log);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
        // For `RegionPtrWithSnapshotFiles`, don't need to flush cache.
    }

    RegionPtr new_region = new_region_wrap.base;
    {
        auto task_lock = genTaskLock();
        auto region_lock = region_manager.genRegionTaskLock(region_id);

        if (getRegion(region_id) != old_region || (old_region && old_region_index != old_region->appliedIndex()))
        {
            auto s = fmt::format("{}: [region {}] instance changed, should not happen", __PRETTY_FUNCTION__, region_id);
            throw Exception(s, ErrorCodes::LOGICAL_ERROR);
        }

        if (old_region != nullptr)
        {
            LOG_FMT_DEBUG(log, "previous {}, new {}", old_region->getDebugString(), new_region->getDebugString());
            {
                // remove index first
                const auto & range = old_region->makeRaftCommandDelegate(task_lock).getRange().comparableKeys();
                {
                    auto manage_lock = genRegionWriteLock(task_lock);
                    manage_lock.index.remove(range, region_id);
                }
            }
            old_region->assignRegion(std::move(*new_region));
            new_region = old_region;
            {
                // add index
                auto manage_lock = genRegionWriteLock(task_lock);
                manage_lock.index.add(new_region);
            }
        }
        else
        {
            auto manage_lock = genRegionWriteLock(task_lock);
            manage_lock.regions.emplace(region_id, new_region);
            manage_lock.index.add(new_region);
        }

        persistRegion(*new_region, region_lock, "save current region after apply");

        tmt.getRegionTable().shrinkRegionRange(*new_region);
    }
}

extern RegionPtrWithBlock::CachePtr GenRegionPreDecodeBlockData(const RegionPtr &, Context &);

/// `preHandleSnapshotToBlock` read data from SSTFiles and predoced the data as a block
RegionPreDecodeBlockDataPtr KVStore::preHandleSnapshotToBlock(
    RegionPtr new_region,
    const SSTViewVec snaps,
    uint64_t /*index*/,
    uint64_t /*term*/,
    TMTContext & tmt)
{
    RegionPreDecodeBlockDataPtr cache{nullptr};
    {
        decltype(bg_gc_region_data)::value_type tmp;
        std::lock_guard lock(bg_gc_region_data_mutex);
        if (!bg_gc_region_data.empty())
        {
            tmp.swap(bg_gc_region_data.back());
            bg_gc_region_data.pop_back();
        }
    }

    Stopwatch watch;
    auto & ctx = tmt.getContext();
    SCOPE_EXIT({ GET_METRIC(tiflash_raft_command_duration_seconds, type_apply_snapshot_predecode).Observe(watch.elapsedSeconds()); });

    {
        LOG_FMT_INFO(log, "Pre-handle snapshot {} with {} TiKV sst files", new_region->toString(false), snaps.len);
        // Iterator over all SST files and insert key-values into `new_region`
        for (UInt64 i = 0; i < snaps.len; ++i)
        {
            const auto & snapshot = snaps.views[i];
            auto sst_reader = SSTReader{proxy_helper, snapshot};

            uint64_t kv_size = 0;
            while (sst_reader.remained())
            {
                auto key = sst_reader.key();
                auto value = sst_reader.value();
                new_region->insert(snaps.views[i].type, TiKVKey(key.data, key.len), TiKVValue(value.data, value.len));
                ++kv_size;
                sst_reader.next();
            }

            LOG_FMT_INFO(log,
                         "Decode {} got [cf: {}, kv size: {}]",
                         std::string_view(snapshot.path.data, snapshot.path.len),
                         CFToName(snapshot.type),
                         kv_size);
            // Note that number of keys in different cf will be aggregated into one metrics
            GET_METRIC(tiflash_raft_process_keys, type_apply_snapshot).Increment(kv_size);
        }
        {
            LOG_FMT_INFO(log, "Start to pre-decode {} into block", new_region->toString());
            auto block_cache = GenRegionPreDecodeBlockData(new_region, ctx);
            if (block_cache)
            {
                std::stringstream ss;
                block_cache->toString(ss);
                LOG_FMT_INFO(log, "Got pre-decode block cache {}", ss.str());
            }
            else
                LOG_FMT_INFO(log, "Got empty pre-decode block cache");

            cache = std::move(block_cache);
        }
        LOG_FMT_INFO(log, "Pre-handle snapshot {} cost {}ms", new_region->toString(false), watch.elapsedMilliseconds());
    }

    return cache;
}

std::vector<UInt64> KVStore::preHandleSnapshotToFiles(
    RegionPtr new_region,
    const SSTViewVec snaps,
    uint64_t index,
    uint64_t term,
    TMTContext & tmt)
{
    std::vector<UInt64> ingest_ids;
    try
    {
        ingest_ids = preHandleSSTsToDTFiles(new_region, snaps, index, term, DM::FileConvertJobType::ApplySnapshot, tmt);
    }
    catch (DB::Exception & e)
    {
        e.addMessage(fmt::format("(while preHandleSnapshot region_id={}, index={}, term={})", new_region->id(), index, term));
        e.rethrow();
    }
    return ingest_ids;
}

/// `preHandleSSTsToDTFiles` read data from SSTFiles and generate DTFile(s) for commited data
/// return the ids of DTFile(s), the uncommited data will be inserted to `new_region`
std::vector<UInt64> KVStore::preHandleSSTsToDTFiles(
    RegionPtr new_region,
    const SSTViewVec snaps,
    uint64_t /*index*/,
    uint64_t /*term*/,
    DM::FileConvertJobType job_type,
    TMTContext & tmt)
{
    auto context = tmt.getContext();
    bool force_decode = false;
    size_t expected_block_size = DEFAULT_MERGE_BLOCK_SIZE;

    // Use failpoint to change the expected_block_size for some test cases
    fiu_do_on(FailPoints::force_set_sst_to_dtfile_block_size, { expected_block_size = 3; });

    Stopwatch watch;
    SCOPE_EXIT({ GET_METRIC(tiflash_raft_command_duration_seconds, type_apply_snapshot_predecode).Observe(watch.elapsedSeconds()); });

    PageIds generated_ingest_ids;
    TableID physical_table_id = InvalidTableID;
    while (true)
    {
        // If any schema changes is detected during decoding SSTs to DTFiles, we need to cancel and recreate DTFiles with
        // the latest schema. Or we will get trouble in `BoundedSSTFilesToBlockInputStream`.
        std::shared_ptr<DM::SSTFilesToDTFilesOutputStream> stream;
        try
        {
            // Get storage schema atomically, will do schema sync if the storage does not exists.
            // Will return the storage even if it is tombstoned.
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
                gc_safepoint = PDClientHelper::getGCSafePointWithRetry(pd_client,
                                                                       /* ignore_cache= */ false,
                                                                       context.getSettingsRef().safe_point_update_interval_seconds);
            }
            physical_table_id = storage->getTableInfo().id;

            // Read from SSTs and refine the boundary of blocks output to DTFiles
            auto sst_stream = std::make_shared<DM::SSTFilesToBlockInputStream>(
                new_region,
                snaps,
                proxy_helper,
                schema_snap,
                gc_safepoint,
                force_decode,
                tmt,
                expected_block_size);
            auto bounded_stream = std::make_shared<DM::BoundedSSTFilesToBlockInputStream>(sst_stream, ::DB::TiDBPkColumnID, schema_snap);
            stream = std::make_shared<DM::SSTFilesToDTFilesOutputStream>(
                bounded_stream,
                storage,
                schema_snap,
                snapshot_apply_method,
                job_type,
                tmt);

            stream->writePrefix();
            stream->write();
            stream->writeSuffix();
            generated_ingest_ids = stream->ingestIds();

            (void)table_drop_lock; // the table should not be dropped during ingesting file
            break;
        }
        catch (DB::Exception & e)
        {
            auto try_clean_up = [&stream]() -> void {
                if (stream != nullptr)
                    stream->cancel();
            };
            if (e.code() == ErrorCodes::REGION_DATA_SCHEMA_UPDATED)
            {
                // The schema of decoding region data has been updated, need to clear and recreate another stream for writing DTFile(s)
                new_region->clearAllData();
                try_clean_up();

                if (force_decode)
                {
                    // Can not decode data with `force_decode == true`, must be something wrong
                    throw;
                }

                // Update schema and try to decode again
                LOG_FMT_INFO(log, "Decoding Region snapshot data meet error, sync schema and try to decode again {} [error={}]", new_region->toString(true), e.displayText());
                GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Increment();
                tmt.getSchemaSyncer()->syncSchemas(context);
                // Next time should force_decode
                force_decode = true;

                continue;
            }
            else if (e.code() == ErrorCodes::TABLE_IS_DROPPED)
            {
                // We can ignore if storage is dropped.
                LOG_FMT_INFO(log, "Pre-handle snapshot to DTFiles is ignored because the table is dropped {}", new_region->toString(true));
                try_clean_up();
                break;
            }
            else
            {
                // Other unrecoverable error, throw
                e.addMessage(fmt::format("physical_table_id={}", physical_table_id));
                throw;
            }
        }
    }

    return generated_ingest_ids;
}

template <typename RegionPtrWrap>
void KVStore::handlePreApplySnapshot(const RegionPtrWrap & new_region, TMTContext & tmt)
{
    LOG_FMT_INFO(log, "Try to apply snapshot {}", new_region->toString(true));

    Stopwatch watch;
    SCOPE_EXIT({ GET_METRIC(tiflash_raft_command_duration_seconds, type_apply_snapshot_flush).Observe(watch.elapsedSeconds()); });

    checkAndApplySnapshot(new_region, tmt);

    FAIL_POINT_PAUSE(FailPoints::pause_until_apply_raft_snapshot);

    LOG_FMT_INFO(log, "{} apply snapshot success", new_region->toString(false));
}

template void KVStore::handlePreApplySnapshot<RegionPtrWithBlock>(const RegionPtrWithBlock &, TMTContext &);
template void KVStore::handlePreApplySnapshot<RegionPtrWithSnapshotFiles>(const RegionPtrWithSnapshotFiles &, TMTContext &);
template void KVStore::checkAndApplySnapshot<RegionPtrWithBlock>(const RegionPtrWithBlock &, TMTContext &);
template void KVStore::checkAndApplySnapshot<RegionPtrWithSnapshotFiles>(const RegionPtrWithSnapshotFiles &, TMTContext &);
template void KVStore::onSnapshot<RegionPtrWithBlock>(const RegionPtrWithBlock &, RegionPtr, UInt64, TMTContext &);
template void KVStore::onSnapshot<RegionPtrWithSnapshotFiles>(const RegionPtrWithSnapshotFiles &, RegionPtr, UInt64, TMTContext &);


static const metapb::Peer & findPeer(const metapb::Region & region, UInt64 peer_id)
{
    for (const auto & peer : region.peers())
    {
        if (peer.id() == peer_id)
        {
            return peer;
        }
    }

    throw Exception(
        fmt::format("{}: [peer {}] not found in [region {}]", __PRETTY_FUNCTION__, peer_id, region.id()),
        ErrorCodes::LOGICAL_ERROR);
}

RegionPtr KVStore::genRegionPtr(metapb::Region && region, UInt64 peer_id, UInt64 index, UInt64 term)
{
    auto meta = ({
        auto peer = findPeer(region, peer_id);
        raft_serverpb::RaftApplyState apply_state;
        {
            apply_state.set_applied_index(index);
            apply_state.mutable_truncated_state()->set_index(index);
            apply_state.mutable_truncated_state()->set_term(term);
        }
        RegionMeta(std::move(peer), std::move(region), std::move(apply_state));
    });

    return std::make_shared<Region>(std::move(meta), proxy_helper);
}

void KVStore::handleApplySnapshot(
    metapb::Region && region,
    uint64_t peer_id,
    const SSTViewVec snaps,
    uint64_t index,
    uint64_t term,
    TMTContext & tmt)
{
    auto new_region = genRegionPtr(std::move(region), peer_id, index, term);
    if (snapshot_apply_method == TiDB::SnapshotApplyMethod::Block)
        handlePreApplySnapshot(RegionPtrWithBlock{new_region, preHandleSnapshotToBlock(new_region, snaps, index, term, tmt)}, tmt);
    else
        handlePreApplySnapshot(RegionPtrWithSnapshotFiles{new_region, preHandleSnapshotToFiles(new_region, snaps, index, term, tmt)}, tmt);
}

EngineStoreApplyRes KVStore::handleIngestSST(UInt64 region_id, const SSTViewVec snaps, UInt64 index, UInt64 term, TMTContext & tmt)
{
    auto region_task_lock = region_manager.genRegionTaskLock(region_id);

    Stopwatch watch;
    SCOPE_EXIT({ GET_METRIC(tiflash_raft_command_duration_seconds, type_ingest_sst).Observe(watch.elapsedSeconds()); });

    const RegionPtr region = getRegion(region_id);
    if (region == nullptr)
    {
        LOG_FMT_WARNING(log, "[region {}] is not found at [term {}, index {}], might be removed already", region_id, term, index);
        return EngineStoreApplyRes::NotFound;
    }

    fiu_do_on(FailPoints::force_set_sst_decode_rand, {
        static int num_call = 0;
        switch (num_call++ % 3)
        {
        case 0:
            snapshot_apply_method = TiDB::SnapshotApplyMethod::Block;
            break;
        case 1:
            snapshot_apply_method = TiDB::SnapshotApplyMethod::DTFile_Directory;
            break;
        case 2:
            snapshot_apply_method = TiDB::SnapshotApplyMethod::DTFile_Single;
            break;
        default:
            break;
        }
        LOG_FMT_INFO(log, "{} ingest sst by method {}", region->toString(true), applyMethodToString(snapshot_apply_method));
    });

    const auto func_try_flush = [&]() {
        if (!region->writeCFCount())
            return;
        try
        {
            tmt.getRegionTable().tryFlushRegion(region, false);
            tryFlushRegionCacheInStorage(tmt, *region, log);
        }
        catch (Exception & e)
        {
            // sst of write cf may be ingested first, exception may be raised because there is no matched data in default cf.
            // ignore it.
            LOG_FMT_DEBUG(log, "catch but ignore exception: {}", e.message());
        }
    };

    if (snapshot_apply_method == TiDB::SnapshotApplyMethod::Block)
    {
        // try to flush remain data in memory.
        func_try_flush();
        region->handleIngestSSTInMemory(snaps, index, term);
        // after `handleIngestSSTInMemory`, all data are stored in `region`, try to flush committed data into storage
        func_try_flush();
    }
    else
    {
        // try to flush remain data in memory.
        func_try_flush();
        auto tmp_region = handleIngestSSTByDTFile(region, snaps, index, term, tmt);
        region->finishIngestSSTByDTFile(std::move(tmp_region), index, term);
        // after `finishIngestSSTByDTFile`, try to flush committed data into storage
        func_try_flush();
    }

    if (region->dataSize())
    {
        LOG_FMT_INFO(log, "{} with data {}, skip persist", region->toString(true), region->dataInfo());
        return EngineStoreApplyRes::None;
    }
    else
    {
        persistRegion(*region, region_task_lock, __FUNCTION__);
        return EngineStoreApplyRes::Persist;
    }
}

RegionPtr KVStore::handleIngestSSTByDTFile(const RegionPtr & region, const SSTViewVec snaps, UInt64 index, UInt64 term, TMTContext & tmt)
{
    if (index <= region->appliedIndex())
        return nullptr;

    // Create a tmp region to store uncommitted data
    RegionPtr tmp_region;
    {
        auto meta_region = region->getMetaRegion();
        auto meta_snap = region->dumpRegionMetaSnapshot();
        auto peer_id = meta_snap.peer.id();
        tmp_region = genRegionPtr(std::move(meta_region), peer_id, index, term);
    }

    // Decode the KV pairs in ingesting SST into DTFiles
    PageIds ingest_ids;
    try
    {
        ingest_ids = preHandleSSTsToDTFiles(tmp_region, snaps, index, term, DM::FileConvertJobType::IngestSST, tmt);
    }
    catch (DB::Exception & e)
    {
        e.addMessage(fmt::format("(while handleIngestSST region_id={}, index={}, term={})", tmp_region->id(), index, term));
        e.rethrow();
    }

    // If `ingest_ids` is empty, ingest SST won't write delete_range for ingest region, it is safe to
    // ignore the step of calling `ingestFiles`
    if (!ingest_ids.empty())
    {
        auto table_id = region->getMappedTableID();
        if (auto storage = tmt.getStorages().get(table_id); storage)
        {
            // Ingest DTFiles into DeltaMerge storage
            auto & context = tmt.getContext();
            try
            {
                // Acquire `drop_lock` so that no other threads can drop the storage. `alter_lock` is not required.
                auto table_lock = storage->lockForShare(getThreadName());
                auto key_range = DM::RowKeyRange::fromRegionRange(
                    region->getRange(),
                    table_id,
                    storage->isCommonHandle(),
                    storage->getRowKeyColumnSize());
                // Call `ingestFiles` to ingest external DTFiles.
                // Note that ingest sst won't remove the data in the key range
                auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
                dm_storage->ingestFiles(key_range, ingest_ids, /*clear_data_in_range=*/false, context.getSettingsRef());
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

} // namespace DB
