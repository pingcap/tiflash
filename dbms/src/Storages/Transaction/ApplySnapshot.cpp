#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>
#include <Core/TMTPKType.h>
#include <Interpreters/Context.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/CHTableHandle.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/PDTiKVClient.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionDataMover.h>
#include <Storages/Transaction/SSTReader.h>
#include <Storages/Transaction/TMTContext.h>

#include <ext/scope_guard.h>

namespace DB
{

namespace FailPoints
{
extern const char pause_until_apply_raft_snapshot[];
} // namespace FailPoints

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int TABLE_IS_DROPPED;
extern const int REGION_DATA_SCHEMA_UPDATED;
} // namespace ErrorCodes

void KVStore::checkAndApplySnapshot(const RegionPtrWithBlock & new_region, TMTContext & tmt)
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
            throw Exception(std::string(__PRETTY_FUNCTION__) + ": region " + std::to_string(region_id) + " already has newer index "
                    + std::to_string(old_applied_index) + ", should not happen",
                ErrorCodes::LOGICAL_ERROR);
        }
        else if (old_applied_index == new_index)
        {
            LOG_WARNING(log,
                old_region->toString(false) << " already has same applied index, just ignore next process. "
                                            << "Please check log whether server crashed after successfully applied snapshot.");
            return;
        }

        {
            LOG_INFO(log, old_region->toString() << " set state to Applying");
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
        handleRegionsByRangeOverlap(new_range->comparableKeys(), [&](RegionMap region_map, const KVStoreTaskLock &) {
            for (const auto & region : region_map)
            {
                if (region.first != region_id)
                {
                    throw Exception(std::string(__PRETTY_FUNCTION__) + ": range of region " + std::to_string(region_id)
                            + " is overlapped with region " + std::to_string(region.first) + ", should not happen",
                        ErrorCodes::LOGICAL_ERROR);
                }
            }
        });
    }

    {
        auto table_id = new_region->getMappedTableID();
        if (auto storage = tmt.getStorages().get(table_id); storage)
        {
            switch (storage->engineType())
            {
                case TiDB::StorageEngine::TMT:
                {
                    auto & context = tmt.getContext();
                    // Traverse all table in storage and update `new_region`.
                    if (storage->getTableInfo().is_common_handle)
                        throw Exception("TMT table does not support clustered index", ErrorCodes::NOT_IMPLEMENTED);
                    HandleMap handle_map;
                    const auto handle_range = getHandleRangeByTable(new_region->getRange()->rawKeys(), table_id);

                    auto table_lock = storage->lockStructureForShare(getThreadName());

                    auto tmt_storage = std::dynamic_pointer_cast<StorageMergeTree>(storage);
                    const bool pk_is_uint64 = getTMTPKType(*tmt_storage->getData().primary_key_data_types[0]) == TMTPKType::UINT64;

                    if (pk_is_uint64)
                    {
                        const auto [n, new_range] = CHTableHandle::splitForUInt64TableHandle(handle_range);
                        getHandleMapByRange<UInt64>(context, *tmt_storage, new_range[0], handle_map);
                        if (n > 1)
                            getHandleMapByRange<UInt64>(context, *tmt_storage, new_range[1], handle_map);
                    }
                    else
                        getHandleMapByRange<Int64>(context, *tmt_storage, handle_range, handle_map);

                    Timestamp safe_point = PDClientHelper::getGCSafePointWithRetry(tmt.getPDClient(), /* ignore_cache= */ true);
                    new_region->compareAndCompleteSnapshot(handle_map, safe_point);
                    break;
                }
                case TiDB::StorageEngine::DT:
                {
                    break;
                }
                default:
                    throw Exception(
                        "Unknown StorageEngine: " + toString(static_cast<Int32>(storage->engineType())), ErrorCodes::LOGICAL_ERROR);
            }
        }
    }

    onSnapshot(new_region, old_region, old_applied_index, tmt);
}

void KVStore::onSnapshot(const RegionPtrWithBlock & new_region_wrap, RegionPtr old_region, UInt64 old_region_index, TMTContext & tmt)
{
    RegionID region_id = new_region_wrap->id();

    {
        auto table_id = new_region_wrap->getMappedTableID();
        if (auto storage = tmt.getStorages().get(table_id); storage)
        {
            switch (storage->engineType())
            {
                case TiDB::StorageEngine::DT:
                {
                    try
                    {
                        auto & context = tmt.getContext();
                        // Acquire `drop_lock` so that no other threads can drop the storage. `alter_lock` is not required.
                        auto table_lock = storage->lockForShare(getThreadName());
                        auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
                        auto key_range = DM::RowKeyRange::fromRegionRange(
                            new_region_wrap->getRange(), table_id, storage->isCommonHandle(), storage->getRowKeyColumnSize());
                        // Call `deleteRange` to delete data for range
                        dm_storage->deleteRange(key_range, context.getSettingsRef());
                    }
                    catch (DB::Exception & e)
                    {
                        // We can ignore if storage is dropped.
                        if (e.code() != ErrorCodes::TABLE_IS_DROPPED)
                            throw;
                    }
                    break;
                }
                default:
                    break;
            }
        }
    }

    {
        const auto range = new_region_wrap->getRange();
        auto & region_table = tmt.getRegionTable();
        // extend region to make sure data won't be removed.
        region_table.extendRegionRange(region_id, *range);
        // try to flush data into ch first.
        try
        {
            auto tmp = region_table.tryFlushRegion(new_region_wrap, false);
            {
                std::lock_guard<std::mutex> lock(bg_gc_region_data_mutex);
                bg_gc_region_data.push_back(std::move(tmp));
            }
            tryFlushRegionCacheInStorage(tmt, *new_region_wrap, log);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    RegionPtr new_region = new_region_wrap.base;
    {
        auto task_lock = genTaskLock();
        auto region_lock = region_manager.genRegionTaskLock(region_id);

        if (getRegion(region_id) != old_region || (old_region && old_region_index != old_region->appliedIndex()))
        {
            throw Exception(
                std::string(__PRETTY_FUNCTION__) + ": region " + std::to_string(region_id) + " instance changed, should not happen",
                ErrorCodes::LOGICAL_ERROR);
        }

        if (old_region != nullptr)
        {
            LOG_DEBUG(log, __FUNCTION__ << ": previous " << old_region->toString(true) << " ; new " << new_region->toString(true));
            region_range_index.remove(old_region->makeRaftCommandDelegate(task_lock).getRange().comparableKeys(), region_id);
            old_region->assignRegion(std::move(*new_region));
            new_region = old_region;
        }
        else
        {
            auto manage_lock = genRegionManageLock();
            regionsMut().emplace(region_id, new_region);
        }

        persistRegion(*new_region, region_lock, "save current region after apply");
        region_range_index.add(new_region);

        tmt.getRegionTable().shrinkRegionRange(*new_region);
    }
}


extern RegionPtrWithBlock::CachePtr GenRegionPreDecodeBlockData(const RegionPtr &, Context &);

RegionPreDecodeBlockDataPtr KVStore::preHandleSnapshot(RegionPtr new_region, const SSTViewVec snaps, TMTContext & tmt)
{
    RegionPreDecodeBlockDataPtr cache{nullptr};
    {
        decltype(bg_gc_region_data)::value_type tmp;
        std::lock_guard<std::mutex> lock(bg_gc_region_data_mutex);
        if (!bg_gc_region_data.empty())
        {
            tmp.swap(bg_gc_region_data.back());
            bg_gc_region_data.pop_back();
        }
    }

    Stopwatch watch;
    auto & ctx = tmt.getContext();
    auto metrics = ctx.getTiFlashMetrics();
    SCOPE_EXIT(
        { GET_METRIC(metrics, tiflash_raft_command_duration_seconds, type_apply_snapshot_predecode).Observe(watch.elapsedSeconds()); });

    {
        LOG_INFO(log, "Pre-handle snapshot " << new_region->toString(false) << " with " << snaps.len << " TiKV sst files");
        // Iterator over all SST files and insert key-values into `new_region`
        for (UInt64 i = 0; i < snaps.len; ++i)
        {
            auto & snapshot = snaps.views[i];
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

            LOG_INFO(log,
                "Decode " << std::string_view(snapshot.path.data, snapshot.path.len) << " got [cf: " << CFToName(snapshot.type)
                          << ", kv size: " << kv_size << "]");
            // Note that number of keys in different cf will be aggregated into one metrics
            GET_METRIC(metrics, tiflash_raft_process_keys, type_apply_snapshot).Increment(kv_size);
        }
        {
            LOG_INFO(log, "Start to pre-decode " << new_region->toString() << " into block");
            auto block_cache = GenRegionPreDecodeBlockData(new_region, ctx);
            if (block_cache)
                LOG_INFO(log, "Got pre-decode block cache"; block_cache->toString(oss_internal_rare));
            else
                LOG_INFO(log, "Got empty pre-decode block cache");

            cache = std::move(block_cache);
        }
        LOG_INFO(log, "Pre-handle snapshot " << new_region->toString(false) << " cost " << watch.elapsedMilliseconds() << "ms");
    }

    return cache;
}

void KVStore::handlePreApplySnapshot(const RegionPtrWithBlock & new_region, TMTContext & tmt)
{
    LOG_INFO(log, "Try to apply snapshot: " << new_region->toString(true));

    Stopwatch watch;
    SCOPE_EXIT({
        auto & ctx = tmt.getContext();
        GET_METRIC(ctx.getTiFlashMetrics(), tiflash_raft_command_duration_seconds, type_apply_snapshot_flush)
            .Observe(watch.elapsedSeconds());
    });

    checkAndApplySnapshot(new_region, tmt);

    FAIL_POINT_PAUSE(FailPoints::pause_until_apply_raft_snapshot);

    LOG_INFO(log, new_region->toString(false) << " apply snapshot success");
}

static const metapb::Peer & findPeer(const metapb::Region & region, UInt64 peer_id)
{
    for (const auto & peer : region.peers())
    {
        if (peer.id() == peer_id)
        {
            return peer;
        }
    }

    throw Exception(std::string(__PRETTY_FUNCTION__) + ": peer " + DB::toString(peer_id) + " not found", ErrorCodes::LOGICAL_ERROR);
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
    metapb::Region && region, UInt64 peer_id, const SSTViewVec snaps, UInt64 index, UInt64 term, TMTContext & tmt)
{
    auto new_region = genRegionPtr(std::move(region), peer_id, index, term);
    handlePreApplySnapshot(RegionPtrWithBlock{new_region, preHandleSnapshot(new_region, snaps, tmt)}, tmt);
}

EngineStoreApplyRes KVStore::handleIngestSST(UInt64 region_id, const SSTViewVec snaps, UInt64 index, UInt64 term, TMTContext & tmt)
{
    auto region_task_lock = region_manager.genRegionTaskLock(region_id);

    Stopwatch watch;
    auto & ctx = tmt.getContext();
    SCOPE_EXIT(
        { GET_METRIC(ctx.getTiFlashMetrics(), tiflash_raft_command_duration_seconds, type_ingest_sst).Observe(watch.elapsedSeconds()); });

    const RegionPtr region = getRegion(region_id);
    if (region == nullptr)
    {
        LOG_WARNING(log,
            __PRETTY_FUNCTION__ << ": [region " << region_id << "] is not found at [term " << term << ", index " << index
                                << "], might be removed already");
        return EngineStoreApplyRes::NotFound;
    }

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
            LOG_DEBUG(log, "catch but ignore exception: " << e.message());
        }
    };

    // try to flush remain data in memory.
    func_try_flush();
    region->handleIngestSST(snaps, index, term, tmt);
    func_try_flush();

    if (region->dataSize())
    {
        LOG_INFO(log, __FUNCTION__ << ": " << region->toString(true) << " with data " << region->dataInfo() << " skip persist");
        return EngineStoreApplyRes::None;
    }
    else
    {
        persistRegion(*region, region_task_lock, __FUNCTION__);
        return EngineStoreApplyRes::Persist;
    }
}

} // namespace DB
