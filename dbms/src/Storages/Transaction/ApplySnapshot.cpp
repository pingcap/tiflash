#include <Common/TiFlashMetrics.h>
#include <Core/TMTPKType.h>
#include <Interpreters/Context.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/CHTableHandle.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/PDTiKVClient.h>
#include <Storages/Transaction/ProxyFFIType.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionDataMover.h>
#include <Storages/Transaction/TMTContext.h>

#include <ext/scope_guard.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

void KVStore::tryApplySnapshot(RegionPtr new_region, Context & context)
{
    auto & tmt = context.getTMTContext();

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
            region_persister.persist(*old_region, region_lock);
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
        Timestamp safe_point = PDClientHelper::getGCSafePointWithRetry(tmt.getPDClient(), /* ignore_cache= */ true);

        // Traverse all table in ch and update handle_maps.
        auto table_id = new_region->getMappedTableID();
        if (auto storage = tmt.getStorages().get(table_id); storage)
        {
            switch (storage->engineType())
            {
                case TiDB::StorageEngine::TMT:
                {
                    if (storage->getTableInfo().is_common_handle)
                        throw Exception("TMT table does not support clustered index", ErrorCodes::NOT_IMPLEMENTED);
                    HandleMap handle_map;
                    const auto handle_range = getHandleRangeByTable(new_region->getRange()->rawKeys(), table_id);

                    auto table_lock = storage->lockStructure(false, __PRETTY_FUNCTION__);

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

                    new_region->compareAndCompleteSnapshot(handle_map, safe_point);
                    break;
                }
                case TiDB::StorageEngine::DT:
                {
                    // acquire lock so that no other threads can change storage's structure
                    auto table_lock = storage->lockStructure(true, __PRETTY_FUNCTION__);
                    // In StorageDeltaMerge, we use deleteRange to remove old data
                    auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
                    dm_storage->deleteRange(DM::RowKeyRange::fromRegionRange(new_region->getRange(), table_id, storage->isCommonHandle(),
                                                storage->getRowKeyColumnSize()),
                        context.getSettingsRef());
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

RegionPtr GenRegionPtr(metapb::Region && region, UInt64 peer_id, UInt64 index, UInt64 term, TMTContext & tmt)
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
    IndexReaderCreateFunc index_reader_create = [&]() -> IndexReaderPtr { return tmt.createIndexReader(); };
    return std::make_shared<Region>(std::move(meta), index_reader_create);
}

void KVStore::preHandleTiKVSnapshot(RegionPtr new_region, const SnapshotViewArray snaps, TMTContext & tmt)
{
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
    SCOPE_EXIT({
        GET_METRIC(ctx.getTiFlashMetrics(), tiflash_raft_command_duration_seconds, type_apply_snapshot_predecode)
            .Observe(watch.elapsedSeconds());
    });

    {
        std::stringstream ss;
        ss << "Pre-handle tikv snapshot " << new_region->toString(false);
        if (snaps.len)
            ss << " with data ";
        for (UInt64 i = 0; i < snaps.len; ++i)
        {
            auto & snapshot = snaps.views[i];
            for (UInt64 n = 0; n < snapshot.len; ++n)
            {
                auto & k = snapshot.keys[n];
                auto & v = snapshot.vals[n];
                new_region->insert(snapshot.cf, TiKVKey(k.data, k.len), TiKVValue(v.data, v.len));
            }

            ss << "[cf: " << CFToName(snapshot.cf) << ", kv size: " << snapshot.len << "],";
            // Note that number of keys in different cf will be aggregated into one metrics
            GET_METRIC(ctx.getTiFlashMetrics(), tiflash_raft_process_keys, type_apply_snapshot).Increment(snapshot.len);
        }
        new_region->tryPreDecodeTiKVValue(tmt);
        ss << " cost " << watch.elapsedMilliseconds() << "ms";
        LOG_INFO(log, ss.str());
    }
}

void KVStore::handleApplySnapshot(RegionPtr new_region, TMTContext & tmt)
{
    LOG_INFO(log, "Try to apply snapshot: " << new_region->toString(true));

    Stopwatch watch;
    SCOPE_EXIT({
        auto & ctx = tmt.getContext();
        GET_METRIC(ctx.getTiFlashMetrics(), tiflash_raft_command_duration_seconds, type_apply_snapshot_flush)
            .Observe(watch.elapsedSeconds());
    });

    tryApplySnapshot(new_region, tmt.getContext());

    LOG_INFO(log, new_region->toString(false) << " apply snapshot success");
}

void KVStore::handleApplySnapshot(
    metapb::Region && region, UInt64 peer_id, const SnapshotViewArray snaps, UInt64 index, UInt64 term, TMTContext & tmt)
{
    auto new_region = GenRegionPtr(std::move(region), peer_id, index, term, tmt);
    preHandleTiKVSnapshot(new_region, snaps, tmt);
    handleApplySnapshot(new_region, tmt);
}

TiFlashApplyRes KVStore::handleIngestSST(UInt64 region_id, const SnapshotViewArray snaps, UInt64 index, UInt64 term, TMTContext & tmt)
{
    auto region_task_lock = region_manager.genRegionTaskLock(region_id);

    Stopwatch watch;
    auto & ctx = tmt.getContext();
    SCOPE_EXIT(
        { GET_METRIC(ctx.getTiFlashMetrics(), tiflash_raft_command_duration_seconds, type_ingest_sst).Observe(watch.elapsedSeconds()); });

    const RegionPtr region = getRegion(region_id);
    if (region == nullptr)
    {
        LOG_WARNING(log, __PRETTY_FUNCTION__ << ": [region " << region_id << "] is not found, might be removed already");
        return TiFlashApplyRes::NotFound;
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
    region->tryPreDecodeTiKVValue(tmt);
    func_try_flush();

    if (region->dataSize())
    {
        LOG_INFO(log, __FUNCTION__ << ": " << region->toString(true) << " with data " << region->dataInfo() << " skip persist");
        return TiFlashApplyRes::None;
    }
    else
    {
        LOG_INFO(log, __FUNCTION__ << ": try to persist " << region->toString(true));
        region_persister.persist(*region, region_task_lock);
        return TiFlashApplyRes::Persist;
    }
}

bool KVStore::preGenTiFlashSnapshot(UInt64 region_id, UInt64 snap_index, TMTContext & tmt)
{
    auto region_task_lock = region_manager.genRegionTaskLock(region_id);

    const RegionPtr region = getRegion(region_id);

    if (region == nullptr)
    {
        LOG_WARNING(log, __PRETTY_FUNCTION__ << ": [region " << region_id << "] is not found, should not generate snapshot");
        return false;
    }

    if (auto index = region->appliedIndex(); index != snap_index)
    {
        LOG_WARNING(log, __PRETTY_FUNCTION__ << ": expected apply index " << snap_index << " but got " << index);
        return false;
    }

    tmt.getRegionTable().tryFlushRegion(region, false);
    tryFlushRegionCacheInStorage(tmt, *region, log);

    LOG_INFO(log, __FUNCTION__ << ": try to persist " << region->toString(true));
    region_persister.persist(*region, region_task_lock);
    return true;
}

} // namespace DB
