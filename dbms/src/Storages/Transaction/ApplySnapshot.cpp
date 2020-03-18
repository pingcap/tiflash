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

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

bool KVStore::tryApplySnapshot(RegionPtr new_region, Context & context, bool try_flush_region)
{
    auto & tmt = context.getTMTContext();

    auto old_region = getRegion(new_region->id());
    UInt64 old_applied_index = 0;
    KVStore::RegionsAppliedindexMap regions_to_check;

    if (old_region)
    {
        old_applied_index = old_region->appliedIndex();
        if (old_applied_index >= new_region->appliedIndex())
        {
            LOG_WARNING(log, new_region->toString(false) << " already has newer index " << old_applied_index);
            return false;
        }
    }

    {
        Timestamp safe_point = PDClientHelper::getGCSafePointWithRetry(tmt.getPDClient(), /* ignore_cache= */ true);

        HandleMap handle_map;

        {
            std::stringstream ss;
            // Get all regions whose range overlapped with the one of new_region.
            const auto & new_range = new_region->getRange();

            ss << "New range " << new_range->comparableKeys().first.key.toHex() << "," << new_range->comparableKeys().second.key.toHex()
               << " is overlapped with ";

            handleRegionsByRangeOverlap(new_range->comparableKeys(), [&](RegionMap region_map, const KVStoreTaskLock & task_lock) {
                for (const auto & region : region_map)
                {
                    auto & region_delegate = region.second->makeRaftCommandDelegate(task_lock);
                    regions_to_check.emplace(region.first, std::make_pair(region.second, region_delegate.appliedIndex()));
                    ss << region_delegate.toString(true) << " ";
                }
            });
            if (!regions_to_check.empty())
                LOG_DEBUG(log, ss.str());
            else
                LOG_DEBUG(log, ss.str() << "no region");

            // Get all handle with largest version in those regions.
            for (const auto & region_info : regions_to_check)
                new_region->compareAndUpdateHandleMaps(*region_info.second.first, handle_map);
        }

        // Traverse all table in ch and update handle_maps.
        auto table_id = new_region->getMappedTableID();
        if (auto storage = tmt.getStorages().get(table_id); storage)
        {
            const auto handle_range = new_region->getHandleRangeByTable(table_id);
            switch (storage->engineType())
            {
                case TiDB::StorageEngine::TMT:
                {
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
                    break;
                }
                case TiDB::StorageEngine::DT:
                {
                    // acquire lock so that no other threads can change storage's structure
                    auto table_lock = storage->lockStructure(true, __PRETTY_FUNCTION__);
                    // In StorageDeltaMerge, we use deleteRange to remove old data
                    auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
                    DM::HandleRange dm_handle_range = toDMHandleRange(handle_range);
                    dm_storage->deleteRange(dm_handle_range, context.getSettingsRef());
                    break;
                }
                default:
                    throw Exception(
                        "Unknown StorageEngine: " + toString(static_cast<Int32>(storage->engineType())), ErrorCodes::LOGICAL_ERROR);
            }
        }

        new_region->compareAndCompleteSnapshot(handle_map, safe_point);
    }

    if (old_region)
    {
        auto info = std::make_pair(old_region, old_applied_index);
        auto res = regions_to_check.emplace(old_region->id(), info);
        if (!res.second)
        {
            if (res.first->second != info)
            {
                LOG_WARNING(log, old_region->toString() << " doesn't match index");
                return false;
            }
        }
    }

    return onSnapshot(new_region, tmt, regions_to_check, try_flush_region);
}

static const metapb::Peer & findPeer(const metapb::Region & region, UInt64 peer_id)
{
    for (const auto & peer : region.peers())
    {
        if (peer.id() == peer_id)
        {
            if (!peer.is_learner())
                throw Exception(std::string(__PRETTY_FUNCTION__) + ": peer is not learner, should not happen", ErrorCodes::LOGICAL_ERROR);
            return peer;
        }
    }

    throw Exception(std::string(__PRETTY_FUNCTION__) + ": peer " + DB::toString(peer_id) + " not found", ErrorCodes::LOGICAL_ERROR);
}

void KVStore::handleApplySnapshot(metapb::Region && region, UInt64 peer_id, const SnapshotDataView & lock_buff,
    const SnapshotDataView & write_buff, const SnapshotDataView & default_buff, UInt64 index, UInt64 term, TMTContext & tmt)
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
    auto new_region = std::make_shared<Region>(std::move(meta), index_reader_create);

    LOG_INFO(log, "Try to apply snapshot: " << new_region->toString(true));

    {
        struct CfData
        {
            ColumnFamilyType type;
            const SnapshotDataView & data;
        };
        std::array<CfData, 3> cf_data_list = {CfData{ColumnFamilyType::Lock, (lock_buff)}, CfData{ColumnFamilyType::Write, (write_buff)},
            CfData{ColumnFamilyType::Default, (default_buff)}};

        for (const auto & cf_data : cf_data_list)
        {
            for (UInt64 n = 0; n < cf_data.data.len; ++n)
            {
                auto & k = cf_data.data.keys[n];
                auto & v = cf_data.data.vals[n];
                auto key = std::string(k.data, k.len);
                auto value = std::string(v.data, v.len);
                new_region->insert(cf_data.type, TiKVKey(std::move(key)), TiKVValue(std::move(value)));
            }
        }
    }

    new_region->tryPreDecodeTiKVValue(tmt);

    bool status = tryApplySnapshot(new_region, tmt.getContext(), true);

    LOG_INFO(log, new_region->toString(false) << " apply snapshot " << (status ? "success" : "fail"));
}

void KVStore::handleIngestSST(UInt64 region_id, const SnapshotDataView & write_buff, const SnapshotDataView & default_buff, UInt64 index,
    UInt64 term, TMTContext & tmt)
{
    auto region_task_lock = region_manager.genRegionTaskLock(region_id);

    const RegionPtr region = getRegion(region_id);

    if (region == nullptr)
        throw Exception(std::string(__PRETTY_FUNCTION__) + ": region " + std::to_string(region_id) + " is not found");

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
            LOG_TRACE(log, __FUNCTION__ << ": catch but ignore exception: " << e.message());
        }
    };

    // try to flush remain data in memory.
    func_try_flush();
    region->handleIngestSST(write_buff, default_buff, index, term);
    region->tryPreDecodeTiKVValue(tmt);
    func_try_flush();

    region_persister.persist(*region, region_task_lock);
}

} // namespace DB
