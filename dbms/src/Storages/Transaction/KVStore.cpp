#include <Interpreters/Context.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>
#include <Storages/Transaction/BackgroundService.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFIType.h>
#include <Storages/Transaction/RaftCommandResult.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTContext.h>

#include <chrono>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

KVStore::KVStore(const std::string & data_dir)
    : region_persister(data_dir, region_manager), raft_cmd_res(std::make_unique<RaftCommandResult>()), log(&Logger::get("KVStore"))
{}

void KVStore::restore(const IndexReaderCreateFunc & index_reader_create)
{
    auto task_lock = genTaskLock();
    auto manage_lock = genRegionManageLock();

    regionsMut() = region_persister.restore(const_cast<IndexReaderCreateFunc *>(&index_reader_create));

    std::stringstream ss;
    ss << "Restored " << regions().size() << " regions. ";

    // init range index
    for (const auto & [id, region] : regions())
    {
        std::ignore = id;
        region_range_index.add(region);
        ss << region->toString() << "; ";
    }

    LOG_INFO(log, ss.str());
}

RegionPtr KVStore::getRegion(const RegionID region_id) const
{
    auto manage_lock = genRegionManageLock();
    if (auto it = regions().find(region_id); it != regions().end())
        return it->second;
    return nullptr;
}

void KVStore::handleRegionsByRangeOverlap(
    const RegionRange & range, std::function<void(RegionMap, const KVStoreTaskLock &)> && callback) const
{
    auto task_lock = genTaskLock();
    callback(region_range_index.findByRangeOverlap(range), task_lock);
}

const RegionManager::RegionTaskElement & RegionManager::getRegionTaskCtrl(const RegionID region_id) const
{
    std::lock_guard<std::mutex> lock(mutex);

    if (auto it = regions_ctrl.find(region_id); it != regions_ctrl.end())
        return it->second;

    return regions_ctrl.try_emplace(region_id).first->second;
}

RegionTaskLock RegionManager::genRegionTaskLock(const RegionID region_id) const
{
    return RegionTaskLock(getRegionTaskCtrl(region_id).mutex);
}

size_t KVStore::regionSize() const
{
    auto manage_lock = genRegionManageLock();
    return regions().size();
}

void KVStore::traverseRegions(std::function<void(RegionID, const RegionPtr &)> && callback) const
{
    auto manage_lock = genRegionManageLock();
    for (auto it = regions().begin(); it != regions().end(); ++it)
        callback(it->first, it->second);
}

void KVStore::tryFlushRegionCacheInStorage(TMTContext & tmt, const Region & region, Poco::Logger * log)
{
    if (tmt.isBgFlushDisabled())
    {
        auto table_id = region.getMappedTableID();
        auto handle_range = region.getHandleRangeByTable(table_id);
        auto storage = tmt.getStorages().get(table_id);
        if (storage == nullptr)
        {
            LOG_WARNING(log,
                "tryFlushRegionCacheInStorage can not get table for region:" + region.toString()
                    + " with table id: " + DB::toString(table_id) + ", ignored");
            return;
        }
        storage->flushCache(tmt.getContext(), handle_range);
    }
}

void KVStore::onSnapshot(RegionPtr new_region, RegionPtr old_region, UInt64 old_region_index, TMTContext & tmt)
{
    RegionID region_id = new_region->id();

    {
        const auto range = new_region->getRange();
        auto & region_table = tmt.getRegionTable();
        // extend region to make sure data won't be removed.
        region_table.extendRegionRange(region_id, *range);
        // try to flush data into ch first.
        try
        {
            auto tmp = region_table.tryFlushRegion(new_region, false);
            {
                std::lock_guard<std::mutex> lock(bg_gc_region_data_mutex);
                bg_gc_region_data.push_back(std::move(tmp));
            }
            tryFlushRegionCacheInStorage(tmt, *new_region, log);
        }
        catch (...)
        {
        }
    }

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

        region_persister.persist(*new_region, region_lock);
        region_range_index.add(new_region);

        tmt.getRegionTable().shrinkRegionRange(*new_region);
    }
}

void KVStore::tryPersist(const RegionID region_id)
{
    auto region = getRegion(region_id);
    if (region)
    {
        LOG_INFO(log, "Try to persist " << region->toString(false));
        region_persister.persist(*region);
        LOG_INFO(log, "After persisted " << region->toString(false) << ", cache " << region->dataSize() << " bytes");
    }
}

void KVStore::gcRegionCache(Seconds gc_persist_period)
{
    decltype(bg_gc_region_data) tmp;
    {
        std::lock_guard<std::mutex> lock(bg_gc_region_data_mutex);
        tmp.swap(bg_gc_region_data);
    }
    Timepoint now = Clock::now();
    if (now < (last_gc_time.load() + gc_persist_period))
        return;
    last_gc_time = now;
    region_persister.gc();
}

void KVStore::mockRemoveRegion(const DB::RegionID region_id, RegionTable & region_table)
{
    auto task_lock = genTaskLock();
    auto region_lock = region_manager.genRegionTaskLock(region_id);
    // mock remove region should remove data by default
    removeRegion(region_id, /* remove_data */ true, region_table, task_lock, region_lock);
}

void KVStore::removeRegion(const RegionID region_id, bool remove_data, RegionTable & region_table, const KVStoreTaskLock & task_lock,
    const RegionTaskLock & region_lock)
{
    LOG_INFO(log, "Start to remove [region " << region_id << "]");

    RegionPtr region;
    {
        auto manage_lock = genRegionManageLock();
        auto it = regions().find(region_id);
        region = it->second;
        regionsMut().erase(it);
    }
    {
        // remove index
        region_range_index.remove(region->makeRaftCommandDelegate(task_lock).getRange().comparableKeys(), region_id);
    }

    region_persister.drop(region_id, region_lock);
    LOG_INFO(log, "Persisted [region " << region_id << "] deleted");

    region_table.removeRegion(region_id, remove_data, region_lock);

    LOG_INFO(log, "Remove [region " << region_id << "] done");
}

KVStoreTaskLock KVStore::genTaskLock() const { return KVStoreTaskLock(task_mutex); }
RegionMap & KVStore::regionsMut() { return region_manager.regions; }
const RegionMap & KVStore::regions() const { return region_manager.regions; }
KVStore::RegionManageLock KVStore::genRegionManageLock() const { return RegionManageLock(region_manager.mutex); }

TiFlashApplyRes KVStore::handleWriteRaftCmd(
    raft_cmdpb::RaftCmdRequest && request, UInt64 region_id, UInt64 index, UInt64 term, TMTContext & tmt)
{
    std::vector<BaseBuffView> keys;
    std::vector<BaseBuffView> vals;
    std::vector<WriteCmdType> cmd_types;
    std::vector<ColumnFamilyType> cmd_cf;
    keys.reserve(request.requests_size());
    vals.reserve(request.requests_size());
    cmd_types.reserve(request.requests_size());
    cmd_cf.reserve(request.requests_size());

    for (const auto & req : request.requests())
    {
        auto type = req.cmd_type();

        switch (type)
        {
            case raft_cmdpb::CmdType::Put:
                keys.push_back({req.put().key().data(), req.put().key().size()});
                vals.push_back({req.put().value().data(), req.put().value().size()});
                cmd_types.push_back(WriteCmdType::Put);
                cmd_cf.push_back(NameToCF(req.put().cf()));
                break;
            case raft_cmdpb::CmdType::Delete:
                keys.push_back({req.delete_().key().data(), req.delete_().key().size()});
                vals.push_back({nullptr, 0});
                cmd_types.push_back(WriteCmdType::Del);
                cmd_cf.push_back(NameToCF(req.delete_().cf()));
                break;
            default:
                break;
        }
    }
    return handleWriteRaftCmd(
        WriteCmdsView{.keys = keys.data(), .vals = vals.data(), .cmd_types = cmd_types.data(), .cmd_cf = cmd_cf.data(), .len = keys.size()},
        region_id, index, term, tmt);
}

TiFlashApplyRes KVStore::handleWriteRaftCmd(const WriteCmdsView & cmds, UInt64 region_id, UInt64 index, UInt64 term, TMTContext & tmt)
{
    auto region_persist_lock = region_manager.genRegionTaskLock(region_id);

    const RegionPtr region = getRegion(region_id);
    if (region == nullptr)
    {
        LOG_WARNING(log, __PRETTY_FUNCTION__ << ": [region " << region_id << "] is not found, might be removed already");
        return TiFlashApplyRes::NotFound;
    }

    const auto ori_size = region->dataSize();
    auto res = region->handleWriteRaftCmd(cmds, index, term, tmt);

    {
        tmt.getRegionTable().updateRegion(*region);
        if (region->dataSize() != ori_size && !tmt.isBgFlushDisabled())
        {
            tmt.getBackgroundService().addRegionToDecode(region);
        }
    }

    return res;
}

void KVStore::handleDestroy(UInt64 region_id, TMTContext & tmt)
{
    auto task_lock = genTaskLock();
    const auto region = getRegion(region_id);
    if (region == nullptr)
    {
        LOG_INFO(log, __PRETTY_FUNCTION__ << ": [region " << region_id << "] is not found, might be removed already");
        return;
    }
    LOG_INFO(log, "Handle destroy " << region->toString());
    region->setPendingRemove();
    removeRegion(region_id, /* remove_data */ true, tmt.getRegionTable(), task_lock, region_manager.genRegionTaskLock(region_id));
}

void KVStore::setRegionCompactLogPeriod(Seconds period) { REGION_COMPACT_LOG_PERIOD = period; }

void KVStore::persistRegion(const Region & region, const RegionTaskLock & region_task_lock)
{
    LOG_INFO(log, "Start to persist " << region.toString(true) << ", cache size: " << region.dataSize() << " bytes");
    region_persister.persist(region, region_task_lock);
    LOG_DEBUG(log, "Persist " << region.toString(false) << " done");
}

TiFlashApplyRes KVStore::handleUselessAdminRaftCmd(
    raft_cmdpb::AdminCmdType cmd_type, UInt64 curr_region_id, UInt64 index, UInt64 term, TMTContext & tmt)
{
    auto region_task_lock = region_manager.genRegionTaskLock(curr_region_id);
    bool sync_log = true;
    const RegionPtr curr_region_ptr = getRegion(curr_region_id);
    if (curr_region_ptr == nullptr)
    {
        LOG_WARNING(log, __PRETTY_FUNCTION__ << ": [region " << curr_region_id << "] is not found, might be removed already");
        return TiFlashApplyRes::NotFound;
    }

    auto & curr_region = *curr_region_ptr;

    LOG_INFO(log,
        curr_region.toString(false) << " handle useless admin command " << raft_cmdpb::AdminCmdType_Name(cmd_type) << " at [term: " << term
                                    << ", index: " << index << "]");

    curr_region.handleWriteRaftCmd({}, index, term, tmt);

    if (cmd_type != raft_cmdpb::AdminCmdType::CompactLog)
    {
        sync_log = false;
    }
    else
    {
        if (curr_region.lastCompactLogTime() + REGION_COMPACT_LOG_PERIOD.load(std::memory_order_relaxed) > Clock::now())
        {
            sync_log = false;
            LOG_DEBUG(log, curr_region.toString(false) << " ignore compact log cmd");
        }
        else
        {
            curr_region.markCompactLog();
        }
    }

    if (sync_log)
    {
        tryFlushRegionCacheInStorage(tmt, curr_region, log);
        persistRegion(curr_region, region_task_lock);
        return TiFlashApplyRes::Persist;
    }
    return TiFlashApplyRes::None;
}

TiFlashApplyRes KVStore::handleAdminRaftCmd(raft_cmdpb::AdminRequest && request,
    raft_cmdpb::AdminResponse && response,
    UInt64 curr_region_id,
    UInt64 index,
    UInt64 term,
    TMTContext & tmt)
{
    auto type = request.cmd_type();
    switch (request.cmd_type())
    {
        // CompactLog | VerifyHash | ComputeHash won't change region meta, there is no need to occupy task lock of kvstore.
        case raft_cmdpb::AdminCmdType::CompactLog:
        case raft_cmdpb::AdminCmdType::VerifyHash:
        case raft_cmdpb::AdminCmdType::ComputeHash:
            return handleUselessAdminRaftCmd(type, curr_region_id, index, term, tmt);
        default:
            break;
    }

    RegionTable & region_table = tmt.getRegionTable();

    auto task_lock = genTaskLock();

    {
        auto region_task_lock = region_manager.genRegionTaskLock(curr_region_id);
        const RegionPtr curr_region_ptr = getRegion(curr_region_id);
        if (curr_region_ptr == nullptr)
        {
            LOG_WARNING(log, __PRETTY_FUNCTION__ << ": [region " << curr_region_id << "] is not found, might be removed already");
            return TiFlashApplyRes::NotFound;
        }

        auto & curr_region = *curr_region_ptr;
        curr_region.makeRaftCommandDelegate(task_lock).handleAdminRaftCmd(
            request, response, index, term, *this, region_table, *raft_cmd_res);
        RaftCommandResult & result = *raft_cmd_res;

        // After region split / merge, try to flush it
        const auto try_to_flush_region = [&tmt](const RegionPtr & region) {
            if (tmt.isBgFlushDisabled())
            {
                tmt.getRegionTable().tryFlushRegion(region, false);
            }
            else
            {
                if (region->writeCFCount() >= 8192)
                    tmt.getBackgroundService().addRegionToFlush(region);
            }
        };

        const auto persist_and_sync = [&](const Region & region) {
            tryFlushRegionCacheInStorage(tmt, region, log);
            persistRegion(region, region_task_lock);
        };

        const auto handle_batch_split = [&](Regions & split_regions) {
            {
                auto manage_lock = genRegionManageLock();

                for (auto & new_region : split_regions)
                {
                    auto [it, ok] = regionsMut().emplace(new_region->id(), new_region);
                    if (!ok)
                    {
                        // definitely, any region's index is greater or equal than the initial one.

                        // if there is already a region with same id, it means program crashed while persisting.
                        // just use the previous one.
                        new_region = it->second;
                    }
                }
            }

            {
                region_range_index.remove(result.ori_region_range->comparableKeys(), curr_region_id);
                region_range_index.add(curr_region_ptr);

                for (auto & new_region : split_regions)
                    region_range_index.add(new_region);
            }

            {
                // update region_table first is safe, because the core rule is established: the range in RegionTable
                // is always >= range in KVStore.
                for (const auto & new_region : split_regions)
                    region_table.updateRegion(*new_region);
                region_table.shrinkRegionRange(curr_region);
            }

            {
                for (const auto & new_region : split_regions)
                    try_to_flush_region(new_region);
            }

            {
                // persist curr_region at last. if program crashed after split_region is persisted, curr_region can
                // continue to complete split operation.
                for (const auto & new_region : split_regions)
                {
                    // no need to lock those new regions, because they don't have middle state.
                    persist_and_sync(*new_region);
                }
                persist_and_sync(curr_region);
            }
        };

        const auto handle_change_peer = [&]() {
            if (curr_region.isPendingRemove())
            {
                // remove `curr_region` from this node, we can remove its data.
                removeRegion(curr_region_id, /* remove_data */ true, region_table, task_lock, region_task_lock);
            }
            else
                persist_and_sync(curr_region);
        };

        const auto handle_commit_merge = [&](const RegionID source_region_id) {
            region_table.shrinkRegionRange(curr_region);
            try_to_flush_region(curr_region_ptr);
            persist_and_sync(curr_region);
            {
                auto source_region = getRegion(source_region_id);
                // `source_region` is merged, don't remove its data in storage.
                removeRegion(
                    source_region_id, /* remove_data */ false, region_table, task_lock, region_manager.genRegionTaskLock(source_region_id));
            }
            region_range_index.remove(result.ori_region_range->comparableKeys(), curr_region_id);
            region_range_index.add(curr_region_ptr);
        };

        switch (result.type)
        {
            case RaftCommandResult::Type::IndexError:
            {
                if (type == raft_cmdpb::AdminCmdType::CommitMerge)
                {
                    if (auto source_region = getRegion(request.commit_merge().source().id()); source_region)
                    {
                        LOG_WARNING(log,
                            "Admin cmd " << raft_cmdpb::AdminCmdType_Name(type) << " has been applied, try to remove source "
                                         << source_region->toString(false));
                        source_region->setPendingRemove();
                        // `source_region` is merged, don't remove its data in storage.
                        removeRegion(source_region->id(), /* remove_data */ false, region_table, task_lock,
                            region_manager.genRegionTaskLock(source_region->id()));
                    }
                }
                break;
            }
            case RaftCommandResult::Type::BatchSplit:
                handle_batch_split(result.split_regions);
                break;
            case RaftCommandResult::Type::Default:
                persist_and_sync(curr_region);
                break;
            case RaftCommandResult::Type::ChangePeer:
                handle_change_peer();
                break;
            case RaftCommandResult::Type::CommitMerge:
                handle_commit_merge(result.source_region_id);
                break;
            default:
                throw Exception("Unsupported RaftCommandResult", ErrorCodes::LOGICAL_ERROR);
        }

        return TiFlashApplyRes::Persist;
    }
}
} // namespace DB
