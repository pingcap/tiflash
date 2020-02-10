#include <chrono>

#include <Interpreters/Context.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/Transaction/BackgroundService.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFIType.h>
#include <Storages/Transaction/RaftCommandResult.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTContext.h>

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

    LOG_INFO(log, "start to restore regions");
    regionsMut() = region_persister.restore(const_cast<IndexReaderCreateFunc *>(&index_reader_create));

    // init range index
    for (const auto & region : regions())
        region_range_index.add(region.second);

    LOG_INFO(log, "restore regions done");
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

bool KVStore::onSnapshot(RegionPtr new_region, TMTContext & tmt, const RegionsAppliedindexMap & regions_to_check, bool try_flush_region)
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
            if (try_flush_region)
                region_table.tryFlushRegion(new_region, false);
        }
        catch (...)
        {
        }
    }

    {
        auto task_lock = genTaskLock();
        auto region_lock = region_manager.genRegionTaskLock(region_id);

        for (const auto & region_info : regions_to_check)
        {
            const auto & region = region_info.second.first;

            if (auto it = regions().find(region_info.first); it != regions().end())
            {
                if (it->second != region || region->appliedIndex() != region_info.second.second)
                {
                    LOG_WARNING(log, __FUNCTION__ << ": instance changed region " << region_info.first);
                    return false;
                }
            }
            else
            {
                LOG_WARNING(log, __FUNCTION__ << ": not found " << region->toString(false));
                return false;
            }
        }

        RegionPtr old_region = getRegion(region_id);
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

    return true;
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
    removeRegion(region_id, region_table, task_lock, region_lock);
}

void KVStore::removeRegion(
    const RegionID region_id, RegionTable & region_table, const KVStoreTaskLock & task_lock, const RegionTaskLock & region_lock)
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

    region_table.removeRegion(region_id);

    LOG_INFO(log, "Remove [region " << region_id << "] done");
}

KVStoreTaskLock KVStore::genTaskLock() const { return KVStoreTaskLock(task_mutex); }
RegionMap & KVStore::regionsMut() { return region_manager.regions; }
const RegionMap & KVStore::regions() const { return region_manager.regions; }
KVStore::RegionManageLock KVStore::genRegionManageLock() const { return RegionManageLock(region_manager.mutex); }

TiFlashApplyRes KVStore::handleWriteRaftCmd(
    raft_cmdpb::RaftCmdRequest && request, UInt64 region_id, UInt64 index, UInt64 term, TMTContext & tmt)
{
    auto region_persist_lock = region_manager.genRegionTaskLock(region_id);

    const RegionPtr region = getRegion(region_id);
    if (region == nullptr)
    {
        LOG_WARNING(log, __PRETTY_FUNCTION__ << ": [region " << region_id << "] is not found, might be removed already");
        return TiFlashApplyRes::NotFound;
    }
    const auto ori_size = region->dataSize();
    region->handleWriteRaftCmd(std::move(request), index, term);
    {
        tmt.getRegionTable().updateRegion(*region);
        if (region->dataSize() != ori_size)
        {
            if (tmt.isBgFlushDisabled())
            {
                // Decode data in region and then flush
                region->tryPreDecodeTiKVValue(tmt);
                tmt.getRegionTable().tryFlushRegion(region, true);
            }
            else
            {
                tmt.getBackgroundService().addRegionToDecode(region);
            }
        }
    }
    return TiFlashApplyRes::None;
}

void KVStore::handleDestroy(UInt64 region_id, TMTContext & tmt)
{
    auto task_lock = genTaskLock();
    const auto region = getRegion(region_id);
    if (region == nullptr)
    {
        LOG_WARNING(log, __PRETTY_FUNCTION__ << ": [region " << region_id << "] is not found, might be removed already");
        return;
    }
    LOG_INFO(log, "Handle destroy " << region->toString());
    region->setPendingRemove();
    removeRegion(region_id, tmt.getRegionTable(), task_lock, region_manager.genRegionTaskLock(region_id));
}

void KVStore::setRegionCompactLogPeriod(Seconds period) { REGION_COMPACT_LOG_PERIOD = period; }

TiFlashApplyRes KVStore::handleAdminRaftCmd(raft_cmdpb::AdminRequest && request,
    raft_cmdpb::AdminResponse && response,
    UInt64 curr_region_id,
    UInt64 index,
    UInt64 term,
    TMTContext & tmt)
{
    RegionTable & region_table = tmt.getRegionTable();

    auto task_lock = genTaskLock();

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
        curr_region.makeRaftCommandDelegate(task_lock).handleAdminRaftCmd(
            request, response, index, term, *this, region_table, *raft_cmd_res);
        RaftCommandResult & result = *raft_cmd_res;
        auto type = request.cmd_type();
        switch (type)
        {
            case raft_cmdpb::AdminCmdType::CompactLog:
            {
                if (curr_region.lastCompactLogTime() + REGION_COMPACT_LOG_PERIOD.load(std::memory_order_relaxed) > Clock::now())
                    sync_log = false;
                else
                {
                    curr_region.markCompactLog();
                    LOG_INFO(log, curr_region.toString(true) << " make proxy compact log");
                }
                break;
            }
            case raft_cmdpb::AdminCmdType::VerifyHash:
            case raft_cmdpb::AdminCmdType::ComputeHash:
            {
                static const Seconds REGION_MAX_PERSIST_PERIOD(60 * 20);
                if (curr_region.lastPersistTime() + REGION_MAX_PERSIST_PERIOD > Clock::now())
                    sync_log = false;
                break;
            }
            default:
                break;
        }

        const auto persist_region = [&](const Region & region) {
            LOG_INFO(log, "Start to persist " << region.toString(true) << ", cache size: " << region.dataSize() << " bytes");
            region_persister.persist(region, region_task_lock);
            LOG_INFO(log, "Persist " << region.toString(false) << " done");
        };

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

        const auto persist_and_sync = [&]() {
            if (sync_log)
                persist_region(curr_region);
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
                    persist_region(*new_region);
                }
                persist_region(curr_region);
            }
        };

        const auto handle_change_peer = [&]() {
            if (curr_region.isPendingRemove())
                removeRegion(curr_region_id, region_table, task_lock, region_task_lock);
            else
                persist_and_sync();
        };

        const auto handle_commit_merge = [&](const RegionID source_region_id) {
            region_table.shrinkRegionRange(curr_region);
            try_to_flush_region(curr_region_ptr);
            persist_region(curr_region);
            {
                auto source_region = getRegion(source_region_id);
                source_region->setPendingRemove();
                removeRegion(source_region_id, region_table, task_lock, region_manager.genRegionTaskLock(source_region_id));
            }
            region_range_index.remove(result.ori_region_range->comparableKeys(), curr_region_id);
            region_range_index.add(curr_region_ptr);
        };

        switch (result.type)
        {
            case RaftCommandResult::Type::IndexError:
            {
                sync_log = true;
                if (type == raft_cmdpb::AdminCmdType::CommitMerge)
                {
                    if (auto source_region = getRegion(request.commit_merge().source().id()); source_region)
                    {
                        LOG_WARNING(log,
                            "Admin cmd " << raft_cmdpb::AdminCmdType_Name(type) << " has been applied, try to remove source "
                                         << source_region->toString(false));
                        source_region->setPendingRemove();
                        removeRegion(source_region->id(), region_table, task_lock, region_manager.genRegionTaskLock(source_region->id()));
                    }
                }
                break;
            }
            case RaftCommandResult::Type::BatchSplit:
                handle_batch_split(result.split_regions);
                break;
            case RaftCommandResult::Type::Default:
                persist_and_sync();
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

        return sync_log ? TiFlashApplyRes::Persist : TiFlashApplyRes::None;
    }
}
} // namespace DB
