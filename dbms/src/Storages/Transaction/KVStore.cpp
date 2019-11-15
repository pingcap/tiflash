#include <Interpreters/Context.h>
#include <Raft/RaftContext.h>
#include <Raft/RaftService.h>
#include <Storages/Transaction/KVStore.h>
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

KVStore::KVStore(Context & context_, const std::string & data_dir)
    : context(context_),
      region_persister(data_dir, region_manager),
      raft_cmd_res(std::make_unique<RaftCommandResult>()),
      log(&Logger::get("KVStore"))
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

    // Remove regions whose state = Tombstone, those regions still exist because progress crash after persisted and before removal.
    {
        std::vector<RegionID> regions_to_remove;
        for (auto & p : regions())
        {
            const RegionPtr & region = p.second;
            if (region->isPendingRemove())
                regions_to_remove.push_back(region->id());
        }
        for (const auto region_id : regions_to_remove)
            removeRegion(region_id, task_lock);
    }
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

bool KVStore::onSnapshot(RegionPtr new_region, const RegionsAppliedindexMap & regions_to_check, bool try_flush_region)
{
    RegionID region_id = new_region->id();

    {
        const auto range = new_region->getRange();
        auto & region_table = context.getTMTContext().getRegionTable();
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

        context.getTMTContext().getRegionTable().shrinkRegionRange(*new_region);
    }

    return true;
}

void KVStore::onServiceCommand(enginepb::CommandRequestBatch && cmds)
{
    TMTContext & tmt_context = context.getTMTContext();
    RegionTable & region_table = tmt_context.getRegionTable();
    RaftService & raft_service = context.getRaftService();

    enginepb::CommandResponseBatch response_batch;

    const auto report_region_destroy = [&](const RegionID region_id) {
        auto & resp = *(response_batch.add_responses());
        resp.mutable_header()->set_region_id(region_id);
        resp.mutable_header()->set_destroyed(true);
        LOG_INFO(log, "Report [region " << region_id << "] destroyed");
    };

    auto task_lock = genTaskLock();

    for (auto && cmd : *cmds.mutable_requests())
    {
        const auto & header = cmd.header();
        const auto curr_region_id = header.region_id();

        const RegionPtr curr_region_ptr = getRegion(curr_region_id);
        if (curr_region_ptr == nullptr)
        {
            LOG_WARNING(log, "[onServiceCommand] [region " << curr_region_id << "] is not found, might be removed already");
            report_region_destroy(curr_region_id);
            continue;
        }

        auto & curr_region = *curr_region_ptr;
        auto region_persist_lock = region_manager.genRegionTaskLock(curr_region_id);

        if (header.destroy())
        {
            LOG_INFO(log, "Try to remove " << curr_region.toString() << " because of tombstone.");
            curr_region.setPendingRemove();
            removeRegion(curr_region_id, task_lock);

            report_region_destroy(curr_region_id);

            continue;
        }

        curr_region.makeRaftCommandDelegate(task_lock).onCommand(std::move(cmd), *this, region_table, *raft_cmd_res);
        RaftCommandResult & result = *raft_cmd_res;

        const auto region_report = [&]() { *(response_batch.add_responses()) = curr_region.toCommandResponse(); };

        const auto report_sync_log = [&]() {
            if (result.sync_log)
            {
                LOG_INFO(log, "Report " << curr_region.toString(true) << " for sync");
                region_report();
            }
        };

        const auto persist_region = [&](const Region & region) {
            LOG_INFO(log, "Start to persist " << region.toString(true) << ", cache size: " << region.dataSize() << " bytes");
            region_persister.persist(region, region_persist_lock);
            LOG_INFO(log, "Persist " << region.toString(false) << " done");
        };

        const auto persist_and_sync = [&]() {
            if (result.sync_log)
                persist_region(curr_region);
            report_sync_log();
        };

        const auto handle_compact_log = [&]() {
            if (curr_region.writeCFCount())
            {
                try
                {
                    auto tmp = region_table.tryFlushRegion(curr_region_ptr, false);
                    raft_service.dataMemReclaim(std::move(tmp));
                }
                catch (...)
                {
                }
            }
            persist_and_sync();
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
                region_range_index.remove(result.range_before_split->comparableKeys(), curr_region_id);
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
                // persist curr_region at last. if program crashed after split_region is persisted, curr_region can
                // continue to complete split operation.
                for (const auto & new_region : split_regions)
                {
                    // no need to lock those new regions, because they don't have middle state.
                    persist_region(*new_region);
                }
                persist_region(curr_region);
            }

            report_sync_log();
        };

        const auto handle_update_table = [&]() {
            region_table.updateRegion(curr_region);
            persist_and_sync();
        };

        const auto handle_change_peer = [&]() {
            if (curr_region.isPendingRemove())
            {
                removeRegion(curr_region_id, task_lock);
                report_sync_log();
            }
            else
                persist_and_sync();
        };

        switch (result.type)
        {
            case RaftCommandResult::Type::IndexError:
                persist_and_sync();
                break;
            case RaftCommandResult::Type::BatchSplit:
                handle_batch_split(result.split_regions);
                break;
            case RaftCommandResult::Type::UpdateTable:
                handle_update_table();
                raft_service.addRegionToDecode(curr_region_ptr);
                break;
            case RaftCommandResult::Type::Default:
                persist_and_sync();
                break;
            case RaftCommandResult::Type::ChangePeer:
                handle_change_peer();
                break;
            case RaftCommandResult::Type::CompactLog:
                handle_compact_log();
                break;
            default:
                throw Exception("Unsupported RaftCommandResult", ErrorCodes::LOGICAL_ERROR);
        }
    }

    if (raft_context)
    {
        if (response_batch.responses_size())
            raft_context->send(response_batch);
    }
}

void KVStore::reportStatusToProxy()
{
    auto lock = genTaskLock();

    enginepb::CommandResponseBatch response_batch;
    {
        auto manage_lock = genRegionManageLock();

        if (regions().empty())
            return;

        for (const auto & p : regions())
            *(response_batch.add_responses()) = p.second->toCommandResponse();
    }

    if (raft_context)
        raft_context->send(response_batch);

    LOG_INFO(log, "Report status of " << response_batch.responses_size() << " regions to proxy");
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

bool KVStore::gcRegionCache(Seconds gc_persist_period)
{
    Timepoint now = Clock::now();
    if (now < (last_gc_time.load() + gc_persist_period))
        return false;
    last_gc_time = now;

    region_persister.gc();

    return false;
}

void KVStore::removeRegion(const RegionID region_id, const KVStoreTaskLock & task_lock)
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

    region_persister.drop(region_id);

    context.getTMTContext().getRegionTable().removeRegion(region_id);

    LOG_INFO(log, "Remove [region " << region_id << "] done");
}

void KVStore::setRaftContext(DB::RaftContext * raft_context)
{
    auto lock = genTaskLock();

    this->raft_context = raft_context;
}

KVStoreTaskLock KVStore::genTaskLock() const { return KVStoreTaskLock(task_mutex); }
RegionMap & KVStore::regionsMut() { return region_manager.regions; }
const RegionMap & KVStore::regions() const { return region_manager.regions; }
KVStore::RegionManageLock KVStore::genRegionManageLock() const { return RegionManageLock(region_manager.mutex); }

} // namespace DB
