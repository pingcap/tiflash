#include <chrono>

#include <Interpreters/Context.h>
#include <Raft/RaftContext.h>
#include <Raft/RaftService.h>
#include <Storages/StorageDeltaMerge.h>
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
            removeRegion(region_id, nullptr, task_lock);
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

bool KVStore::onSnapshot(RegionPtr new_region, Context * context, const RegionsAppliedindexMap & regions_to_check)
{
    RegionID region_id = new_region->id();
    {
        auto region_lock = region_manager.genRegionTaskLock(region_id);
        region_persister.persist(*new_region, region_lock);
    }

    {
        auto task_lock = genTaskLock();
        auto region_lock = region_manager.genRegionTaskLock(region_id);

        for (const auto & region_info : regions_to_check)
        {
            const auto & region = region_info.second.first;

            if (auto it = regions().find(region_info.first); it != regions().end())
            {
                if (it->second != region)
                {
                    LOG_WARNING(log, "[onSnapshot] " << it->second->toString() << " instance changed");
                    return false;
                }
                if (region->appliedIndex() != region_info.second.second)
                {
                    LOG_WARNING(log, "[onSnapshot] " << it->second->toString() << " instance changed");
                    return false;
                }
            }
            else
            {
                LOG_WARNING(log, "[onSnapshot] " << region->toString(false) << " not found");
                return false;
            }
        }

        RegionPtr old_region = getRegion(region_id);
        if (old_region != nullptr)
        {
            LOG_DEBUG(log, "[onSnapshot] previous " << old_region->toString(true) << " ; new " << new_region->toString(true));
            region_range_index.remove(old_region->makeRaftCommandDelegate(task_lock).getRange().comparableKeys(), region_id);
            old_region->assignRegion(std::move(*new_region));
            new_region = old_region;
        }
        else
        {
            auto manage_lock = genRegionManageLock();
            regionsMut().emplace(region_id, new_region);
        }

        region_range_index.add(new_region);
    }

    // if the operation about RegionTable is out of the protection of task_mutex, we should make sure that it can't delete any mapping relation.
    if (context)
    {
        context->getRaftService().addRegionToDecode(new_region);
        context->getTMTContext().getRegionTable().applySnapshotRegion(*new_region);
        if (context->getTMTContext().disableBgFlush())
        {
            context->getTMTContext().getRegionTable().tryFlushRegion(new_region->id());
            LOG_DEBUG(log, "[syncFlush] Apply snapshot for region " << new_region->id());
        }
    }

    return true;
}

void KVStore::onServiceCommand(enginepb::CommandRequestBatch && cmds, RaftContext & raft_ctx)
{
    TMTContext * tmt_context = raft_ctx.context ? &(raft_ctx.context->getTMTContext()) : nullptr;
    RegionTable * region_table = tmt_context ? &(tmt_context->getRegionTable()) : nullptr;
    RaftService * raft_service = raft_ctx.context ? &(raft_ctx.context->getRaftService()) : nullptr;

    enginepb::CommandResponseBatch responseBatch;

    const auto report_region_destroy = [&](const RegionID region_id) {
        auto & resp = *(responseBatch.add_responses());
        resp.mutable_header()->set_region_id(region_id);
        resp.mutable_header()->set_destroyed(true);
        LOG_INFO(log, "Report [region " << region_id << "] destroyed");
    };

    auto task_lock = genTaskLock();

    TableIDSet tables_to_flush;
    std::unordered_set<RegionID> dirty_regions;

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
            removeRegion(curr_region_id, region_table, task_lock);

            report_region_destroy(curr_region_id);

            continue;
        }

        curr_region.makeRaftCommandDelegate(task_lock).onCommand(std::move(cmd), *this, region_table, *raft_cmd_res);
        RaftCommandResult & result = *raft_cmd_res;

        if (tmt_context != nullptr && tmt_context->disableBgFlush())
        {
            // Since background threads which may acquire `dirtyFlag` are disabled, it's safe to check it directly.
            if (curr_region.dirtyFlag())
            {
                dirty_regions.emplace(curr_region_id);
            }

            for (auto id : result.table_ids)
            {
                tables_to_flush.emplace(id);
            }
        }

        const auto region_report = [&]() { *(responseBatch.add_responses()) = curr_region.toCommandResponse(); };

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
                {
                    region_table->updateRegionForSplit(*new_region, curr_region_id);
                    raft_service->addRegionToFlush(*new_region);
                }
                region_table->shrinkRegionRange(curr_region);
                raft_service->addRegionToFlush(curr_region);
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

        const auto handle_update_table_ids = [&](const TableIDSet & table_ids) {
            if (region_table)
                region_table->updateRegion(curr_region, table_ids);

            persist_and_sync();
        };

        const auto handle_change_peer = [&]() {
            if (curr_region.isPendingRemove())
            {
                removeRegion(curr_region_id, region_table, task_lock);
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
            case RaftCommandResult::Type::UpdateTableID:
                handle_update_table_ids(result.table_ids);
                raft_service->addRegionToDecode(curr_region_ptr);
                break;
            case RaftCommandResult::Type::Default:
                persist_and_sync();
                break;
            case RaftCommandResult::Type::ChangePeer:
                handle_change_peer();
                break;
            default:
                throw Exception("Unsupported RaftCommandResult", ErrorCodes::LOGICAL_ERROR);
        }
    }

    if (tmt_context != nullptr && tmt_context->disableBgFlush())
    {
        auto & region_table = tmt_context->getRegionTable();
        for (auto table_id : tables_to_flush)
        {
            auto s_time = Clock::now();
            auto regions_to_flush = region_table.getRegionsByTable(table_id);
            for (auto region : regions_to_flush)
            {
                if (auto && itr = dirty_regions.find(region.first); itr != dirty_regions.end())
                {
                    // Check dirty again
                    if (region.second->dirtyFlag())
                        region_table.tryFlushRegion(region.first, table_id, true);
                }
            }
            auto e_time = Clock::now();
            LOG_DEBUG(log,
                "[syncFlush]"
                    << " table_id " << table_id << ", cost "
                    << std::chrono::duration_cast<std::chrono::milliseconds>(e_time - s_time).count() << "ms");
        }
    }

    if (responseBatch.responses_size())
        raft_ctx.send(responseBatch);
}

void KVStore::report(RaftContext & raft_ctx)
{
    auto lock = genTaskLock();

    enginepb::CommandResponseBatch responseBatch;
    {
        auto manage_lock = genRegionManageLock();

        if (regions().empty())
            return;

        for (const auto & p : regions())
            *(responseBatch.add_responses()) = p.second->toCommandResponse();
    }

    raft_ctx.send(responseBatch);

    LOG_INFO(log, "Report status of " << responseBatch.responses_size() << " regions to proxy");
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

bool KVStore::tryPersist(const Seconds kvstore_try_persist_period, const Seconds region_persist_period)
{
    Timepoint now = Clock::now();
    if (now < (last_try_persist_time.load() + kvstore_try_persist_period))
        return false;
    last_try_persist_time = now;

    RegionMap all_region_copy;
    traverseRegions([&](const RegionID region_id, const RegionPtr & region) {
        if (now < (region->lastPersistTime() + region_persist_period))
            return;
        if (region->dirtyFlag() == 0)
            return;
        all_region_copy[region_id] = region;
    });

    std::stringstream ss;
    bool persist_job = false;

    for (auto && [region_id, region] : all_region_copy)
    {
        persist_job = true;

        region_persister.persist(*region);

        ss << region_id << ",";
    }

    if (persist_job)
    {
        LOG_DEBUG(log, "Regions ( " << ss.str() << ") are persisted");
    }

    bool gc_job = region_persister.gc();

    return persist_job || gc_job;
}

void KVStore::removeRegion(const RegionID region_id, RegionTable * region_table, const KVStoreTaskLock & task_lock)
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

    if (region_table)
        region_table->removeRegion(region_id);


    LOG_INFO(log, "Remove [region " << region_id << "] done");
}

KVStoreTaskLock KVStore::genTaskLock() const { return KVStoreTaskLock(task_mutex); }
RegionMap & KVStore::regionsMut() { return region_manager.regions; }
const RegionMap & KVStore::regions() const { return region_manager.regions; }
KVStore::RegionManageLock KVStore::genRegionManageLock() const { return RegionManageLock(region_manager.mutex); }

} // namespace DB
