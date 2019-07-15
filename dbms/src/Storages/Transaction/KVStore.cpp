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

KVStore::KVStore(const std::string & data_dir) : region_persister(data_dir), log(&Logger::get("KVStore")) {}

void KVStore::restore(const RegionClientCreateFunc & region_client_create)
{
    std::lock_guard<std::mutex> lock(mutex);
    LOG_INFO(log, "start to restore regions");
    region_persister.restore(regions, const_cast<RegionClientCreateFunc *>(&region_client_create));
    LOG_INFO(log, "restore regions done");

    // Remove regions whose state = Tombstone, those regions still exist because progress crash after persisted and before removal.
    {
        std::vector<RegionID> regions_to_remove;
        for (auto & p : regions)
        {
            RegionPtr & region = p.second;
            if (region->isPendingRemove())
                regions_to_remove.push_back(region->id());
        }
        for (const auto region_id : regions_to_remove)
            removeRegion(region_id, nullptr);
    }
}

RegionPtr KVStore::getRegion(RegionID region_id) const
{
    std::lock_guard<std::mutex> lock(mutex);
    if (auto it = regions.find(region_id); it != regions.end())
        return it->second;
    return nullptr;
}

size_t KVStore::regionSize() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return regions.size();
}

void KVStore::traverseRegions(std::function<void(RegionID region_id, const RegionPtr & region)> && callback) const
{
    std::lock_guard<std::mutex> lock(mutex);
    for (auto it = regions.begin(); it != regions.end(); ++it)
        callback(it->first, it->second);
}

bool KVStore::onSnapshot(RegionPtr new_region, RegionTable * region_table, const std::optional<UInt64> expect_old_index)
{
    region_persister.persist(new_region);

    {
        std::lock_guard<std::mutex> lock(task_mutex);

        RegionID region_id = new_region->id();
        RegionPtr old_region = getRegion(region_id);
        if (old_region != nullptr)
        {
            UInt64 old_index = old_region->getProbableIndex();

            // in test, may not need expect_old_index.
            if (expect_old_index.has_value())
            {
                if (old_index != *expect_old_index)
                {
                    LOG_WARNING(log, "KVStore::onSnapshot " << old_region->toString(true) << " changed during applying snapshot");
                    return false;
                }
            }

            LOG_DEBUG(log, "KVStore::onSnapshot previous " << old_region->toString(true) << " ; new " << new_region->toString(true));
            if (old_index >= new_region->getProbableIndex())
            {
                LOG_INFO(log, "KVStore::onSnapshot discard new region because of index is outdated");
                return false;
            }
            old_region->assignRegion(std::move(*new_region));
            new_region = old_region;
        }
        else
        {
            std::lock_guard<std::mutex> lock(mutex);
            regions[region_id] = new_region;
        }
    }

    // if the operation about RegionTable is out of the protection of task_mutex, we should make sure that it can't delete any mapping relation.
    if (region_table)
        region_table->applySnapshotRegion(*new_region);

    return true;
}

void KVStore::onServiceCommand(const enginepb::CommandRequestBatch & cmds, RaftContext & raft_ctx)
{
    TMTContext * tmt_context = raft_ctx.context ? &(raft_ctx.context->getTMTContext()) : nullptr;
    RegionTable * region_table = tmt_context ? &(tmt_context->getRegionTable()) : nullptr;

    enginepb::CommandResponseBatch responseBatch;

    const auto report_region_destroy = [&](RegionID region_id) {
        auto & resp = *(responseBatch.add_responses());
        resp.mutable_header()->set_region_id(region_id);
        resp.mutable_header()->set_destroyed(true);
        LOG_INFO(log, "Report [region " << region_id << "] destroyed");
    };

    std::lock_guard<std::mutex> lock(task_mutex);

    for (const auto & cmd : cmds.requests())
    {
        auto & header = cmd.header();
        auto curr_region_id = header.region_id();
        RegionPtr curr_region;
        {
            std::lock_guard<std::mutex> lock(mutex);
            auto it = regions.find(curr_region_id);
            if (unlikely(it == regions.end()))
            {
                LOG_WARNING(log, "[KVStore::onServiceCommand] [region " << curr_region_id << "] is not found, might be removed already");
                report_region_destroy(curr_region_id);

                continue;
            }
            curr_region = it->second;
        }

        auto region_persist_lock = curr_region->genPersistLock();

        if (header.destroy())
        {
            LOG_INFO(log, "Try to remove " << curr_region->toString() << " because of tombstone.");
            curr_region->setPendingRemove();
            removeRegion(curr_region_id, region_table);

            report_region_destroy(curr_region_id);

            continue;
        }

        RaftCommandResult result = curr_region->onCommand(cmd);

        const auto region_report = [&]() { *(responseBatch.add_responses()) = curr_region->toCommandResponse(); };

        const auto report_sync_log = [&]() {
            if (result.sync_log)
            {
                LOG_INFO(log, "Report " << curr_region->toString(true) << " for sync");
                region_report();
            }
        };

        const auto persist_region = [&](const RegionPtr & region) {
            LOG_INFO(log, "Start to persist " << region->toString(true) << ", cache size: " << region->dataSize() << " bytes");
            region_persister.persist(region, region_persist_lock);
            LOG_INFO(log, "Persist " << region->toString(false) << " done");
        };

        const auto persist_and_sync = [&]() {
            if (result.sync_log)
                persist_region(curr_region);
            report_sync_log();
        };

        const auto handle_batch_split = [&](Regions & split_regions) {
            auto & raft_service = raft_ctx.context->getRaftService();
            raft_service.addRegionToFlush(*curr_region);

            {
                std::lock_guard<std::mutex> lock(mutex);

                for (auto & new_region : split_regions)
                {
                    auto [it, ok] = regions.emplace(new_region->id(), new_region);
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
                // persist curr_region at last. if program crashed after split_region is persisted, curr_region can
                // continue to complete split operation.
                for (const auto & new_region : split_regions)
                {
                    persist_region(new_region);
                    region_table->updateRegionForSplit(*new_region, curr_region_id);
                    raft_service.addRegionToFlush(*new_region);
                }

                persist_region(curr_region);
                region_table->shrinkRegionRange(*curr_region);
            }

            report_sync_log();
        };

        const auto handle_update_table_ids = [&](const TableIDSet & table_ids) {
            if (region_table)
                region_table->updateRegion(*curr_region, table_ids);

            persist_and_sync();
        };

        const auto handle_change_peer = [&]() {
            if (curr_region->isPendingRemove())
            {
                removeRegion(curr_region_id, region_table);
                report_sync_log();
            }
            else
                persist_and_sync();
        };

        switch (result.type)
        {
            case RaftCommandResult::Type::IndexError:
                report_sync_log();
                break;
            case RaftCommandResult::Type::BatchSplit:
                handle_batch_split(result.split_regions);
                break;
            case RaftCommandResult::Type::UpdateTableID:
                handle_update_table_ids(result.table_ids);
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

    if (responseBatch.responses_size())
        raft_ctx.send(responseBatch);
}

void KVStore::report(RaftContext & raft_ctx)
{
    std::lock_guard<std::mutex> lock(task_mutex);

    enginepb::CommandResponseBatch responseBatch;
    {
        std::lock_guard<std::mutex> lock(mutex);

        if (regions.empty())
            return;

        for (const auto & p : regions)
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
        region_persister.persist(region);
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

        region_persister.persist(region);

        ss << region_id << ",";
    }

    if (persist_job)
    {
        LOG_DEBUG(log, "Regions ( " << ss.str() << ") are persisted");
    }

    bool gc_job = region_persister.gc();

    return persist_job || gc_job;
}

void KVStore::removeRegion(const RegionID region_id, RegionTable * region_table)
{
    LOG_INFO(log, "Start to remove [region " << region_id << "]");

    RegionPtr region;
    {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = regions.find(region_id);
        region = it->second;
        regions.erase(it);
    }

    region_persister.drop(region_id);

    if (region_table)
        region_table->removeRegion(region_id);

    LOG_INFO(log, "Remove [region " << region_id << "] done");
}

void KVStore::updateRegionTableBySnapshot(RegionTable & region_table)
{
    std::lock_guard<std::mutex> lock(mutex);
    LOG_INFO(log, "start to update RegionTable by snapshot");
    region_table.applySnapshotRegions(regions);
    LOG_INFO(log, "update RegionTable done");
}

} // namespace DB
