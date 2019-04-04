#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/TMTContext.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

KVStore::KVStore(const std::string & data_dir) : region_persister(data_dir), log(&Logger::get("KVStore")) {}

void KVStore::restore(const Region::RegionClientCreateFunc & region_client_create, std::vector<RegionID> * regions_to_remove)
{
    std::lock_guard<std::mutex> lock(mutex);
    LOG_INFO(log, "start to restore regions");
    region_persister.restore(regions, const_cast<Region::RegionClientCreateFunc *>(&region_client_create));
    LOG_INFO(log, "restore regions done");

    // Remove regions which pending_remove = true, those regions still exist because progress crash after persisted and before removal.
    if (regions_to_remove != nullptr)
    {
        for (auto & p : regions)
        {
            RegionPtr & region = p.second;
            if (region->isPendingRemove())
                regions_to_remove->push_back(region->id());
        }
    }
}

RegionPtr KVStore::getRegion(RegionID region_id)
{
    std::lock_guard<std::mutex> lock(mutex);
    auto it = regions.find(region_id);
    return (it == regions.end()) ? nullptr : it->second;
}

size_t KVStore::regionSize() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return regions.size();
}

void KVStore::traverseRegions(std::function<void(RegionID region_id, const RegionPtr & region)> && callback)
{
    std::lock_guard<std::mutex> lock(mutex);
    for (auto it = regions.begin(); it != regions.end(); ++it)
        callback(it->first, it->second);
}

void KVStore::onSnapshot(RegionPtr new_region, Context * context)
{
    TMTContext * tmt_ctx = context ? &(context->getTMTContext()) : nullptr;

    auto table_ids = new_region->getCommittedRecordTableID();

    {
        std::lock_guard<std::mutex> lock(task_mutex);

        RegionID region_id = new_region->id();
        RegionPtr old_region = getRegion(region_id);
        if (old_region != nullptr)
        {
            LOG_DEBUG(log, "KVStore::onSnapshot: previous " << old_region->toString(true) << " ; new " << new_region->toString(true));

            if (old_region->getProbableIndex() >= new_region->getProbableIndex())
            {
                LOG_DEBUG(log, "KVStore::onSnapshot: discard new region because of index is outdated");
                return;
            }
            old_region->reset(std::move(*new_region));
            new_region = old_region;
        }
        else
        {
            std::lock_guard<std::mutex> lock(mutex);
            regions[region_id] = new_region;
        }

        if (new_region->isPendingRemove())
        {
            removeRegion(region_id, context);
            return;
        }
    }

    region_persister.persist(new_region);

    if (tmt_ctx)
        tmt_ctx->region_table.applySnapshotRegion(new_region, table_ids);
}

void KVStore::onServiceCommand(const enginepb::CommandRequestBatch & cmds, RaftContext & raft_ctx)
{
    Context * context = raft_ctx.context;
    TMTContext * tmt_ctx = (bool)(context) ? &(context->getTMTContext()) : nullptr;

    enginepb::CommandResponseBatch responseBatch;
    for (const auto & cmd : cmds.requests())
    {
        auto & header = cmd.header();
        auto curr_region_id = header.region_id();

        std::lock_guard<std::mutex> lock(task_mutex);

        RegionPtr curr_region;
        {
            std::lock_guard<std::mutex> lock(mutex);
            auto it = regions.find(curr_region_id);
            if (unlikely(it == regions.end()))
            {
                LOG_WARNING(log, "Region[" << curr_region_id << " not found, maybe removed already?");
                continue;
            }
            curr_region = it->second;
        }

        if (header.destroy())
        {
            LOG_INFO(log, curr_region->toString() << " is removed by tombstone.");
            curr_region->setPendingRemove();
            removeRegion(curr_region_id, context);

            LOG_INFO(log, "Sync status because of removal by tombstone: " << curr_region->toString(true));
            auto & resp = *(responseBatch.mutable_responses()->Add());
            resp.mutable_header()->set_region_id(curr_region_id);
            resp.mutable_header()->set_destroyed(true);

            continue;
        }

        auto [split_regions, table_ids, sync] = curr_region->onCommand(cmd);

        if (curr_region->isPendingRemove())
        {
            LOG_DEBUG(log, curr_region->toString() << " (after cmd) is in pending remove status, remove it now.");
            removeRegion(curr_region_id, context);

            LOG_INFO(log, "Sync status because of removal: " << curr_region->toString(true));
            *(responseBatch.mutable_responses()->Add()) = curr_region->toCommandResponse();

            continue;
        }

        // TODO: split update kvstore first then region_table, merge should reverse.
        if (!split_regions.empty())
        {
            {
                std::vector<RegionPtr> tmp_split_regions;
                tmp_split_regions.reserve(split_regions.size());
                tmp_split_regions.swap(split_regions);

                std::lock_guard<std::mutex> lock(mutex);

                for (const auto & new_region : tmp_split_regions)
                {
                    auto [it, ok] = regions.emplace(new_region->id(), new_region);
                    if (!ok)
                    {
                        // definitely, any region's index is greater or equal than the initial one, discard it.
                        continue;
                    }
                    else
                    {
                        // add new region
                        split_regions.push_back(it->second);
                    }
                }
            }

            {
                region_persister.persist(curr_region);
                for (const auto & region : split_regions)
                    region_persister.persist(region);
            }

            if (tmt_ctx)
                tmt_ctx->region_table.splitRegion(curr_region, split_regions);
        }
        else
        {
            if (tmt_ctx)
                tmt_ctx->region_table.updateRegion(curr_region, table_ids);

            if (sync)
                region_persister.persist(curr_region);
        }

        if (sync)
        {
            LOG_INFO(log, "Sync status: " << curr_region->toString(true));

            *(responseBatch.mutable_responses()->Add()) = curr_region->toCommandResponse();
            for (const auto & region : split_regions)
                *(responseBatch.mutable_responses()->Add()) = region->toCommandResponse();
        }
    }

    if (responseBatch.responses_size())
        raft_ctx.send(responseBatch);
}

void KVStore::report(RaftContext & raft_ctx)
{
    std::lock_guard<std::mutex> lock(mutex);

    if (regions.empty())
        return;

    enginepb::CommandResponseBatch responseBatch;
    for (const auto & p : regions)
        *(responseBatch.mutable_responses()->Add()) = p.second->toCommandResponse();
    raft_ctx.send(responseBatch);
}

bool KVStore::tryPersistAndReport(RaftContext & context, const Seconds kvstore_try_persist_period, const Seconds region_persist_period)
{
    Timepoint now = Clock::now();
    if (now < (last_try_persist_time.load() + kvstore_try_persist_period))
        return false;
    last_try_persist_time = now;

    bool persist_job = false;

    enginepb::CommandResponseBatch responseBatch;

    RegionMap all_region_copy;
    traverseRegions([&](const RegionID region_id, const RegionPtr & region) {
        if (now < (region->lastPersistTime() + region_persist_period))
            return;
        if (region->persistParm() == 0)
            return;
        all_region_copy[region_id] = region;
    });

    std::stringstream ss;

    for (auto && [region_id, region] : all_region_copy)
    {
        persist_job = true;

        auto response = responseBatch.mutable_responses()->Add();

        region_persister.persist(region, response);

        ss << "(" << region_id << "," << region->persistParm() << ") ";
    }

    if (persist_job)
    {
        LOG_TRACE(log, "Regions " << ss.str() << "report status");
        LOG_TRACE(log, "Batch report regions status");
        context.send(responseBatch);
    }

    bool gc_job = region_persister.gc();

    return persist_job || gc_job;
}

void KVStore::removeRegion(RegionID region_id, Context * context)
{
    RegionPtr region;
    {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = regions.find(region_id);
        region = it->second;
        regions.erase(it);
    }

    region_persister.drop(region_id);
    if (context)
        context->getTMTContext().region_table.removeRegion(region);
}

void KVStore::updateRegionTableBySnapshot(RegionTable & region_table)
{
    std::lock_guard<std::mutex> lock(mutex);
    LOG_INFO(log, "start to update RegionTable by snapshot");
    region_table.applySnapshotRegions(regions);
    LOG_INFO(log, "update RegionTable done");
}

} // namespace DB
