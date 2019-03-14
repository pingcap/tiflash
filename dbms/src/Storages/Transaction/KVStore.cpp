#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/TMTContext.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

// TODO move to Settings.h
static Seconds REGION_PERSIST_PERIOD(60);      // 1 minutes
static Seconds KVSTORE_TRY_PERSIST_PERIOD(10); // 10 seconds

KVStore::KVStore(const std::string & data_dir, Context *) : region_persister(data_dir), log(&Logger::get("KVStore"))
{
    std::lock_guard<std::mutex> lock(mutex);
    region_persister.restore(regions);

    // Remove regions which pending_remove = true, those regions still exist because progress crash after persisted and before removal.
    std::vector<RegionID> to_remove;
    for (auto p : regions)
    {
        RegionPtr & region = p.second;
        if (region->isPendingRemove())
            to_remove.push_back(region->id());
    }

    for (auto & region_id : to_remove)
    {
        LOG_INFO(log, "Region [" << region_id << "] is removed after restored.");
        auto it = regions.find(region_id);
        RegionPtr region = it->second;
        regions.erase(it);
        std::ignore = region;
        // TODO: remove region from region_table later, not now
    }
}

RegionPtr KVStore::getRegion(RegionID region_id)
{
    std::lock_guard<std::mutex> lock(mutex);
    auto it = regions.find(region_id);
    return (it == regions.end()) ? nullptr : it->second;
}

RegionMap KVStore::getRegions()
{
    std::lock_guard<std::mutex> lock(mutex);
    return regions;
}

void KVStore::traverseRegions(std::function<void(const RegionPtr & region)> callback)
{
    std::lock_guard<std::mutex> lock(mutex);
    for (auto it = regions.begin(); it != regions.end(); ++it)
        callback(it->second);
}

void KVStore::onSnapshot(const RegionPtr & region, Context * context)
{
    TMTContext * tmt_ctx = (bool)(context) ? &(context->getTMTContext()) : nullptr;
    auto region_id = region->id();

    RegionPtr old_region;
    {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = regions.find(region_id);
        if (it != regions.end())
            old_region = it->second;
    }

    if (old_region != nullptr && old_region->getIndex() >= region->getIndex())
        return;

    region_persister.persist(region);

    {
        std::lock_guard<std::mutex> lock(mutex);
        regions.insert_or_assign(region_id, region);
    }

    if (tmt_ctx)
        tmt_ctx->region_table.applySnapshotRegion(region);
}

void KVStore::onServiceCommand(const enginepb::CommandRequestBatch & cmds, RaftContext & raft_ctx)
{
    Context * context = raft_ctx.context;
    TMTContext * tmt_ctx = (bool)(context) ? &(context->getTMTContext()) : nullptr;

    using std::placeholders::_1;
    using std::placeholders::_2;
    using std::placeholders::_3;

    Region::CmdCallBack callback;
    callback.compute_hash = std::bind(&Consistency::compute, &consistency, _1, _2, _3);
    callback.verify_hash = std::bind(&Consistency::check, &consistency, _1, _2, _3);

    enginepb::CommandResponseBatch responseBatch;
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
                LOG_WARNING(log, "Region[" << curr_region_id << " not found, maybe removed already?");
                continue;
            }
            curr_region = it->second;
        }

        if (curr_region->isPendingRemove())
        {
            // Normally this situation should not exist. Unless some exceptions throw during former removeRegion.
            LOG_DEBUG(log, curr_region->toString() << " (before cmd) is in pending remove status, remove it now.");
            removeRegion(curr_region_id, context);

            LOG_INFO(log, "Sync status because of removal: " << curr_region->toString(true));
            *(responseBatch.mutable_responses()->Add()) = curr_region->toCommandResponse();

            continue;
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

        auto [new_region, split_regions, table_ids, sync] = curr_region->onCommand(cmd, callback);

        if (curr_region->isPendingRemove())
        {
            LOG_DEBUG(log, curr_region->toString() << " (after cmd) is in pending remove status, remove it now.");
            removeRegion(curr_region_id, context);

            LOG_INFO(log, "Sync status because of removal: " << curr_region->toString(true));
            *(responseBatch.mutable_responses()->Add()) = curr_region->toCommandResponse();

            continue;
        }

        if (!split_regions.empty())
        {
            {
                std::lock_guard<std::mutex> lock(mutex);

                regions[new_region->id()] = new_region;

                curr_region = new_region;

                for (const auto & region : split_regions)
                {
                    auto [it, ok] = regions.emplace(region->id(), region);
                    if (!ok)
                    {
                        it->second = region;
                        LOG_INFO(log, "Override existing region " + DB::toString(region->id()));
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
            for (auto & region : split_regions)
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

bool KVStore::tryPersistAndReport(RaftContext & context)
{
    Timepoint now = Clock::now();
    if (now < (last_try_persist_time.load() + KVSTORE_TRY_PERSIST_PERIOD))
        return false;
    last_try_persist_time = now;

    bool persist_job = false;

    enginepb::CommandResponseBatch responseBatch;

    auto all_region_copy = getRegions();

    for (auto && [region_id, region] : all_region_copy)
    {
        std::ignore = region_id;
        if (now < (region->lastPersistTime() + REGION_PERSIST_PERIOD))
            continue;

        persist_job = true;
        region_persister.persist(region);
        region->markPersisted();

        LOG_TRACE(log, "Region " << region->id() << " report status");
        *(responseBatch.mutable_responses()->Add()) = region->toCommandResponse();
    }

    if (persist_job)
    {
        LOG_INFO(log, "Batch report regions status");
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

} // namespace DB
