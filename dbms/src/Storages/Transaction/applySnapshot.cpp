#include <Interpreters/Context.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionDataMover.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/applySnapshot.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

static const std::string RegionSnapshotName = "RegionSnapshot";

bool applySnapshot(const KVStorePtr & kvstore, RegionPtr new_region, Context * context)
{
    Logger * log = &Logger::get(RegionSnapshotName);

    auto old_region = kvstore->getRegion(new_region->id());
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

    if (context)
    {
        auto & tmt = context->getTMTContext();
        Timestamp safe_point = tmt.getPDClient()->getGCSafePoint();

        std::unordered_map<TableID, HandleMap> handle_maps;

        {
            std::stringstream ss;
            // Get all regions whose range overlapped with the one of new_region.
            const auto & new_range = new_region->getRange();

            ss << "New range " << new_range->comparableKeys().first.key.toHex() << "," << new_range->comparableKeys().second.key.toHex()
               << " is overlapped with ";

            kvstore->handleRegionsByRangeOverlap(new_range->comparableKeys(), [&](RegionMap region_map, const KVStoreTaskLock & task_lock) {
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
                new_region->compareAndUpdateHandleMaps(*region_info.second.first, handle_maps);
        }

        // Traverse all table in ch and update handle_maps.
        for (auto [table_id, storage] : tmt.getStorages().getAllStorage())
        {
            const auto handle_range = new_region->getHandleRangeByTable(table_id);
            if (handle_range.first >= handle_range.second)
                continue;
            {
                auto merge_tree = std::dynamic_pointer_cast<StorageMergeTree>(storage);
                auto table_lock = merge_tree->lockStructure(false, __PRETTY_FUNCTION__);
                if (merge_tree->is_dropped)
                    continue;

                getHandleMapByRange(*context, *merge_tree, handle_range, handle_maps[table_id]);
            }
        }

        for (auto & [table_id, handle_map] : handle_maps)
            new_region->compareAndCompleteSnapshot(handle_map, table_id, safe_point);
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

    return kvstore->onSnapshot(new_region, context, regions_to_check);
}

void applySnapshot(const KVStorePtr & kvstore, RequestReader read, Context * context)
{
    Logger * log = &Logger::get(RegionSnapshotName);

    enginepb::SnapshotRequest request;
    auto ok = read(&request);
    if (!ok)
        throw Exception("Read snapshot fail", ErrorCodes::LOGICAL_ERROR);

    if (!request.has_state())
        throw Exception("Failed to read snapshot state", ErrorCodes::LOGICAL_ERROR);

    const auto & state = request.state();
    pingcap::kv::RegionClientPtr region_client = nullptr;
    auto meta = RegionMeta(state.peer(), state.region(), state.apply_state());
    RegionClientCreateFunc region_client_create = [&](pingcap::kv::RegionVerID id) -> pingcap::kv::RegionClientPtr {
        if (context)
        {
            auto & tmt_ctx = context->getTMTContext();
            return tmt_ctx.createRegionClient(id);
        }
        return nullptr;
    };
    auto new_region = std::make_shared<Region>(std::move(meta), region_client_create);

    LOG_INFO(log, "Try to apply snapshot: " << new_region->toString(true));

    while (read(&request))
    {
        if (!request.has_data())
            throw Exception("Failed to read snapshot data", ErrorCodes::LOGICAL_ERROR);

        auto & data = *request.mutable_data();
        auto & cf_data = *data.mutable_data();
        for (auto it = cf_data.begin(); it != cf_data.end(); ++it)
        {
            auto & key = *it->mutable_key();
            auto & value = *it->mutable_value();

            new_region->insert(data.cf(), TiKVKey(std::move(key)), TiKVValue(std::move(value)));
        }
    }

    if (new_region->isPeerRemoved())
        throw Exception("[applySnapshot] region is removed, should not happen", ErrorCodes::LOGICAL_ERROR);

    bool status = applySnapshot(kvstore, new_region, context);

    LOG_INFO(log, new_region->toString(false) << " apply snapshot " << (status ? "success" : "fail"));
}

} // namespace DB
