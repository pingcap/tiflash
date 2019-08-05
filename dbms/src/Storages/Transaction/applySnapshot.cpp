#include <Core/TMTPKType.h>
#include <Interpreters/Context.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/CHTableHandle.h>
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

    if (old_region)
    {
        if (old_region->getIndex() >= new_region->getIndex())
        {
            LOG_WARNING(log, "Region " << new_region->id() << " already has newer index, " << old_region->toString(true));
            return false;
        }
    }

    if (context)
    {
        auto & tmt = context->getTMTContext();
        Timestamp safe_point = tmt.getPDClient()->getGCSafePoint();

        if (old_region)
            new_region->compareAndCompleteSnapshot(safe_point, *old_region);

        for (auto [table_id, storage] : tmt.getStorages().getAllStorage())
        {
            const auto handle_range = new_region->getHandleRangeByTable(table_id);
            if (handle_range.first >= handle_range.second)
                continue;
            HandleMap handle_map;

            {
                auto merge_tree = std::dynamic_pointer_cast<StorageMergeTree>(storage);
                auto table_lock = merge_tree->lockStructure(true, __PRETTY_FUNCTION__);

                const bool pk_is_uint64 = getTMTPKType(*merge_tree->getData().primary_key_data_types[0]) == TMTPKType::UINT64;

                if (pk_is_uint64)
                {
                    const auto [n, new_range] = CHTableHandle::splitForUInt64TableHandle(handle_range);
                    handle_map = getHandleMapByRange<UInt64>(*context, *merge_tree, new_range[0]);
                    if (n > 1)
                    {
                        auto new_handle_map = getHandleMapByRange<UInt64>(*context, *merge_tree, new_range[1]);
                        for (auto & [handle, data] : new_handle_map)
                            handle_map[handle] = std::move(data);
                    }
                }
                else
                    handle_map = getHandleMapByRange<Int64>(*context, *merge_tree, handle_range);
            }

            new_region->compareAndCompleteSnapshot(handle_map, table_id, safe_point);
        }
    }

    // context may be null in test cases.
    return kvstore->onSnapshot(new_region, context ? &context->getTMTContext().getRegionTable() : nullptr);
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

            auto & tikv_key = static_cast<TiKVKey &>(key);
            auto & tikv_value = static_cast<TiKVValue &>(value);

            new_region->insert(data.cf(), std::move(tikv_key), std::move(tikv_value));
        }
    }

    if (new_region->isPeerRemoved())
        throw Exception("[applySnapshot] region is removed, should not happen", ErrorCodes::LOGICAL_ERROR);

    bool status = applySnapshot(kvstore, new_region, context);

    LOG_INFO(log, "Region " << new_region->id() << " apply snapshot " << (status ? "success" : "fail"));
}

} // namespace DB
