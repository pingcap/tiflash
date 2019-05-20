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

void applySnapshot(KVStorePtr kvstore, RequestReader read, Context * context)
{
    Logger * log = &Logger::get("RegionSnapshot");

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
    auto new_region = std::make_shared<Region>(meta, region_client_create);

    LOG_INFO(log, "Region " << new_region->id() << " try to apply snapshot: " << new_region->toString(true));

    auto old_region = kvstore->getRegion(new_region->id());

    std::optional<UInt64> expect_old_index;
    {
        expect_old_index = old_region->getIndex();
        if (expect_old_index >= new_region->getIndex())
        {
            LOG_INFO(log, "Region " << new_region->id() << " already has newer index, " << old_region->toString(true));
            return;
        }
    }

    while (read(&request))
    {
        if (!request.has_data())
            throw Exception("Failed to read snapshot data", ErrorCodes::LOGICAL_ERROR);
        const auto & data = request.data();

        {
            auto cf_data = data.data();
            auto it = cf_data.begin();
            auto cf_name = data.cf();
            auto key = TiKVKey();
            auto value = TiKVValue();
            new_region->batchInsert([&](Region::BatchInsertElement & node) -> bool {
                if (it == cf_data.end())
                    return false;
                key = TiKVKey(it->key());
                value = TiKVValue(it->value());
                node = Region::BatchInsertElement(&key, &value, &cf_name);
                ++it;
                return true;
            });
        }
    }

    {
        if (new_region->isPeerRemoved())
            new_region->setPendingRemove();
    }

    if (context)
    {
        auto & tmt = context->getTMTContext();
        Timestamp safe_point = tmt.getPDClient()->getGCSafePoint();
        auto table_id_set = new_region->getCommittedRecordTableID();

        auto old_region_cache_data = old_region->dumpWriteCFTableHandleVersion();

        for (const auto table_id : table_id_set)
        {
            const auto handle_range = new_region->getHandleRangeByTable(table_id);
            if (handle_range.first >= handle_range.second)
                continue;
            HandleMap handle_map;

            if (auto storage = tmt.storages.get(table_id); storage)
            {
                auto merge_tree = std::dynamic_pointer_cast<StorageMergeTree>(storage);
                auto table_lock = merge_tree->lockStructure(true, __PRETTY_FUNCTION__);

                bool pk_is_uint64 = false;
                {
                    std::string handle_col_name = merge_tree->getData().getPrimarySortDescription()[0].column_name;
                    const auto pk_type = merge_tree->getColumns().getPhysical(handle_col_name).type->getFamilyName();

                    if (std::strcmp(pk_type, TypeName<UInt64>::get()) == 0)
                        pk_is_uint64 = true;
                }

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
                    new_region->compareAndCompleteSnapshot(handle_map, table_id, safe_point);
                }
                else
                    handle_map = getHandleMapByRange<Int64>(*context, *merge_tree, handle_range);
            }

            if (auto it = old_region_cache_data.find(table_id); it != old_region_cache_data.end())
            {
                for (const auto & [handle, ts, del] : it->second)
                {
                    const HandleMap::mapped_type cur_ele = {ts, del};
                    auto [it, ok] = handle_map.emplace(handle, cur_ele);
                    if (!ok)
                    {
                        auto & ele = it->second;
                        ele = std::max(ele, cur_ele);
                    }
                }
            }

            new_region->compareAndCompleteSnapshot(handle_map, table_id, safe_point);
        }
    }

    // context may be null in test cases.
    if (kvstore->onSnapshot(new_region, context ? &context->getTMTContext().region_table : nullptr, expect_old_index))
        LOG_INFO(log, "Region " << new_region->id() << " apply snapshot success");
}

} // namespace DB
