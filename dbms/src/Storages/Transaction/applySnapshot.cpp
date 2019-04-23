#include <Interpreters/Context.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
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
    if (!ok || !request.has_state())
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
    auto region = std::make_shared<Region>(meta, region_client_create);

    LOG_INFO(log, "Region " << region->id() << " apply snapshot " << region->toString(true));

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
            region->batchInsert([&](Region::BatchInsertNode & node) -> bool {
                if (it == cf_data.end())
                    return false;
                key = TiKVKey(it->key());
                value = TiKVValue(it->value());
                node = Region::BatchInsertNode(&key, &value, &cf_name);
                ++it;
                return true;
            });
        }
    }

    {
        if (region->isPeerRemoved())
            region->setPendingRemove();
    }

    // context may be null in test cases.
    kvstore->onSnapshot(region, context ? &context->getTMTContext().region_table : nullptr);

    LOG_INFO(log, "Region " << region->id() << " apply snapshot done.");
}

} // namespace DB
