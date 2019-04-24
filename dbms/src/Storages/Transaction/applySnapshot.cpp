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
    // TODO REVIEW: use two 'throw' here, 'not ok' and 'no state'
    if (!ok || !request.has_state())
        throw Exception("Failed to read snapshot state", ErrorCodes::LOGICAL_ERROR);

    const auto & state = request.state();
    pingcap::kv::RegionClientPtr region_client = nullptr;
    auto meta = RegionMeta(state.peer(), state.region(), state.apply_state());

    Region::RegionClientCreateFunc region_client_create = [&](pingcap::kv::RegionVerID id) -> pingcap::kv::RegionClientPtr {
        // context may be null in test cases.
        if (context)
        {
            auto & tmt_ctx = context->getTMTContext();
            return tmt_ctx.createRegionClient(id);
        }
        return nullptr;
    };
    auto region = std::make_shared<Region>(meta, region_client_create);

    LOG_INFO(log, "Region " << region->id() << " apply snapshot " << region->toString(true));

    // TODO REVIEW: this snapshot may be a big one, in that case, we should not hold all data of this region in memory
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
            // - REVIEW: the batch inserting logic is OK, but the calling stack is weird.
            //   May be we should just do lock action on each node-inserting, it's also fast when lock contention is low.
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
    kvstore->onSnapshot(region, context);

    LOG_INFO(log, "Region " << region->id() << " apply snapshot done.");
}

} // namespace DB
