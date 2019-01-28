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
    if (context) {
        auto & tmt_ctx = context->getTMTContext();
        auto pd_client = tmt_ctx.getPDClient();
        if (!pd_client->isMock()) {
            auto region_cache = tmt_ctx.getRegionCache();
            region_client = std::make_shared<pingcap::kv::RegionClient>(region_cache, tmt_ctx.getRpcClient(), meta.getRegionVerID());
        }
    }
    auto region = std::make_shared<Region>(meta, region_client);

    LOG_INFO(log, "Region " << region->id() << " apply snapshot " << region->toString(true));

    while (read(&request))
    {
        if (!request.has_data())
            throw Exception("Failed to read snapshot data", ErrorCodes::LOGICAL_ERROR);
        const auto & data = request.data();
        data.cf();
        for (const auto & kv : data.data())
            region->insert(data.cf(), TiKVKey{kv.key()}, TiKVValue{kv.value()});
    }

    // context may be null in test cases.
    if (context)
        kvstore->onSnapshot(region, context);
    else
        kvstore->onSnapshot(region, nullptr);

    LOG_INFO(log, "Region " << region->id() << " apply snapshot done.");
}

} // namespace DB
