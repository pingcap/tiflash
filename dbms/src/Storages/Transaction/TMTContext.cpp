#include <Interpreters/Context.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/TMTContext.h>
#include <pd/MockPDClient.h>

namespace DB
{

TMTContext::TMTContext(Context & context, std::vector<String> addrs)
    : kvstore(std::make_shared<KVStore>(context.getPath() + "kvstore/")),
      region_table(context, context.getPath() + "regmap/", std::bind(&KVStore::getRegion, kvstore.get(), std::placeholders::_1)),
      schema_syncer(std::make_shared<HttpJsonSchemaSyncer>()),
      pd_client(addrs.size() == 0 ? static_cast<pingcap::pd::IClient *>(new pingcap::pd::MockPDClient())
                                  : static_cast<pingcap::pd::IClient *>(new pingcap::pd::Client(addrs))),
      region_cache(std::make_shared<pingcap::kv::RegionCache>(pd_client)),
      rpc_client(std::make_shared<pingcap::kv::RpcClient>())
{
    kvstore->restore([&](pingcap::kv::RegionVerID id) -> pingcap::kv::RegionClientPtr {
            return this->createRegionClient(id);
        }, &regions_to_remove);
    for (RegionID id : regions_to_remove)
        kvstore->removeRegion(id, &context);
    regions_to_remove.clear();
}

SchemaSyncerPtr TMTContext::getSchemaSyncer() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return schema_syncer;
}

void TMTContext::setSchemaSyncer(SchemaSyncerPtr rhs)
{
    std::lock_guard<std::mutex> lock(mutex);
    schema_syncer = rhs;
}

pingcap::pd::ClientPtr TMTContext::getPDClient() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return pd_client;
}

void TMTContext::setPDClient(pingcap::pd::ClientPtr rhs)
{
    std::lock_guard<std::mutex> lock(mutex);
    pd_client = rhs;
}

pingcap::kv::RegionClientPtr TMTContext::createRegionClient(pingcap::kv::RegionVerID region_version_id) const
{
    std::lock_guard<std::mutex> lock(mutex);
    return pd_client->isMock() ? nullptr : std::make_shared<pingcap::kv::RegionClient>(region_cache, rpc_client, region_version_id);
}

pingcap::kv::RegionCachePtr TMTContext::getRegionCache() const { return region_cache; }

pingcap::kv::RpcClientPtr TMTContext::getRpcClient()
{
    return rpc_client;
}

} // namespace DB
