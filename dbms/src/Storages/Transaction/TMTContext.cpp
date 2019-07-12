#include <Interpreters/Context.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TiDBSchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <pd/MockPDClient.h>

namespace DB
{

const static std::string kvstore_name = "kvstore/";
const static std::string region_table_name = "regmap/";

TMTContext::TMTContext(
    Context & context, const std::vector<std::string> & addrs, const std::string & learner_key, const std::string & learner_value)
    : kvstore(std::make_shared<KVStore>(context.getPath() + kvstore_name)),
      region_table(context, context.getPath() + region_table_name),
      pd_client(addrs.size() == 0 ? static_cast<pingcap::pd::IClient *>(new pingcap::pd::MockPDClient())
                                  : static_cast<pingcap::pd::IClient *>(new pingcap::pd::Client(addrs))),
      region_cache(std::make_shared<pingcap::kv::RegionCache>(pd_client, learner_key, learner_value)),
      rpc_client(std::make_shared<pingcap::kv::RpcClient>()),
      schema_syncer(std::make_shared<TiDBSchemaSyncer>(pd_client, region_cache, rpc_client))
{}

void TMTContext::restore()
{
    std::vector<RegionID> regions_to_remove;

    kvstore->restore(
        [&](pingcap::kv::RegionVerID id) -> pingcap::kv::RegionClientPtr { return this->createRegionClient(id); }, &regions_to_remove);
    region_table.restore(std::bind(&KVStore::getRegion, kvstore.get(), std::placeholders::_1));
    for (RegionID id : regions_to_remove)
        kvstore->removeRegion(id, &region_table);

    kvstore->updateRegionTableBySnapshot(region_table);
    initialized = true;
}

KVStorePtr & TMTContext::getKVStore() { return kvstore; }

const KVStorePtr & TMTContext::getKVStore() const { return kvstore; }

TMTStorages & TMTContext::getStorages() { return storages; }

const TMTStorages & TMTContext::getStorages() const { return storages; }

RegionTable & TMTContext::getRegionTable() { return region_table; }

const RegionTable & TMTContext::getRegionTable() const { return region_table; }

bool TMTContext::isInitialized() const { return initialized; }

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

pingcap::kv::RpcClientPtr TMTContext::getRpcClient() { return rpc_client; }

} // namespace DB
