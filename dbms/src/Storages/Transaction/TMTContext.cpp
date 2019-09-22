#include <Interpreters/Context.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/RaftCommandResult.h>
#include <Storages/Transaction/RegionRangeKeys.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDBSchemaSyncer.h>
#include <pd/MockPDClient.h>

namespace DB
{

TMTContext::TMTContext(Context & context, const std::vector<std::string> & addrs, const std::string & learner_key,
    const std::string & learner_value, const std::unordered_set<std::string> & ignore_databases_, const std::string & kvstore_path)
    : kvstore(std::make_shared<KVStore>(kvstore_path)),
      region_table(context),
      pd_client(addrs.size() == 0 ? static_cast<pingcap::pd::IClient *>(new pingcap::pd::MockPDClient())
                                  : static_cast<pingcap::pd::IClient *>(new pingcap::pd::Client(addrs))),
      region_cache(std::make_shared<pingcap::kv::RegionCache>(pd_client, learner_key, learner_value)),
      rpc_client(std::make_shared<pingcap::kv::RpcClient>()),
      ignore_databases(ignore_databases_),
      schema_syncer(addrs.size() == 0
              ? std::static_pointer_cast<SchemaSyncer>(std::make_shared<TiDBSchemaSyncer<true>>(pd_client, region_cache, rpc_client))
              : std::static_pointer_cast<SchemaSyncer>(std::make_shared<TiDBSchemaSyncer<false>>(pd_client, region_cache, rpc_client)))
{}

void TMTContext::restore()
{
    kvstore->restore([&](pingcap::kv::RegionVerID id) -> pingcap::kv::RegionClientPtr { return this->createRegionClient(id); });
    region_table.restore();
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

const std::unordered_set<std::string> & TMTContext::getIgnoreDatabases() const { return ignore_databases; }

} // namespace DB
