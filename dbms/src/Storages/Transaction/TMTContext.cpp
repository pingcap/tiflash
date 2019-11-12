#include <Common/DNSCache.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/RaftCommandResult.h>
#include <Storages/Transaction/RegionRangeKeys.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDBSchemaSyncer.h>
#include <pingcap/pd/MockPDClient.h>

namespace DB
{

TMTContext::TMTContext(Context & context, const std::vector<std::string> & addrs, const std::string & learner_key,
    const std::string & learner_value, const std::unordered_set<std::string> & ignore_databases_, const std::string & kvstore_path,
    const std::string & flash_service_address_,
    ::TiDB::StorageEngine engine_,
    bool disable_bg_flush_)
    : kvstore(std::make_shared<KVStore>(kvstore_path)),
      region_table(context),
      pd_client(addrs.size() == 0 ? static_cast<pingcap::pd::IClient *>(new pingcap::pd::MockPDClient())
                                  : static_cast<pingcap::pd::IClient *>(new pingcap::pd::Client(addrs))),
      region_cache(std::make_shared<pingcap::kv::RegionCache>(pd_client, learner_key, learner_value)),
      rpc_client(std::make_shared<pingcap::kv::RpcClient>()),
      ignore_databases(ignore_databases_),
      schema_syncer(addrs.size() == 0
              ? std::static_pointer_cast<SchemaSyncer>(std::make_shared<TiDBSchemaSyncer<true>>(pd_client, region_cache, rpc_client))
              : std::static_pointer_cast<SchemaSyncer>(std::make_shared<TiDBSchemaSyncer<false>>(pd_client, region_cache, rpc_client))),
      flash_service_address(flash_service_address_),
      engine(engine_),
      disable_bg_flush(disable_bg_flush_)
{}

void TMTContext::restore()
{
    kvstore->restore([&](pingcap::kv::RegionVerID id) -> IndexReaderPtr { return this->createIndexReader(id); });
    region_table.restore();
    initialized = true;
}

KVStorePtr & TMTContext::getKVStore() { return kvstore; }

const KVStorePtr & TMTContext::getKVStore() const { return kvstore; }

ManagedStorages & TMTContext::getStorages() { return storages; }

const ManagedStorages & TMTContext::getStorages() const { return storages; }

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

pingcap::pd::ClientPtr TMTContext::getPDClient() const { return pd_client; }

IndexReaderPtr TMTContext::createIndexReader(pingcap::kv::RegionVerID region_version_id) const
{
    std::lock_guard<std::mutex> lock(mutex);
    if (pd_client->isMock())
    {
        return nullptr;
    }
    // Assume net type of flash_service_address is AF_NET.
    auto socket_addr = DNSCache::instance().resolveHostAndPort(flash_service_address);
    std::string flash_service_ip = socket_addr.host().toString();
    UInt16 flash_service_port = socket_addr.port();
    if (flash_service_ip.empty())
    {
        throw Exception("Cannot resolve flash service address " + flash_service_address, ErrorCodes::LOGICAL_ERROR);
    }
    return std::make_shared<IndexReader>(region_cache, rpc_client, region_version_id, flash_service_ip, flash_service_port);
}

const std::unordered_set<std::string> & TMTContext::getIgnoreDatabases() const { return ignore_databases; }

} // namespace DB
