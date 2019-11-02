#pragma once

#include <Storages/Transaction/PDTiKVClient.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTStorages.h>

#include <unordered_set>

namespace DB
{

class Context;

class KVStore;
using KVStorePtr = std::shared_ptr<KVStore>;

class SchemaSyncer;
using SchemaSyncerPtr = std::shared_ptr<SchemaSyncer>;

class TMTContext : private boost::noncopyable
{
public:
    const KVStorePtr & getKVStore() const;
    KVStorePtr & getKVStore();

    const TMTStorages & getStorages() const;
    TMTStorages & getStorages();

    const RegionTable & getRegionTable() const;
    RegionTable & getRegionTable();

    bool isInitialized() const;

    // TODO: get flusher args from config file
    explicit TMTContext(Context & context, const std::vector<std::string> & addrs, const std::string & learner_key,
        const std::string & learner_value, const std::unordered_set<std::string> & ignore_databases_, const std::string & kv_store_path,
        const std::string & flash_service_address_);

    SchemaSyncerPtr getSchemaSyncer() const;
    void setSchemaSyncer(SchemaSyncerPtr);

    pingcap::pd::ClientPtr getPDClient() const;

    IndexReaderPtr createIndexReader(pingcap::kv::RegionVerID region_version_id) const;

    void restore();

    const std::unordered_set<std::string> & getIgnoreDatabases() const;

private:
    KVStorePtr kvstore;
    TMTStorages storages;
    RegionTable region_table;

private:
    pingcap::pd::ClientPtr pd_client;
    pingcap::kv::RegionCachePtr region_cache;
    pingcap::kv::RpcClientPtr rpc_client;

    mutable std::mutex mutex;
    std::atomic_bool initialized = false;

    const std::unordered_set<std::string> ignore_databases;
    SchemaSyncerPtr schema_syncer;

    String flash_service_address;
};

} // namespace DB
