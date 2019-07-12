#pragma once

#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTStorages.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tikv/RegionClient.h>
#pragma GCC diagnostic pop

namespace DB
{

class Context;

class KVStore;
using KVStorePtr = std::shared_ptr<KVStore>;

class SchemaSyncer;
using SchemaSyncerPtr = std::shared_ptr<SchemaSyncer>;

class TMTContext
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
        const std::string & learner_value, const std::string & kv_store_path, const std::string & region_mapping_path);

    SchemaSyncerPtr getSchemaSyncer() const;
    void setSchemaSyncer(SchemaSyncerPtr);

    pingcap::pd::ClientPtr getPDClient() const;
    void setPDClient(pingcap::pd::ClientPtr);

    pingcap::kv::RegionClientPtr createRegionClient(pingcap::kv::RegionVerID region_version_id) const;

    pingcap::kv::RegionCachePtr getRegionCache() const;

    pingcap::kv::RpcClientPtr getRpcClient();

    void restore();

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

    SchemaSyncerPtr schema_syncer;
};

} // namespace DB
