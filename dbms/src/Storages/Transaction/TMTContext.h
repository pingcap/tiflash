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
private:
    KVStorePtr kvstore;
    TMTStorages storages;
    RegionTable region_table;

public:
    const KVStorePtr & getKVStore() const;
    KVStorePtr & getKVStoreMut();

    const TMTStorages & getStorages() const;
    TMTStorages & getStoragesMut();

    const RegionTable & getRegionTable() const;
    RegionTable & getRegionTableMut();

    bool isInitialized() const;

    // TODO: get flusher args from config file
    explicit TMTContext(Context & context_, std::vector<String> addrs, std::string learner_key_, std::string learner_value_);

    SchemaSyncerPtr getSchemaSyncer() const;
    void setSchemaSyncer(SchemaSyncerPtr);

    pingcap::pd::ClientPtr getPDClient() const;
    void setPDClient(pingcap::pd::ClientPtr);

    pingcap::kv::RegionClientPtr createRegionClient(pingcap::kv::RegionVerID region_version_id) const;

    pingcap::kv::RegionCachePtr getRegionCache() const;

    pingcap::kv::RpcClientPtr getRpcClient();

    void restore();

private:
    SchemaSyncerPtr schema_syncer;
    pingcap::pd::ClientPtr pd_client;
    pingcap::kv::RegionCachePtr region_cache;
    pingcap::kv::RpcClientPtr rpc_client;

    mutable std::mutex mutex;
    std::atomic_bool initialized = false;
};

} // namespace DB
