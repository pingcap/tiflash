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

    const ManagedStorages & getStorages() const;
    ManagedStorages & getStorages();

    const RegionTable & getRegionTable() const;
    RegionTable & getRegionTable();

    bool isInitialized() const;

    // TODO: get flusher args from config file
    explicit TMTContext(Context & context, const std::vector<std::string> & addrs, const std::string & learner_key,
        const std::string & learner_value, const std::unordered_set<std::string> & ignore_databases_, const std::string & kv_store_path,
        const std::string & regine_addr,
        TiDB::StorageEngine engine_);

    SchemaSyncerPtr getSchemaSyncer() const;
    void setSchemaSyncer(SchemaSyncerPtr);

    pingcap::pd::ClientPtr getPDClient() const;

    IndexReaderPtr createIndexReader(pingcap::kv::RegionVerID region_version_id) const;

    void restore();

    const std::unordered_set<std::string> & getIgnoreDatabases() const;

    ::TiDB::StorageEngine getEngineType() const { return engine; }

private:
    KVStorePtr kvstore;
    ManagedStorages storages;
    RegionTable region_table;

private:
    pingcap::pd::ClientPtr pd_client;
    pingcap::kv::RegionCachePtr region_cache;
    pingcap::kv::RpcClientPtr rpc_client;

    mutable std::mutex mutex;
    std::atomic_bool initialized = false;

    const std::unordered_set<std::string> ignore_databases;
    SchemaSyncerPtr schema_syncer;

    String raft_service_address;
    ::TiDB::StorageEngine engine;
};

} // namespace DB
