#pragma once

#include <Poco/Util/AbstractConfiguration.h>
#include <Storages/Transaction/PDTiKVClient.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/StorageEngineType.h>
#include <Storages/Transaction/TMTStorages.h>

namespace DB
{

class Context;

class KVStore;
using KVStorePtr = std::shared_ptr<KVStore>;

class SchemaSyncer;
using SchemaSyncerPtr = std::shared_ptr<SchemaSyncer>;

class BackgroundService;
using BackGroundServicePtr = std::unique_ptr<BackgroundService>;

class TMTContext : private boost::noncopyable
{
public:
    const KVStorePtr & getKVStore() const;
    KVStorePtr & getKVStore();

    const ManagedStorages & getStorages() const;
    ManagedStorages & getStorages();

    const RegionTable & getRegionTable() const;
    RegionTable & getRegionTable();

    const BackgroundService & getBackgroundService() const;
    BackgroundService & getBackgroundService();

    Context & getContext();
    bool isInitialized() const;

    bool isBgFlushDisabled() const { return disable_bg_flush; }

    // TODO: get flusher args from config file
    explicit TMTContext(Context & context, const std::vector<std::string> & addrs, const std::string & learner_key,
        const std::string & learner_value, const std::unordered_set<std::string> & ignore_databases_, const std::string & kv_store_path,
        TiDB::StorageEngine engine_, bool disable_bg_flush_, const grpc::SslCredentialsOptions & cred_options);

    SchemaSyncerPtr getSchemaSyncer() const;
    void setSchemaSyncer(SchemaSyncerPtr);

    pingcap::pd::ClientPtr getPDClient() const;

    pingcap::kv::Cluster * getKVCluster() {return cluster.get();}

    IndexReaderPtr createIndexReader() const;

    void restore();

    const std::unordered_set<std::string> & getIgnoreDatabases() const;

    ::TiDB::StorageEngine getEngineType() const { return engine; }

    void reloadConfig(const Poco::Util::AbstractConfiguration & config);

    const std::atomic_bool & getTerminated() const;
    void setTerminated();

    const KVClusterPtr & getCluster() const { return cluster; }

private:
    Context & context;
    KVStorePtr kvstore;
    ManagedStorages storages;
    RegionTable region_table;
    BackGroundServicePtr background_service;

private:
    KVClusterPtr cluster;

    mutable std::mutex mutex;
    std::atomic_bool initialized = false;

    const std::unordered_set<std::string> ignore_databases;
    SchemaSyncerPtr schema_syncer;

    ::TiDB::StorageEngine engine;

    bool disable_bg_flush;

    std::atomic_bool terminated{false};
};

} // namespace DB
