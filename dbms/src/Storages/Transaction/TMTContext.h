#pragma once

#include <Poco/Util/AbstractConfiguration.h>
#include <Storages/GCManager.h>
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

class MPPTaskManager;
using MPPTaskManagerPtr = std::shared_ptr<MPPTaskManager>;

class GCManager;
using GCManagerPtr = std::shared_ptr<GCManager>;

struct TiFlashRaftConfig;

class TMTContext : private boost::noncopyable
{
public:
    enum class StoreStatus : uint8_t
    {
        _MIN = 0,
        Idle,
        Ready,
        Running,
        Stopping,
        Terminated,
        _MAX,
    };

public:
    const KVStorePtr & getKVStore() const;
    KVStorePtr & getKVStore();

    const ManagedStorages & getStorages() const;
    ManagedStorages & getStorages();

    const RegionTable & getRegionTable() const;
    RegionTable & getRegionTable();

    const BackgroundService & getBackgroundService() const;
    BackgroundService & getBackgroundService();

    GCManager & getGCManager();

    Context & getContext();

    bool isBgFlushDisabled() const { return disable_bg_flush; }

    explicit TMTContext(Context & context_, const TiFlashRaftConfig & raft_config, const pingcap::ClusterConfig & cluster_config_);

    SchemaSyncerPtr getSchemaSyncer() const;
    void setSchemaSyncer(SchemaSyncerPtr);

    pingcap::pd::ClientPtr getPDClient() const;

    pingcap::kv::Cluster * getKVCluster() { return cluster.get(); }

    MPPTaskManagerPtr getMPPTaskManager();

    void restore(const TiFlashRaftProxyHelper * proxy_helper = nullptr);

    const std::unordered_set<std::string> & getIgnoreDatabases() const;

    ::TiDB::StorageEngine getEngineType() const { return engine; }

    void reloadConfig(const Poco::Util::AbstractConfiguration & config);

    bool isInitialized() const;
    StoreStatus getStoreStatus(std::memory_order = std::memory_order_seq_cst) const;
    void setStatusRunning();
    void setStatusStopping();
    void setStatusTerminated();
    bool checkShuttingDown(std::memory_order = std::memory_order_seq_cst) const;
    bool checkTerminated(std::memory_order = std::memory_order_seq_cst) const;
    bool checkRunning(std::memory_order = std::memory_order_seq_cst) const;

    const KVClusterPtr & getCluster() const { return cluster; }

    UInt64 replicaReadMaxThread() const;
    UInt64 batchReadIndexTimeout() const;
    Int64 waitRegionReadyTimeout() const;

private:
    Context & context;
    KVStorePtr kvstore;
    ManagedStorages storages;
    RegionTable region_table;
    BackGroundServicePtr background_service;
    GCManager gc_manager;

    KVClusterPtr cluster;

    mutable std::mutex mutex;

    std::atomic<StoreStatus> store_status{StoreStatus::Idle};

    const std::unordered_set<std::string> ignore_databases;
    SchemaSyncerPtr schema_syncer;
    MPPTaskManagerPtr mpp_task_manager;

    ::TiDB::StorageEngine engine;

    bool disable_bg_flush;

    std::atomic_uint64_t replica_read_max_thread;
    std::atomic_uint64_t batch_read_index_timeout_ms;
    std::atomic_int64_t wait_region_ready_timeout_sec;
};

const std::string & IntoStoreStatusName(TMTContext::StoreStatus status);

} // namespace DB
