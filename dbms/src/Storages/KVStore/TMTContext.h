// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Interpreters/Context_fwd.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Server/RaftConfigParser.h>
#include <Storages/GCManager.h>
#include <Storages/KVStore/Decode/RegionTable.h>
#include <Storages/KVStore/StorageEngineType.h>
#include <Storages/KVStore/TMTStorages.h>
#include <Storages/KVStore/TiKVHelpers/PDTiKVClient.h>

namespace DB
{
class PathPool;

class KVStore;
using KVStorePtr = std::shared_ptr<KVStore>;

class TiDBSchemaSyncerManager;

class BackgroundService;
using BackGroundServicePtr = std::unique_ptr<BackgroundService>;

class MPPTaskManager;
using MPPTaskManagerPtr = std::shared_ptr<MPPTaskManager>;

class GCManager;
using GCManagerPtr = std::shared_ptr<GCManager>;

struct TiFlashRaftConfig;

struct TiFlashRaftProxyHelper;

// We define a shared ptr here, because TMTContext / SchemaSyncer / IndexReader all need to
// `share` the resource of cluster.
using KVClusterPtr = std::shared_ptr<pingcap::kv::Cluster>;

namespace Etcd
{
class Client;
using ClientPtr = std::shared_ptr<Client>;
} // namespace Etcd
class OwnerManager;
using OwnerManagerPtr = std::shared_ptr<OwnerManager>;
namespace S3
{
class IS3LockClient;
using S3LockClientPtr = std::shared_ptr<IS3LockClient>;
class S3GCManagerService;
using S3GCManagerServicePtr = std::unique_ptr<S3GCManagerService>;
} // namespace S3

class TMTContext : private boost::noncopyable
{
public:
    enum class StoreStatus : uint8_t
    {
        _MIN = 0, // NOLINT(bugprone-reserved-identifier)
        Idle,
        Ready,
        Running,
        Stopping,
        Terminated,
        _MAX, // NOLINT(bugprone-reserved-identifier)
    };

public:
    const KVStorePtr & getKVStore() const;
    KVStorePtr & getKVStore();
    void debugSetKVStore(const KVStorePtr &);

    const ManagedStorages & getStorages() const;
    ManagedStorages & getStorages();

    const RegionTable & getRegionTable() const;
    RegionTable & getRegionTable();

    const BackgroundService & getBackgroundService() const;
    BackgroundService & getBackgroundService();

    GCManager & getGCManager();

    Context & getContext();

    const Context & getContext() const;

    explicit TMTContext(
        Context & context_,
        const TiFlashRaftConfig & raft_config,
        const pingcap::ClusterConfig & cluster_config_);
    ~TMTContext();

    std::shared_ptr<TiDBSchemaSyncerManager> getSchemaSyncerManager() const;

    void updateSecurityConfig(const TiFlashRaftConfig & raft_config, const pingcap::ClusterConfig & cluster_config);

    pingcap::pd::ClientPtr getPDClient() const;

    pingcap::kv::Cluster * getKVCluster() { return cluster.get(); }

    const OwnerManagerPtr & getS3GCOwnerManager() const;

    const S3::S3GCManagerServicePtr & getS3GCManager() const { return s3gc_manager; }

    S3::S3LockClientPtr getS3LockClient() const { return s3lock_client; }

    MPPTaskManagerPtr getMPPTaskManager();

    void shutdown();

    void restore(PathPool & path_pool, const TiFlashRaftProxyHelper * proxy_helper = nullptr);

    const std::unordered_set<std::string> & getIgnoreDatabases() const;

    void reloadConfig(const Poco::Util::AbstractConfiguration & config);

    bool isInitialized() const;
    StoreStatus getStoreStatus(std::memory_order = std::memory_order_seq_cst) const;
    void setStatusRunning();
    void setStatusStopping();
    void setStatusTerminated();
    bool checkShuttingDown(std::memory_order = std::memory_order_seq_cst) const;
    bool checkTerminated(std::memory_order = std::memory_order_seq_cst) const;
    bool checkRunning(std::memory_order = std::memory_order_seq_cst) const;

    UInt64 batchReadIndexTimeout() const;
    // timeout for wait index (ms). "0" means wait infinitely
    UInt64 waitIndexTimeout() const;
    void debugSetWaitIndexTimeout(UInt64 timeout);
    Int64 waitRegionReadyTimeout() const;
    uint64_t readIndexWorkerTick() const;

    Etcd::ClientPtr getEtcdClient() const { return etcd_client; }
    void initS3GCManager(const TiFlashRaftProxyHelper * proxy_helper);

private:
    Context & context;
    KVStorePtr kvstore;
    ManagedStorages storages;
    RegionTable region_table;
    BackGroundServicePtr background_service;
    GCManager gc_manager;

    KVClusterPtr cluster;
    Etcd::ClientPtr etcd_client;

    OwnerManagerPtr s3gc_owner;
    S3::S3LockClientPtr s3lock_client;
    S3::S3GCManagerServicePtr s3gc_manager;

    mutable std::mutex mutex;

    std::atomic<StoreStatus> store_status{StoreStatus::Idle};

    const std::unordered_set<std::string> ignore_databases;
    std::shared_ptr<TiDBSchemaSyncerManager> schema_sync_manager;
    MPPTaskManagerPtr mpp_task_manager;

    std::atomic_uint64_t batch_read_index_timeout_ms;
    std::atomic_uint64_t wait_index_timeout_ms;
    std::atomic_uint64_t read_index_worker_tick_ms;
    std::atomic_int64_t wait_region_ready_timeout_sec;

    TiFlashRaftConfig raftproxy_config;
};

const std::string & IntoStoreStatusName(TMTContext::StoreStatus status);

} // namespace DB
