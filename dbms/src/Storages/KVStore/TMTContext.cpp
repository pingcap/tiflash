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

#include <Flash/Disaggregated/S3LockClient.h>
#include <Flash/Mpp/MPPHandler.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Mpp/MinTSOScheduler.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Server/RaftConfigParser.h>
#include <Storages/KVStore/BackgroundService.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/RegionExecutionResult.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3GCManager.h>
#include <TiDB/Etcd/Client.h>
#include <TiDB/OwnerInfo.h>
#include <TiDB/OwnerManager.h>
#include <TiDB/Schema/SchemaSyncer.h>
#include <TiDB/Schema/TiDBSchemaManager.h>
#include <TiDB/Schema/TiDBSchemaSyncer.h>
#include <common/logger_useful.h>
#include <pingcap/pd/MockPDClient.h>

#include <magic_enum.hpp>
#include <memory>

namespace DB
{
// default batch-read-index timeout is 10_000ms.
extern const uint64_t DEFAULT_BATCH_READ_INDEX_TIMEOUT_MS = 10 * 1000;
// default wait-index timeout is 5 * 60_000ms.
extern const uint64_t DEFAULT_WAIT_INDEX_TIMEOUT_MS = 5 * 60 * 1000;

const int64_t DEFAULT_WAIT_REGION_READY_TIMEOUT_SEC = 20 * 60;

const int64_t DEFAULT_READ_INDEX_WORKER_TICK_MS = 10;

namespace
{
std::shared_ptr<TiDBSchemaSyncerManager> createSchemaSyncer(
    bool exist_pd_addr,
    bool for_unit_test,
    const KVClusterPtr & cluster,
    bool disaggregated_compute_mode)
{
    // Doesn't need SchemaSyncer for tiflash_compute mode.
    if (disaggregated_compute_mode)
        return nullptr;
    if (exist_pd_addr)
    {
        // product env
        // Get DBInfo/TableInfo from TiKV, and create table with names `t_${table_id}`
        return std::make_shared<TiDBSchemaSyncerManager>(cluster, /*mock_getter*/ false, /*mock_mapper*/ false);
    }
    else if (!for_unit_test)
    {
        // mock test
        // Get DBInfo/TableInfo from MockTiDB, and create table with its display names
        return std::make_shared<TiDBSchemaSyncerManager>(cluster, /*mock_getter*/ true, /*mock_mapper*/ true);
    }
    // unit test.
    // Get DBInfo/TableInfo from MockTiDB, but create table with names `t_${table_id}`
    return std::make_shared<TiDBSchemaSyncerManager>(cluster, /*mock_getter*/ true, /*mock_mapper*/ false);
}

// Print log for MPPTask which hasn't been removed for over 25 minutes.
void checkLongLiveMPPTasks(
    const std::unordered_map<String, Stopwatch> & monitored_tasks,
    bool report_metrics,
    const LoggerPtr & log)
{
    String log_info;
    double longest_live_time = 0;
    for (const auto & iter : monitored_tasks)
    {
        auto alive_time = iter.second.elapsedSeconds();
        if (alive_time > longest_live_time)
            longest_live_time = alive_time;
        if (alive_time >= 1500)
            log_info = fmt::format("{} <MPPTask is alive for {} secs, {}>", log_info, alive_time, iter.first);
    }

    if (!log_info.empty())
        LOG_WARNING(log, log_info);
    if (report_metrics)
        GET_METRIC(tiflash_mpp_task_monitor, type_longest_live_time).Set(longest_live_time);
}

void monitorMPPTasks(std::shared_ptr<MPPTaskMonitor> monitor)
{
    std::unique_lock lock(monitor->mu, std::defer_lock);
    while (true)
    {
        lock.lock();

        // Check MPPTasks every 25 minutes
        monitor->cv.wait_for(lock, std::chrono::seconds(1500));

        auto snapshot = monitor->monitored_tasks;
        if (monitor->is_shutdown)
        {
            lock.unlock();
            // When shutting down, the `TiFlashMetrics` instance maybe release, don't
            // report metrics at this time
            checkLongLiveMPPTasks(snapshot, /* report_metrics */ false, monitor->log);
            return;
        }

        lock.unlock();
        checkLongLiveMPPTasks(snapshot, /* report_metrics */ true, monitor->log);
    }
}

void startMonitorMPPTaskThread(const MPPTaskManagerPtr & manager)
{
    newThreadManager()->scheduleThenDetach(false, "MPPTask-Moniter", [monitor = manager->getMPPTaskMonitor()] {
        monitorMPPTasks(monitor);
    });
}
} // namespace

TMTContext::TMTContext(
    Context & context_,
    const TiFlashRaftConfig & raft_config,
    const pingcap::ClusterConfig & cluster_config)
    : context(context_)
    , kvstore(
          context_.getSharedContextDisagg()->isDisaggregatedComputeMode()
                  && context_.getSharedContextDisagg()->use_autoscaler
              ? nullptr
              : std::make_shared<KVStore>(context))
    , region_table(context)
    , background_service(nullptr)
    , gc_manager(context)
    , cluster(
          raft_config.pd_addrs.empty() ? std::make_shared<pingcap::kv::Cluster>()
                                       : std::make_shared<pingcap::kv::Cluster>(raft_config.pd_addrs, cluster_config))
    , ignore_databases(raft_config.ignore_databases)
    , schema_sync_manager(createSchemaSyncer(
          !raft_config.pd_addrs.empty(),
          raft_config.for_unit_test,
          cluster,
          context_.getSharedContextDisagg()->isDisaggregatedComputeMode()))
    , mpp_task_manager(std::make_shared<MPPTaskManager>(std::make_unique<MinTSOScheduler>(
          context.getSettingsRef().task_scheduler_thread_soft_limit,
          context.getSettingsRef().task_scheduler_thread_hard_limit,
          context.getSettingsRef().task_scheduler_active_set_soft_limit)))
    , engine(raft_config.engine)
    , batch_read_index_timeout_ms(DEFAULT_BATCH_READ_INDEX_TIMEOUT_MS)
    , wait_index_timeout_ms(DEFAULT_WAIT_INDEX_TIMEOUT_MS)
    , read_index_worker_tick_ms(DEFAULT_READ_INDEX_WORKER_TICK_MS)
    , wait_region_ready_timeout_sec(DEFAULT_WAIT_REGION_READY_TIMEOUT_SEC)
{
    startMonitorMPPTaskThread(mpp_task_manager);

    etcd_client = Etcd::Client::create(cluster->pd_client, cluster_config);
    if (!raft_config.pd_addrs.empty() && S3::ClientFactory::instance().isEnabled()
        && !context.getSharedContextDisagg()->isDisaggregatedComputeMode())
    {
        s3gc_owner = OwnerManager::createS3GCOwner(context, /*id*/ raft_config.advertise_engine_addr, etcd_client);
        s3gc_owner->campaignOwner(); // start campaign
        s3lock_client = std::make_shared<S3::S3LockClient>(cluster.get(), s3gc_owner);

        S3::S3GCConfig remote_gc_config;
        {
            Int64 gc_method_int = context.getSettingsRef().remote_gc_method;
            if (gc_method_int == 1)
            {
                remote_gc_config.method = S3::S3GCMethod::Lifecycle;
                LOG_INFO(Logger::get(), "Using remote_gc_method={}", magic_enum::enum_name(remote_gc_config.method));
            }
            else if (gc_method_int == 2)
            {
                remote_gc_config.method = S3::S3GCMethod::ScanThenDelete;
                LOG_INFO(Logger::get(), "Using remote_gc_method={}", magic_enum::enum_name(remote_gc_config.method));
            }
            else
            {
                LOG_WARNING(
                    Logger::get(),
                    "Unknown remote gc method from settings, using default method, value={} remote_gc_method={}",
                    gc_method_int,
                    magic_enum::enum_name(remote_gc_config.method));
            }
        }
        // TODO: make it reloadable
        remote_gc_config.interval_seconds = context.getSettingsRef().remote_gc_interval_seconds;
        remote_gc_config.verify_locks = context.getSettingsRef().remote_gc_verify_consistency > 0;
        // set the gc_method so that S3LockService can set tagging when create delmark
        S3::ClientFactory::instance().gc_method = remote_gc_config.method;
        s3gc_manager = std::make_unique<S3::S3GCManagerService>(
            context,
            cluster->pd_client,
            s3gc_owner,
            s3lock_client,
            remote_gc_config);
    }
}

TMTContext::~TMTContext() = default;

void TMTContext::updateSecurityConfig(
    const TiFlashRaftConfig & raft_config,
    const pingcap::ClusterConfig & cluster_config)
{
    if (!raft_config.pd_addrs.empty())
    {
        // update the client config including pd_client
        cluster->update(raft_config.pd_addrs, cluster_config);
        if (etcd_client)
        {
            // update the etcd_client after pd_client get updated
            etcd_client->update(cluster_config);
        }
    }
}

void TMTContext::restore(PathPool & path_pool, const TiFlashRaftProxyHelper * proxy_helper)
{
    // For tiflash_compute mode, kvstore should be nullptr, no need to restore region_table.
    if (context.getSharedContextDisagg()->isDisaggregatedComputeMode()
        && context.getSharedContextDisagg()->use_autoscaler)
        return;

    kvstore->restore(path_pool, proxy_helper);
    region_table.restore();
    store_status = StoreStatus::Ready;

    if (proxy_helper != nullptr)
    {
        // Only create when running with Raft threads
        background_service = std::make_unique<BackgroundService>(*this);
    }
}

void TMTContext::shutdown()
{
    if (mpp_task_manager)
    {
        // notify end to the thread "MPPTask-Moniter"
        mpp_task_manager->shutdown();
    }

    if (s3gc_owner)
    {
        // stop the campaign loop, so the S3LockService will
        // let client retry
        s3gc_owner->cancel();
        s3gc_owner = nullptr;
    }

    if (s3gc_manager)
    {
        s3gc_manager->shutdown();
        s3gc_manager = nullptr;
    }

    if (s3lock_client)
    {
        s3lock_client = nullptr;
    }

    if (background_service)
    {
        background_service->shutdown();
        background_service = nullptr;
    }
}

KVStorePtr & TMTContext::getKVStore()
{
    return kvstore;
}

const KVStorePtr & TMTContext::getKVStore() const
{
    return kvstore;
}

ManagedStorages & TMTContext::getStorages()
{
    return storages;
}

const ManagedStorages & TMTContext::getStorages() const
{
    return storages;
}

RegionTable & TMTContext::getRegionTable()
{
    return region_table;
}

const RegionTable & TMTContext::getRegionTable() const
{
    return region_table;
}

BackgroundService & TMTContext::getBackgroundService()
{
    return *background_service;
}

const BackgroundService & TMTContext::getBackgroundService() const
{
    return *background_service;
}

GCManager & TMTContext::getGCManager()
{
    return gc_manager;
}

Context & TMTContext::getContext()
{
    return context;
}

const Context & TMTContext::getContext() const
{
    return context;
}

bool TMTContext::isInitialized() const
{
    return getStoreStatus() != StoreStatus::Idle;
}

void TMTContext::setStatusRunning()
{
    store_status = StoreStatus::Running;
}

TMTContext::StoreStatus TMTContext::getStoreStatus(std::memory_order memory_order) const
{
    return store_status.load(memory_order);
}

std::shared_ptr<TiDBSchemaSyncerManager> TMTContext::getSchemaSyncerManager() const
{
    std::lock_guard lock(mutex);
    return schema_sync_manager;
}

pingcap::pd::ClientPtr TMTContext::getPDClient() const
{
    return cluster->pd_client;
}

const OwnerManagerPtr & TMTContext::getS3GCOwnerManager() const
{
    return s3gc_owner;
}

MPPTaskManagerPtr TMTContext::getMPPTaskManager()
{
    return mpp_task_manager;
}

const std::unordered_set<std::string> & TMTContext::getIgnoreDatabases() const
{
    return ignore_databases;
}

void TMTContext::reloadConfig(const Poco::Util::AbstractConfiguration & config)
{
    if (context.getSharedContextDisagg()->isDisaggregatedComputeMode()
        && context.getSharedContextDisagg()->use_autoscaler)
        return;

    // static constexpr const char * COMPACT_LOG_MIN_PERIOD = "flash.compact_log_min_period"; // disabled
    static constexpr const char * COMPACT_LOG_MIN_ROWS = "flash.compact_log_min_rows";
    static constexpr const char * COMPACT_LOG_MIN_BYTES = "flash.compact_log_min_bytes";
    static constexpr const char * COMPACT_LOG_MIN_GAP = "flash.compact_log_min_gap";
    static constexpr const char * EAGER_GC_LOG_GAP = "flash.eager_gc_log_gap";
    static constexpr const char * BATCH_READ_INDEX_TIMEOUT_MS = "flash.batch_read_index_timeout_ms";
    static constexpr const char * WAIT_INDEX_TIMEOUT_MS = "flash.wait_index_timeout_ms";
    static constexpr const char * WAIT_REGION_READY_TIMEOUT_SEC = "flash.wait_region_ready_timeout_sec";
    static constexpr const char * READ_INDEX_WORKER_TICK_MS = "flash.read_index_worker_tick_ms";

    // default config about compact-log: period 120s, rows 40k, bytes 32MB.
    getKVStore()->setRegionCompactLogConfig(
        std::max(config.getUInt64(COMPACT_LOG_MIN_ROWS, 40 * 1024), 1),
        std::max(config.getUInt64(COMPACT_LOG_MIN_BYTES, 32 * 1024 * 1024), 1),
        std::max(config.getUInt64(COMPACT_LOG_MIN_GAP, 200), 1),
        config.getUInt64(EAGER_GC_LOG_GAP, 512));
    {
        batch_read_index_timeout_ms
            = config.getUInt64(BATCH_READ_INDEX_TIMEOUT_MS, DEFAULT_BATCH_READ_INDEX_TIMEOUT_MS);
        wait_index_timeout_ms = config.getUInt64(WAIT_INDEX_TIMEOUT_MS, DEFAULT_WAIT_INDEX_TIMEOUT_MS);
        wait_region_ready_timeout_sec = ({
            int64_t t = config.getInt64(WAIT_REGION_READY_TIMEOUT_SEC, /*20min*/ DEFAULT_WAIT_REGION_READY_TIMEOUT_SEC);
            t = t >= 0 ? t : std::numeric_limits<int64_t>::max(); // set -1 to wait infinitely
            t;
        });
        read_index_worker_tick_ms = config.getUInt64(READ_INDEX_WORKER_TICK_MS, DEFAULT_READ_INDEX_WORKER_TICK_MS);
    }
    {
        LOG_INFO(
            Logger::get(),
            "read-index timeout: {}ms; wait-index timeout: {}ms; wait-region-ready timeout: {}s; "
            "read-index-worker-tick: {}ms",
            batchReadIndexTimeout(),
            waitIndexTimeout(),
            waitRegionReadyTimeout(),
            readIndexWorkerTick());
    }
}

bool TMTContext::checkShuttingDown(std::memory_order memory_order) const
{
    return getStoreStatus(memory_order) >= StoreStatus::Stopping;
}
bool TMTContext::checkTerminated(std::memory_order memory_order) const
{
    return getStoreStatus(memory_order) == StoreStatus::Terminated;
}
bool TMTContext::checkRunning(std::memory_order memory_order) const
{
    return getStoreStatus(memory_order) == StoreStatus::Running;
}

void TMTContext::setStatusStopping()
{
    store_status = StoreStatus::Stopping;
    // notify all region to stop learner read.
    kvstore->traverseRegions([](const RegionID, const RegionPtr & region) { region->notifyApplied(); });
}

void TMTContext::setStatusTerminated()
{
    store_status = StoreStatus::Terminated;
}

UInt64 TMTContext::batchReadIndexTimeout() const
{
    return batch_read_index_timeout_ms.load(std::memory_order_relaxed);
}
UInt64 TMTContext::waitIndexTimeout() const
{
    return wait_index_timeout_ms.load(std::memory_order_relaxed);
}
void TMTContext::debugSetWaitIndexTimeout(UInt64 timeout)
{
    return wait_index_timeout_ms.store(timeout, std::memory_order_relaxed);
}
Int64 TMTContext::waitRegionReadyTimeout() const
{
    return wait_region_ready_timeout_sec.load(std::memory_order_relaxed);
}
uint64_t TMTContext::readIndexWorkerTick() const
{
    return read_index_worker_tick_ms.load(std::memory_order_relaxed);
}

void TMTContext::debugSetKVStore(const KVStorePtr & new_kvstore)
{
    kvstore = new_kvstore;
}

const std::string & IntoStoreStatusName(TMTContext::StoreStatus status)
{
    static const std::string StoreStatusName[] = {
        "Idle",
        "Ready",
        "Running",
        "Stopping",
        "Terminated",
    };
    static const std::string Unknown = "Unknown";
    auto idx = static_cast<uint8_t>(status);
    return idx > static_cast<uint8_t>(TMTContext::StoreStatus::_MIN)
            && idx < static_cast<uint8_t>(TMTContext::StoreStatus::_MAX)
        ? StoreStatusName[idx - 1]
        : Unknown;
}

} // namespace DB
