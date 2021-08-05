#include <Common/DNSCache.h>
#include <Flash/Mpp/MPPHandler.h>
#include <Interpreters/Context.h>
#include <Server/RaftConfigParser.h>
#include <Storages/Transaction/BackgroundService.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/RegionExecutionResult.h>
#include <Storages/Transaction/RegionRangeKeys.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDBSchemaSyncer.h>
#include <pingcap/pd/MockPDClient.h>

namespace DB
{
// default batch-read-index timeout is 10_000ms.
extern const uint64_t DEFAULT_BATCH_READ_INDEX_TIMEOUT_MS = 10 * 1000;

TMTContext::TMTContext(Context & context_, const TiFlashRaftConfig & raft_config, const pingcap::ClusterConfig & cluster_config)
    : context(context_),
      kvstore(std::make_shared<KVStore>(context, raft_config.snapshot_apply_method)),
      region_table(context),
      background_service(nullptr),
      gc_manager(context),
      cluster(raft_config.pd_addrs.size() == 0 ? std::make_shared<pingcap::kv::Cluster>()
                                               : std::make_shared<pingcap::kv::Cluster>(raft_config.pd_addrs, cluster_config)),
      ignore_databases(raft_config.ignore_databases),
      schema_syncer(raft_config.pd_addrs.size() == 0
              ? std::static_pointer_cast<SchemaSyncer>(std::make_shared<TiDBSchemaSyncer</*mock*/ true>>(cluster))
              : std::static_pointer_cast<SchemaSyncer>(std::make_shared<TiDBSchemaSyncer</*mock*/ false>>(cluster))),
      mpp_task_manager(std::make_shared<MPPTaskManager>()),
      engine(raft_config.engine),
      disable_bg_flush(raft_config.disable_bg_flush),
      replica_read_max_thread(1),
      batch_read_index_timeout_ms(DEFAULT_BATCH_READ_INDEX_TIMEOUT_MS)
{}

void TMTContext::restore(const TiFlashRaftProxyHelper * proxy_helper)
{
    kvstore->restore(proxy_helper);
    region_table.restore();
    store_status = StoreStatus::Ready;

    background_service = std::make_unique<BackgroundService>(*this);
}

KVStorePtr & TMTContext::getKVStore() { return kvstore; }

const KVStorePtr & TMTContext::getKVStore() const { return kvstore; }

ManagedStorages & TMTContext::getStorages() { return storages; }

const ManagedStorages & TMTContext::getStorages() const { return storages; }

RegionTable & TMTContext::getRegionTable() { return region_table; }

const RegionTable & TMTContext::getRegionTable() const { return region_table; }

BackgroundService & TMTContext::getBackgroundService() { return *background_service; }

const BackgroundService & TMTContext::getBackgroundService() const { return *background_service; }

GCManager & TMTContext::getGCManager() { return gc_manager; }

Context & TMTContext::getContext() { return context; }

bool TMTContext::isInitialized() const { return getStoreStatus() != StoreStatus::Idle; }

void TMTContext::setStatusRunning() { store_status = StoreStatus::Running; }

TMTContext::StoreStatus TMTContext::getStoreStatus(std::memory_order memory_order) const { return store_status.load(memory_order); }

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

pingcap::pd::ClientPtr TMTContext::getPDClient() const { return cluster->pd_client; }

MPPTaskManagerPtr TMTContext::getMPPTaskManager() { return mpp_task_manager; }

const std::unordered_set<std::string> & TMTContext::getIgnoreDatabases() const { return ignore_databases; }

void TMTContext::reloadConfig(const Poco::Util::AbstractConfiguration & config)
{
    static constexpr const char * TABLE_OVERLAP_THRESHOLD = "flash.overlap_threshold";
    static constexpr const char * COMPACT_LOG_MIN_PERIOD = "flash.compact_log_min_period";
    static constexpr const char * COMPACT_LOG_MIN_ROWS = "flash.compact_log_min_rows";
    static constexpr const char * COMPACT_LOG_MIN_BYTES = "flash.compact_log_min_bytes";
    static constexpr const char * REPLICA_READ_MAX_THREAD = "flash.replica_read_max_thread";
    static constexpr const char * BATCH_READ_INDEX_TIMEOUT_MS = "flash.batch_read_index_timeout_ms";
    static constexpr const char * WAIT_REGION_READY_TIMEOUT_SEC = "flash.wait_region_ready_timeout_sec";

    getRegionTable().setTableCheckerThreshold(config.getDouble(TABLE_OVERLAP_THRESHOLD, 0.6));
    // default config about compact-log: period 120s, rows 40k, bytes 32MB.
    getKVStore()->setRegionCompactLogConfig(std::max(config.getUInt64(COMPACT_LOG_MIN_PERIOD, 120), 1),
        std::max(config.getUInt64(COMPACT_LOG_MIN_ROWS, 40 * 1024), 1),
        std::max(config.getUInt64(COMPACT_LOG_MIN_BYTES, 32 * 1024 * 1024), 1));
    {
        replica_read_max_thread = std::max(config.getUInt64(REPLICA_READ_MAX_THREAD, 1), 1);
        batch_read_index_timeout_ms = config.getUInt64(BATCH_READ_INDEX_TIMEOUT_MS, DEFAULT_BATCH_READ_INDEX_TIMEOUT_MS);
        wait_region_ready_timeout_sec = ({
            int64_t t = config.getInt64(WAIT_REGION_READY_TIMEOUT_SEC, /*20min*/ 20 * 60);
            t = t >= 0 ? t : std::numeric_limits<int64_t>::max(); // set -1 to wait infinitely
            t;
        });
    }
    {
        LOG_INFO(&Logger::get(__FUNCTION__),
            "read-index max thread num: " << replicaReadMaxThread() << ", timeout: " << batchReadIndexTimeout() << "ms;"
                                          << " wait-region-ready timeout: " << waitRegionReadyTimeout() << "s");
    }
}

bool TMTContext::checkShuttingDown(std::memory_order memory_order) const { return getStoreStatus(memory_order) >= StoreStatus::Stopping; }
bool TMTContext::checkTerminated(std::memory_order memory_order) const { return getStoreStatus(memory_order) == StoreStatus::Terminated; }
bool TMTContext::checkRunning(std::memory_order memory_order) const { return getStoreStatus(memory_order) == StoreStatus::Running; }

void TMTContext::setStatusStopping()
{
    store_status = StoreStatus::Stopping;
    // notify all region to stop learner read.
    kvstore->traverseRegions([](const RegionID, const RegionPtr & region) { region->notifyApplied(); });
}

void TMTContext::setStatusTerminated() { store_status = StoreStatus::Terminated; }

UInt64 TMTContext::replicaReadMaxThread() const { return replica_read_max_thread.load(std::memory_order_relaxed); }
UInt64 TMTContext::batchReadIndexTimeout() const { return batch_read_index_timeout_ms.load(std::memory_order_relaxed); }
Int64 TMTContext::waitRegionReadyTimeout() const { return wait_region_ready_timeout_sec.load(std::memory_order_relaxed); }

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
    return idx > static_cast<uint8_t>(TMTContext::StoreStatus::_MIN) && idx < static_cast<uint8_t>(TMTContext::StoreStatus::_MAX)
        ? StoreStatusName[idx - 1]
        : Unknown;
}

} // namespace DB
