#include <Common/DNSCache.h>
#include <Interpreters/Context.h>
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

TMTContext::TMTContext(Context & context_, const std::vector<std::string> & addrs,
    const std::unordered_set<std::string> & ignore_databases_, ::TiDB::StorageEngine engine_, bool disable_bg_flush_,
    const pingcap::ClusterConfig & cluster_config)
    : context(context_),
      kvstore(std::make_shared<KVStore>(context)),
      region_table(context),
      background_service(nullptr),
      cluster(addrs.size() == 0 ? std::make_shared<pingcap::kv::Cluster>() : std::make_shared<pingcap::kv::Cluster>(addrs, cluster_config)),
      ignore_databases(ignore_databases_),
      schema_syncer(addrs.size() == 0 ? std::static_pointer_cast<SchemaSyncer>(std::make_shared<TiDBSchemaSyncer<true>>(cluster))
                                      : std::static_pointer_cast<SchemaSyncer>(std::make_shared<TiDBSchemaSyncer<false>>(cluster))),
      engine(engine_),
      disable_bg_flush(disable_bg_flush_)
{}

void TMTContext::restore(const TiFlashRaftProxyHelper * proxy_helper)
{
    kvstore->restore(proxy_helper);
    region_table.restore();
    initialized = true;

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

Context & TMTContext::getContext() { return context; }

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

pingcap::pd::ClientPtr TMTContext::getPDClient() const { return cluster->pd_client; }

const std::unordered_set<std::string> & TMTContext::getIgnoreDatabases() const { return ignore_databases; }

void TMTContext::reloadConfig(const Poco::Util::AbstractConfiguration & config)
{
    static const std::string & TABLE_OVERLAP_THRESHOLD = "flash.overlap_threshold";
    static const std::string & COMPACT_LOG_MIN_PERIOD = "flash.compact_log_min_period";
    static const std::string & REPLICA_READ_MAX_THREAD = "flash.replica_read_max_thread";

    getRegionTable().setTableCheckerThreshold(config.getDouble(TABLE_OVERLAP_THRESHOLD, 0.6));
    getKVStore()->setRegionCompactLogPeriod(std::max(config.getUInt64(COMPACT_LOG_MIN_PERIOD, 120), 1));
    replica_read_max_thread = std::max(config.getUInt64(REPLICA_READ_MAX_THREAD, 1), 1);
}

const std::atomic_bool & TMTContext::getTerminated() const { return terminated; }

void TMTContext::setTerminated()
{
    terminated = true;
    // notify all region to stop learner read.
    kvstore->traverseRegions([](const RegionID, const RegionPtr & region) { region->notifyApplied(); });
}

} // namespace DB
