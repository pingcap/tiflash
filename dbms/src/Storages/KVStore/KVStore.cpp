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

#include <Common/FmtUtils.h>
#include <Common/Stopwatch.h>
#include <Common/SyncPoint/SyncPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <RaftStoreProxyFFI/ProxyFFI.h>
#include <Storages/KVStore/BackgroundService.h>
#include <Storages/KVStore/Decode/RegionTable.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/Disagg/FastAddPeerContext.h>
#include <Storages/KVStore/MultiRaft/RegionExecutionResult.h>
#include <Storages/KVStore/MultiRaft/RegionPersister.h>
#include <Storages/KVStore/Read/ReadIndexWorker.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Types.h>
#include <Storages/StorageDeltaMerge.h>
#include <common/likely.h>

#include <mutex>
#include <tuple>
#include <variant>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int TABLE_IS_DROPPED;
} // namespace ErrorCodes

namespace FailPoints
{
extern const char force_fail_in_flush_region_data[];
extern const char pause_passive_flush_before_persist_region[];
extern const char force_not_clean_fap_on_destroy[];
} // namespace FailPoints

KVStore::KVStore(Context & context)
    : region_persister(
        context.getSharedContextDisagg()->isDisaggregatedComputeMode() ? nullptr
                                                                       : std::make_unique<RegionPersister>(context))
    , raft_cmd_res(std::make_unique<RaftCommandResult>())
    , log(Logger::get())
    , region_compact_log_min_rows(40 * 1024)
    , region_compact_log_min_bytes(32 * 1024 * 1024)
    , region_compact_log_gap(200)
    , region_eager_gc_log_gap(512)
    // Eager RaftLog GC is only enabled under UniPS
    , eager_raft_log_gc_enabled(context.getPageStorageRunMode() == PageStorageRunMode::UNI_PS)
{
    // default config about compact-log: rows 40k, bytes 32MB, gap 200.
    LOG_INFO(log, "KVStore inited, eager_raft_log_gc_enabled={}", eager_raft_log_gc_enabled);
    joint_memory_allocation_map = context.getJointThreadInfoJeallocMap();
    if (joint_memory_allocation_map == nullptr)
    {
        LOG_WARNING(log, "JointThreadInfoJeallocMap is not inited from context");
    }
    fetchProxyConfig(proxy_helper);
}

void KVStore::restore(PathPool & path_pool, const TiFlashRaftProxyHelper * proxy_helper)
{
    if (!region_persister)
        return;

    auto task_lock = genTaskLock();
    auto manage_lock = genRegionMgrWriteLock(task_lock);

    this->proxy_helper = proxy_helper;
    manage_lock.regions = region_persister->restore(path_pool, proxy_helper);

    LOG_INFO(log, "Restored {} regions", manage_lock.regions.size());

    // init range index
    for (const auto & [id, region] : manage_lock.regions)
    {
        std::ignore = id;
        manage_lock.index.add(region);
    }

    {
        const size_t batch = 512;
        std::vector<std::stringstream> msgs;
        msgs.resize(batch);

        // init range index
        for (const auto & [id, region] : manage_lock.regions)
        {
            msgs[id % batch] << region->getDebugString() << ";";
        }

        for (const auto & msg : msgs)
        {
            auto str = msg.str();
            if (!str.empty())
                LOG_INFO(log, "{}", str);
        }
    }
}

void KVStore::fetchProxyConfig(const TiFlashRaftProxyHelper * proxy_helper)
{
    // Try fetch proxy's config as a json string
    if (proxy_helper && proxy_helper->fn_get_config_json)
    {
        RustStrWithView rust_string
            = proxy_helper->fn_get_config_json(proxy_helper->proxy_ptr, ConfigJsonType::ProxyConfigAddressed);
        std::string cpp_string(rust_string.buff.data, rust_string.buff.len);
        RustGcHelper::instance().gcRustPtr(rust_string.inner.ptr, rust_string.inner.type);
        try
        {
            Poco::JSON::Parser parser;
            auto obj = parser.parse(cpp_string);
            auto ptr = obj.extract<Poco::JSON::Object::Ptr>();
            auto raftstore = ptr->getObject("raftstore");
            proxy_config_summary.snap_handle_pool_size = raftstore->getValue<uint64_t>("snap-handle-pool-size");
            auto server = ptr->getObject("server");
            proxy_config_summary.engine_addr = server->getValue<std::string>("engine-addr");
            LOG_INFO(
                log,
                "Parsed proxy config: snap_handle_pool_size={} engine_addr={}",
                proxy_config_summary.snap_handle_pool_size,
                proxy_config_summary.engine_addr);
            proxy_config_summary.valid = true;
        }
        catch (...)
        {
            proxy_config_summary.valid = false;
            // we don't care
            LOG_WARNING(log, "Can't parse config from proxy {}", cpp_string);
        }
    }
}

RegionPtr KVStore::getRegion(RegionID region_id) const
{
    auto manage_lock = genRegionMgrReadLock();
    if (auto it = manage_lock.regions.find(region_id); it != manage_lock.regions.end())
        return it->second;
    return nullptr;
}

// TODO: may get regions not in segment?
RegionMap KVStore::getRegionsByRangeOverlap(const RegionRange & range) const
{
    auto manage_lock = genRegionMgrReadLock();
    return manage_lock.index.findByRangeOverlap(range);
}

RegionTaskLock RegionTaskCtrl::genRegionTaskLock(RegionID region_id) const NO_THREAD_SAFETY_ANALYSIS
{
    RegionTaskElement * e = nullptr;
    {
        auto _ = genLockGuard();
        auto it = regions.try_emplace(region_id).first;
        e = &it->second;
    }
    return RegionTaskLock(e->mutex);
}

RegionTaskLock RegionManager::genRegionTaskLock(RegionID region_id) const
{
    return region_task_ctrl.genRegionTaskLock(region_id);
}

size_t KVStore::regionSize() const
{
    auto manage_lock = genRegionMgrReadLock();
    return manage_lock.regions.size();
}

void KVStore::traverseRegions(std::function<void(RegionID, const RegionPtr &)> && callback) const
{
    auto manage_lock = genRegionMgrReadLock();
    for (const auto & region : manage_lock.regions)
        callback(region.first, region.second);
}

bool KVStore::tryFlushRegionCacheInStorage(
    TMTContext & tmt,
    const Region & region,
    const LoggerPtr & log,
    bool try_until_succeed)
{
    fiu_do_on(FailPoints::force_fail_in_flush_region_data, { return false; });
    auto keyspace_id = region.getKeyspaceID();
    auto table_id = region.getMappedTableID();
    auto storage = tmt.getStorages().get(keyspace_id, table_id);
    if (unlikely(storage == nullptr))
    {
        LOG_WARNING(
            log,
            "tryFlushRegionCacheInStorage can not get table, region {} table_id={}, ignored",
            region.toString(),
            table_id);
        return true;
    }

    try
    {
        // Acquire `drop_lock` so that no other threads can drop the storage during `flushCache`. `alter_lock` is not required.
        auto storage_lock = storage->lockForShare(getThreadNameAndID());
        auto rowkey_range = DM::RowKeyRange::fromRegionRange(
            region.getRange(),
            region.getRange()->getMappedTableID(),
            storage->isCommonHandle(),
            storage->getRowKeyColumnSize());
        return storage->flushCache(tmt.getContext(), rowkey_range, try_until_succeed);
    }
    catch (DB::Exception & e)
    {
        // We can ignore if storage is already dropped.
        if (e.code() == ErrorCodes::TABLE_IS_DROPPED)
            return true;
        else
            throw;
    }
}

void KVStore::gcPersistedRegion(Seconds gc_persist_period)
{
    {
        decltype(bg_gc_region_data) tmp;
        std::lock_guard lock(bg_gc_region_data_mutex);
        tmp.swap(bg_gc_region_data);
    }
    Timepoint now = Clock::now();
    if (now < (last_gc_time.load() + gc_persist_period))
        return;
    last_gc_time = now;
    RUNTIME_CHECK_MSG(
        region_persister,
        "try access to region_persister without initialization, stack={}",
        StackTrace().toString());
    region_persister->gc();
}

void KVStore::removeRegion(
    RegionID region_id,
    bool remove_data,
    RegionTable & region_table,
    const KVStoreTaskLock & task_lock,
    const RegionTaskLock & region_lock)
{
    LOG_INFO(log, "Start to remove region_id={}", region_id);

    {
        auto manage_lock = genRegionMgrWriteLock(task_lock);
        auto it = manage_lock.regions.find(region_id);
        manage_lock.index.remove(
            it->second->makeRaftCommandDelegate(task_lock).getRange().comparableKeys(),
            region_id); // remove index
        manage_lock.regions.erase(it);
    }
    {
        if (read_index_worker_manager) //std::atomic_thread_fence will protect it
        {
            // remove cache & read-index task
            read_index_worker_manager->getWorkerByRegion(region_id).removeRegion(region_id);
        }
    }

    RUNTIME_CHECK_MSG(
        region_persister,
        "try access to region_persister without initialization, stack={}",
        StackTrace().toString());
    region_persister->drop(region_id, region_lock);
    LOG_INFO(log, "Persisted region_id={} deleted", region_id);

    region_table.removeRegion(region_id, remove_data, region_lock);

    LOG_INFO(log, "Remove region_id={} done", region_id);
}

KVStoreTaskLock KVStore::genTaskLock() const
{
    return KVStoreTaskLock(task_mutex);
}

RegionManager::RegionReadLock KVStore::genRegionMgrReadLock() const
{
    return region_manager.genReadLock();
}

RegionManager::RegionWriteLock KVStore::genRegionMgrWriteLock(const KVStoreTaskLock &)
{
    return region_manager.genWriteLock();
}

bool KVStore::tryRegisterEagerRaftLogGCTask(const RegionPtr & region, RegionTaskLock & /*region_persist_lock*/)
{
    if (!eager_raft_log_gc_enabled)
        return false;
    const UInt64 threshold = region_eager_gc_log_gap.load();
    if (threshold == 0) // disabled
        return false;

    // When some peer is down, the TiKV compact log become quite slow and the truncated index
    // is advanced slowly. Under disagg arch, too many RaftLog are stored in UniPS and makes TiFlash OOM.
    // We apply eager RaftLog GC on TiFlash's UniPS.
    auto [last_eager_truncated_index, applied_index] = region->getRaftLogEagerGCRange();
    return raft_log_gc_hints.updateHint(region->id(), last_eager_truncated_index, applied_index, threshold);
}

RaftLogEagerGcTasks::Hints KVStore::getRaftLogGcHints()
{
    return raft_log_gc_hints.getAndClearHints();
}

void KVStore::applyRaftLogGcTaskRes(const RaftLogGcTasksRes & res) const
{
    for (const auto & [region_id, log_index] : res)
    {
        auto region = getRegion(region_id);
        if (!region)
            continue;
        region->updateRaftLogEagerIndex(log_index);
    }
}

void KVStore::handleDestroy(UInt64 region_id, TMTContext & tmt)
{
    handleDestroy(region_id, tmt, genTaskLock());
}

void KVStore::handleDestroy(UInt64 region_id, TMTContext & tmt, const KVStoreTaskLock & task_lock)
{
    const auto region = getRegion(region_id);
    // Always try to clean obsolete FAP snapshot
    if (tmt.getContext().getSharedContextDisagg()->isDisaggregatedStorageMode())
    {
        // Everytime we remove region, we try to clean obsolete fap ingest info.
        auto fap_ctx = tmt.getContext().getSharedContextDisagg()->fap_context;
        fiu_do_on(FailPoints::force_not_clean_fap_on_destroy, { return; });
        fap_ctx->resolveFapSnapshotState(tmt, proxy_helper, region_id, false);
    }
    if (region == nullptr)
    {
        LOG_INFO(log, "region_id={} not found, might be removed already", region_id);
        return;
    }
    LOG_INFO(log, "Handle destroy {}", region->toString());
    region->setPendingRemove();
    removeRegion(
        region_id,
        /* remove_data */ true,
        tmt.getRegionTable(),
        task_lock,
        region_manager.genRegionTaskLock(region_id));
}

void KVStore::setRegionCompactLogConfig(UInt64 rows, UInt64 bytes, UInt64 gap, UInt64 eager_gc_gap)
{
    region_compact_log_min_rows = rows;
    region_compact_log_min_bytes = bytes;
    region_compact_log_gap = gap;
    region_eager_gc_log_gap = eager_gc_gap;

    LOG_INFO(
        log,
        "Region compact log thresholds, rows={} bytes={} gap={} eager_gc_gap={}",
        rows,
        bytes,
        gap,
        eager_gc_gap);
}

void KVStore::setStore(metapb::Store store_)
{
    getStore().update(std::move(store_));
    LOG_INFO(log, "Set store info {}", getStore().base.ShortDebugString());
}

StoreID KVStore::getStoreID(std::memory_order memory_order) const
{
    return getStore().store_id.load(memory_order);
}

KVStore::StoreMeta::Base KVStore::StoreMeta::getMeta() const
{
    std::lock_guard lock(mu);
    return base;
}

metapb::Store KVStore::clonedStoreMeta() const
{
    return getStore().getMeta();
}

const metapb::Store & KVStore::getStoreMeta() const
{
    return this->store.base;
}

metapb::Store & KVStore::debugMutStoreMeta()
{
    return this->store.base;
}

KVStore::StoreMeta & KVStore::getStore()
{
    return this->store;
}

const KVStore::StoreMeta & KVStore::getStore() const
{
    return this->store;
}

void KVStore::StoreMeta::update(Base && base_)
{
    std::lock_guard lock(mu);
    base = std::move(base_);
    store_id = base.id();
}

KVStore::~KVStore()
{
    LOG_INFO(log, "Destroy KVStore");
    releaseReadIndexWorkers();
    LOG_INFO(log, "Destroy KVStore Finished");
}

FileUsageStatistics KVStore::getFileUsageStatistics() const
{
    if (!region_persister)
    {
        return {};
    }

    return region_persister->getFileUsageStatistics();
}

size_t KVStore::getOngoingPrehandleTaskCount() const
{
    return std::max(0, ongoing_prehandle_task_count.load());
}

size_t KVStore::getOngoingPrehandleSubtaskCount() const
{
    return std::max(0, prehandling_trace.ongoing_prehandle_subtask_count.load());
}

static const metapb::Peer & findPeer(const metapb::Region & region, UInt64 peer_id)
{
    for (const auto & peer : region.peers())
    {
        if (peer.id() == peer_id)
        {
            return peer;
        }
    }

    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "{}: peer not found in region, peer_id={} region_id={}",
        __PRETTY_FUNCTION__,
        peer_id,
        region.id());
}

// Generate a temporary region pointer by the given meta
RegionPtr KVStore::genRegionPtr(metapb::Region && region, UInt64 peer_id, UInt64 index, UInt64 term)
{
    auto meta = ({
        auto peer = findPeer(region, peer_id);
        raft_serverpb::RaftApplyState apply_state;
        {
            apply_state.set_applied_index(index);
            apply_state.mutable_truncated_state()->set_index(index);
            apply_state.mutable_truncated_state()->set_term(term);
        }
        RegionMeta(std::move(peer), std::move(region), std::move(apply_state));
    });

    return std::make_shared<Region>(std::move(meta), proxy_helper);
}

RegionTaskLock KVStore::genRegionTaskLock(UInt64 region_id) const
{
    return region_manager.genRegionTaskLock(region_id);
}


void KVStore::reportThreadAllocInfo(std::string_view v, ReportThreadAllocateInfoType type, uint64_t value)
{
    joint_memory_allocation_map->reportThreadAllocInfoForProxy(v, type, value);
}

void KVStore::reportThreadAllocBatch(std::string_view v, ReportThreadAllocateInfoBatch data)
{
    JointThreadInfoJeallocMap::reportThreadAllocBatchForProxy(v, data);
}

} // namespace DB
