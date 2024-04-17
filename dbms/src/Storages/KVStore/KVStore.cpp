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
    , region_eager_gc_log_gap(0) // disable on the serverless branch
    // Eager RaftLog GC is only enabled under UniPS
    , eager_raft_log_gc_enabled(context.getPageStorageRunMode() == PageStorageRunMode::UNI_PS)
{
    // default config about compact-log: rows 40k, bytes 32MB, gap 200.
    LOG_INFO(log, "KVStore inited, eager_raft_log_gc_enabled={}", eager_raft_log_gc_enabled);
    using namespace std::chrono_literals;
    monitoring_thread = new std::thread([&]() {
        while (true)
        {
            std::unique_lock l(monitoring_mut);
            monitoring_cv.wait_for(l, 5000ms, [&]() { return is_terminated; });
            if (is_terminated)
                return;
            recordThreadAllocInfo();
        }
    });
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

    // Try fetch proxy's config as a json string
    // if (proxy_helper && proxy_helper->fn_get_config_json)
    // {
    //     RustStrWithView rust_string
    //         = proxy_helper->fn_get_config_json(proxy_helper->proxy_ptr, ConfigJsonType::ProxyConfigAddressed);
    //     std::string cpp_string(rust_string.buff.data, rust_string.buff.len);
    //     RustGcHelper::instance().gcRustPtr(rust_string.inner.ptr, rust_string.inner.type);
    //     try
    //     {
    //         Poco::JSON::Parser parser;
    //         auto obj = parser.parse(cpp_string);
    //         auto ptr = obj.extract<Poco::JSON::Object::Ptr>();
    //         auto raftstore = ptr->getObject("raftstore");
    //         proxy_config_summary.snap_handle_pool_size = raftstore->getValue<uint64_t>("snap-handle-pool-size");
    //         LOG_INFO(log, "Parsed proxy config snap_handle_pool_size {}", proxy_config_summary.snap_handle_pool_size);
    //         proxy_config_summary.valid = true;
    //     }
    //     catch (...)
    //     {
    //         proxy_config_summary.valid = false;
    //         // we don't care
    //         LOG_WARNING(log, "Can't parse config from proxy {}", cpp_string);
    //     }
    // }
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

RegionTaskLock RegionTaskCtrl::genRegionTaskLock(RegionID region_id) const
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

void KVStore::persistRegion(
    const Region & region,
    const RegionTaskLock & region_task_lock,
    PersistRegionReason reason,
    const char * extra_msg) const
{
    RUNTIME_CHECK_MSG(
        region_persister,
        "try access to region_persister without initialization, stack={}",
        StackTrace().toString());
    auto reason_id = magic_enum::enum_underlying(reason);
    std::string caller = fmt::format("{} {}", PersistRegionReasonMap[reason_id], extra_msg);
    LOG_INFO(
        log,
        "Start to persist {}, cache size: {} bytes for `{}`",
        region.toString(true),
        region.dataSize(),
        caller);
    region_persister->persist(region, region_task_lock);
    LOG_DEBUG(log, "Persist {} done, cache size: {} bytes", region.toString(false), region.dataSize());

    switch (reason)
    {
    case PersistRegionReason::UselessAdminCommand:
        GET_METRIC(tiflash_raft_raft_events_count, type_flush_useless_admin).Increment(1);
        break;
    case PersistRegionReason::AdminCommand:
        GET_METRIC(tiflash_raft_raft_events_count, type_flush_useful_admin).Increment(1);
        break;
    case PersistRegionReason::Flush:
        // It used to be type_exec_compact.
        GET_METRIC(tiflash_raft_raft_events_count, type_flush_passive).Increment(1);
        break;
    case PersistRegionReason::ProactiveFlush:
        GET_METRIC(tiflash_raft_raft_events_count, type_flush_proactive).Increment(1);
        break;
    case PersistRegionReason::ApplySnapshotPrevRegion:
    case PersistRegionReason::ApplySnapshotCurRegion:
        GET_METRIC(tiflash_raft_raft_events_count, type_flush_apply_snapshot).Increment(1);
        break;
    case PersistRegionReason::IngestSst:
        GET_METRIC(tiflash_raft_raft_events_count, type_flush_ingest_sst).Increment(1);
        break;
    case PersistRegionReason::EagerRaftGc:
        GET_METRIC(tiflash_raft_raft_events_count, type_flush_eager_gc).Increment(1);
        break;
    case PersistRegionReason::Debug: // ignore
        break;
    }
}

bool KVStore::needFlushRegionData(UInt64 region_id, TMTContext & tmt)
{
    auto region_task_lock = region_manager.genRegionTaskLock(region_id);
    const RegionPtr curr_region_ptr = getRegion(region_id);
    // TODO Should handle when curr_region_ptr is null.
    return canFlushRegionDataImpl(curr_region_ptr, false, false, tmt, region_task_lock, 0, 0, 0, 0);
}

bool KVStore::tryFlushRegionData(
    UInt64 region_id,
    bool force_persist,
    bool try_until_succeed,
    TMTContext & tmt,
    UInt64 index,
    UInt64 term,
    uint64_t truncated_index,
    uint64_t truncated_term)
{
    auto region_task_lock = region_manager.genRegionTaskLock(region_id);
    const RegionPtr curr_region_ptr = getRegion(region_id);

    if (curr_region_ptr == nullptr)
    {
        /// If we can't find region here, we return true so proxy can trigger a CompactLog.
        /// The triggered CompactLog will be handled by `handleUselessAdminRaftCmd`,
        /// and result in a `EngineStoreApplyRes::NotFound`.
        /// Proxy will print this message and continue: `region not found in engine-store, maybe have exec `RemoveNode` first`.
        LOG_WARNING(
            log,
            "[region_id={} term={} index={}] not exist when flushing, maybe have exec `RemoveNode` first",
            region_id,
            term,
            index);
        return true;
    }

    if (!force_persist)
    {
        GET_METRIC(tiflash_raft_raft_events_count, type_pre_exec_compact).Increment(1);
        // try to flush RegionData according to the mem cache rows/bytes/interval
        return canFlushRegionDataImpl(
            curr_region_ptr,
            true,
            try_until_succeed,
            tmt,
            region_task_lock,
            index,
            term,
            truncated_index,
            truncated_term);
    }

    // force persist
    auto & curr_region = *curr_region_ptr;
    LOG_DEBUG(
        log,
        "flush region due to tryFlushRegionData by force, region_id={} term={} index={}",
        curr_region.id(),
        term,
        index);
    if (!forceFlushRegionDataImpl(curr_region, try_until_succeed, tmt, region_task_lock, index, term))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Force flush region failed, region_id={}", region_id);
    }
    return true;
}

bool KVStore::canFlushRegionDataImpl(
    const RegionPtr & curr_region_ptr,
    UInt8 flush_if_possible,
    bool try_until_succeed,
    TMTContext & tmt,
    const RegionTaskLock & region_task_lock,
    UInt64 index,
    UInt64 term,
    UInt64 truncated_index,
    UInt64 truncated_term)
{
    if (curr_region_ptr == nullptr)
    {
        throw Exception("region not found when trying flush", ErrorCodes::LOGICAL_ERROR);
    }
    auto & curr_region = *curr_region_ptr;

    bool can_flush = false;
    auto [rows, size_bytes] = curr_region.getApproxMemCacheInfo();

    // flush caused by rows
    if (rows >= region_compact_log_min_rows.load(std::memory_order_relaxed))
    {
        GET_METRIC(tiflash_raft_raft_events_count, type_flush_rowcount).Increment(1);
        can_flush = true;
    }
    // flush caused by bytes
    if (size_bytes >= region_compact_log_min_bytes.load(std::memory_order_relaxed))
    {
        GET_METRIC(tiflash_raft_raft_events_count, type_flush_size).Increment(1);
        can_flush = true;
    }
    // flush caused by gap
    auto gap_threshold = region_compact_log_gap.load();
    const auto last_restart_log_applied = curr_region.lastRestartLogApplied();
    if (last_restart_log_applied + gap_threshold > index)
    {
        // Make it more likely to flush after restart to reduce memory consumption
        gap_threshold = std::max(gap_threshold / 2, 1);
    }
    const auto last_compact_log_applied = curr_region.lastCompactLogApplied();
    const auto current_applied_gap = index > last_compact_log_applied ? index - last_compact_log_applied : 0;

    // TODO We will use truncated_index once Proxy/TiKV supports.
    // When a Region is newly created in TiFlash, last_compact_log_applied is 0, we don't trigger immediately.
    if (last_compact_log_applied == 0)
    {
        // We will set `last_compact_log_applied` to current applied_index if it is zero.
        curr_region.setLastCompactLogApplied(index);
    }
    else if (last_compact_log_applied > 0 && index > last_compact_log_applied + gap_threshold)
    {
        GET_METRIC(tiflash_raft_raft_events_count, type_flush_log_gap).Increment(1);
        can_flush = true;
    }

    LOG_DEBUG(
        log,
        "{} approx mem cache info: rows {}, bytes {}, gap {}/{}",
        curr_region.toString(false),
        rows,
        size_bytes,
        current_applied_gap,
        gap_threshold);

    if (can_flush && flush_if_possible)
    {
        // This rarely happens when there are too may raft logs, which don't trigger a proactive flush.
        LOG_INFO(
            log,
            "{} flush region due to tryFlushRegionData, index {} term {} truncated_index {} truncated_term {}"
            " gap {}/{}",
            curr_region.toString(false),
            index,
            term,
            truncated_index,
            truncated_term,
            current_applied_gap,
            gap_threshold);
        GET_METRIC(tiflash_raft_region_flush_bytes, type_flushed).Observe(size_bytes);
        return forceFlushRegionDataImpl(curr_region, try_until_succeed, tmt, region_task_lock, index, term);
    }
    else
    {
        GET_METRIC(tiflash_raft_region_flush_bytes, type_unflushed).Observe(size_bytes);
        GET_METRIC(tiflash_raft_raft_log_gap_count, type_unflushed_applied_index).Observe(current_applied_gap);
    }
    return can_flush;
}

bool KVStore::forceFlushRegionDataImpl(
    Region & curr_region,
    bool try_until_succeed,
    TMTContext & tmt,
    const RegionTaskLock & region_task_lock,
    UInt64 index,
    UInt64 term) const
{
    Stopwatch watch;
    if (index)
    {
        // We advance index when pre exec CompactLog.
        curr_region.handleWriteRaftCmd({}, index, term, tmt);
    }

    if (!tryFlushRegionCacheInStorage(tmt, curr_region, log, try_until_succeed))
    {
        return false;
    }

    // flush cache in storage level is done, persist the region info
    persistRegion(curr_region, region_task_lock, PersistRegionReason::Flush, "");
    // CompactLog will be done in proxy soon, we advance the eager truncate index in TiFlash
    curr_region.updateRaftLogEagerIndex(index);
    curr_region.cleanApproxMemCacheInfo();
    GET_METRIC(tiflash_raft_apply_write_command_duration_seconds, type_flush_region).Observe(watch.elapsedSeconds());
    return true;
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
    stopThreadAllocInfo();
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

static std::string getThreadNameAggPrefix(const std::string_view & s)
{
    if (auto pos = s.find_last_of('-'); pos != std::string::npos)
    {
        return std::string(s.begin(), s.begin() + pos);
    }
    return std::string(s.begin(), s.end());
}

void KVStore::reportThreadAllocInfo(std::string_view thdname, ReportThreadAllocateInfoType type, uint64_t value)
{
    // Many threads have empty name, better just not handle.
    if (thdname.empty())
        return;
    std::string tname(thdname.begin(), thdname.end());
    switch (type)
    {
    case ReportThreadAllocateInfoType::Reset:
    {
        auto & metrics = TiFlashMetrics::instance();
        metrics.registerProxyThreadMemory(getThreadNameAggPrefix(tname));
        {
            std::unique_lock l(memory_allocation_mut);
            memory_allocation_map.insert_or_assign(tname, ThreadInfoJealloc());
        }
        break;
    }
    case ReportThreadAllocateInfoType::Remove:
    {
        std::unique_lock l(memory_allocation_mut);
        memory_allocation_map.erase(tname);
        break;
    }
    case ReportThreadAllocateInfoType::AllocPtr:
    {
        std::shared_lock l(memory_allocation_mut);
        if (value == 0)
            return;
        auto it = memory_allocation_map.find(tname);
        if unlikely (it == memory_allocation_map.end())
        {
            return;
        }
        it->second.allocated_ptr = value;
        break;
    }
    case ReportThreadAllocateInfoType::DeallocPtr:
    {
        std::shared_lock l(memory_allocation_mut);
        if (value == 0)
            return;
        auto it = memory_allocation_map.find(tname);
        if unlikely (it == memory_allocation_map.end())
        {
            return;
        }
        it->second.deallocated_ptr = value;
        break;
    }
    }
}

static const std::unordered_set<std::string> RECORD_WHITE_LIST_THREAD_PREFIX = {"ReadIndexWkr"};

/// For those everlasting threads, we can directly access their allocatedp/allocatedp.
void KVStore::recordThreadAllocInfo()
{
    std::shared_lock l(memory_allocation_mut);
    std::unordered_map<std::string, int64_t> agg_remaining;
    for (const auto & [k, v] : memory_allocation_map)
    {
        auto agg_thread_name = getThreadNameAggPrefix(std::string_view(k.data(), k.size()));
        // Some thread may have shorter lifetime, we can't use this timed task here to upgrade.
        if (RECORD_WHITE_LIST_THREAD_PREFIX.contains(agg_thread_name))
        {
            auto [it, ok] = agg_remaining.emplace(agg_thread_name, 0);
            it->second += v.remaining();
        }
    }
    for (const auto & [k, v] : agg_remaining)
    {
        auto & tiflash_metrics = TiFlashMetrics::instance();
        tiflash_metrics.setProxyThreadMemory(k, v);
    }
}

/// For those threads with shorter life, we must only update in their call chain.
void KVStore::reportThreadAllocBatch(std::string_view name, ReportThreadAllocateInfoBatch data)
{
    // Many threads have empty name, better just not handle.
    if (name.empty())
        return;
    // TODO(jemalloc-trace) Could be costy.
    auto k = getThreadNameAggPrefix(name);
    int64_t v = static_cast<int64_t>(data.alloc) - static_cast<int64_t>(data.dealloc);
    auto & tiflash_metrics = TiFlashMetrics::instance();
    tiflash_metrics.setProxyThreadMemory(k, v);
}

void KVStore::stopThreadAllocInfo()
{
    {
        std::unique_lock lk(monitoring_mut);
        if (monitoring_thread == nullptr)
            return;
        is_terminated = true;
        monitoring_cv.notify_all();
    }
    LOG_INFO(log, "KVStore shutdown, wait thread alloc monitor join");
    monitoring_thread->join();
    delete monitoring_thread;
    monitoring_thread = nullptr;
}

} // namespace DB
