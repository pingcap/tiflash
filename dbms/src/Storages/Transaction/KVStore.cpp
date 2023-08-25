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
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>
#include <Storages/Transaction/BackgroundService.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/ReadIndexWorker.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionExecutionResult.h>
#include <Storages/Transaction/RegionPersister.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/likely.h>

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
} // namespace FailPoints

KVStore::KVStore(Context & context)
    : region_persister(
        context.getSharedContextDisagg()->isDisaggregatedComputeMode()
            ? nullptr
            : std::make_unique<RegionPersister>(context, region_manager))
    , raft_cmd_res(std::make_unique<RaftCommandResult>())
    , log(Logger::get())
    , region_compact_log_period(120)
    , region_compact_log_min_rows(40 * 1024)
    , region_compact_log_min_bytes(32 * 1024 * 1024)
    , region_compact_log_gap(200)
{
    // default config about compact-log: period 120s, rows 40k, bytes 32MB.
    LOG_INFO(log, "KVStore inited");
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

EngineStoreApplyRes KVStore::handleWriteRaftCmdInner(
    const WriteCmdsView & cmds,
    UInt64 region_id,
    UInt64 index,
    UInt64 term,
    TMTContext & tmt,
    DM::WriteResult & write_result)
{
    EngineStoreApplyRes res;
    {
        auto region_persist_lock = region_manager.genRegionTaskLock(region_id);

        const RegionPtr region = getRegion(region_id);
        if (region == nullptr)
        {
            return EngineStoreApplyRes::NotFound;
        }

        auto && [r, w] = region->handleWriteRaftCmd(cmds, index, term, tmt);

        if (region->getClusterRaftstoreVer() == RaftstoreVer::V2)
        {
            region->orphanKeysInfo().advanceAppliedIndex(index);
        }
        write_result = std::move(w);
        res = r;
    }
    /// Safety:
    /// This call is from Proxy's applying thread of this region, so:
    /// 1. No other thread can write from raft to this region even if we unlocked here.
    /// 2. If `proactiveFlushCacheAndRegion` causes a write stall, it will be forwarded to raft layer.
    // TODO(proactive flush)
    return res;
}

EngineStoreApplyRes KVStore::handleWriteRaftCmd(
    const WriteCmdsView & cmds,
    UInt64 region_id,
    UInt64 index,
    UInt64 term,
    TMTContext & tmt)
{
    DM::WriteResult write_result;
    return handleWriteRaftCmdInner(cmds, region_id, index, term, tmt, write_result);
}

void KVStore::handleDestroy(UInt64 region_id, TMTContext & tmt)
{
    handleDestroy(region_id, tmt, genTaskLock());
}

void KVStore::handleDestroy(UInt64 region_id, TMTContext & tmt, const KVStoreTaskLock & task_lock)
{
    const auto region = getRegion(region_id);
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

void KVStore::setRegionCompactLogConfig(UInt64 sec, UInt64 rows, UInt64 bytes, UInt64 gap)
{
    region_compact_log_period = sec;
    region_compact_log_min_rows = rows;
    region_compact_log_min_bytes = bytes;
    region_compact_log_gap = gap;

    LOG_INFO(log, "threshold config: period {}, rows {}, bytes {}, gap {}", sec, rows, bytes, gap);
}

void KVStore::persistRegion(
    const Region & region,
    std::optional<const RegionTaskLock *> region_task_lock,
    PersistRegionReason reason,
    const char * extra_msg) const
{
    RUNTIME_CHECK_MSG(
        region_persister,
        "try access to region_persister without initialization, stack={}",
        StackTrace().toString());
    if (region_task_lock.has_value())
    {
        auto reason_id = magic_enum::enum_underlying(reason);
        std::string caller = fmt::format("{} {}", PersistRegionReasonMap[reason_id], extra_msg);
        LOG_INFO(
            log,
            "Start to persist {}, cache size: {} bytes for `{}`",
            region.toString(true),
            region.dataSize(),
            caller);
        region_persister->persist(region, *region_task_lock.value());
        LOG_DEBUG(log, "Persist {} done", region.toString(false));
    }
    else
    {
        LOG_INFO(log, "Try to persist {}", region.toString(false));
        region_persister->persist(region);
        LOG_INFO(log, "After persisted {}, cache {} bytes", region.toString(false), region.dataSize());
    }

    switch (reason)
    {
    case PersistRegionReason::UselessAdminCommand:
        GET_METRIC(tiflash_raft_raft_events_count, type_flush_useless_admin).Increment(1);
        break;
    case PersistRegionReason::AdminCommand:
        GET_METRIC(tiflash_raft_raft_events_count, type_flush_useful_admin).Increment(1);
        break;
    case PersistRegionReason::Flush:
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
    default:
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

    if (rows >= region_compact_log_min_rows.load(std::memory_order_relaxed))
    {
        GET_METRIC(tiflash_raft_raft_events_count, type_flush_rowcount).Increment(1);
        can_flush = true;
    }
    if (size_bytes >= region_compact_log_min_bytes.load(std::memory_order_relaxed))
    {
        GET_METRIC(tiflash_raft_raft_events_count, type_flush_size).Increment(1);
        can_flush = true;
    }
    auto gap_threshold = region_compact_log_gap.load();
    auto last_restart_log_applied = curr_region.lastRestartLogApplied();
    if (last_restart_log_applied + gap_threshold > index)
    {
        gap_threshold = std::max(gap_threshold / 2, 1);
    }
    auto last_compact_log_applied = curr_region.lastCompactLogApplied();
    auto current_applied_gap = index > last_compact_log_applied ? index - last_compact_log_applied : 0;

    // TODO We will use truncated_index once Proxy/TiKV supports.
    // After restart, last_compact_log_applied is 0, we don't trigger immediately.
    if (last_compact_log_applied && index > last_compact_log_applied + gap_threshold)
    {
        GET_METRIC(tiflash_raft_raft_events_count, type_flush_log_gap).Increment(1);
        can_flush = true;
    }
    if (!last_compact_log_applied)
    {
        // We will set `last_compact_log_applied` to current applied_index if it is zero.
        curr_region.setLastCompactLogApplied(index);
    }

    GET_METRIC(tiflash_raft_raft_events_count, type_pre_exec_compact).Increment(1);
    // GET_METRIC(tiflash_raft_raft_log_lag_count, type_compact_index).Observe(current_gap);
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
        GET_METRIC(tiflash_raft_raft_events_count, type_exec_compact).Increment(1);
        // This rarely happens when there are too may raft logs, which don't trigger a proactive flush.
        LOG_INFO(
            log,
            "{} flush region due to tryFlushRegionData, index {} term {} truncated_index {} truncated_term {} gap "
            "{}/{}",
            curr_region.toString(false),
            index,
            term,
            truncated_index,
            truncated_term,
            current_applied_gap,
            gap_threshold);
        GET_METRIC(tiflash_raft_region_flush_size, type_flushed).Observe(size_bytes);
        return forceFlushRegionDataImpl(curr_region, try_until_succeed, tmt, region_task_lock, index, term);
    }
    else
    {
        GET_METRIC(tiflash_raft_region_flush_size, type_unflushed).Observe(size_bytes);
        GET_METRIC(tiflash_raft_raft_log_lag_count, type_unflushed_applied_index).Observe(current_applied_gap);
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
        // We set actual index when handling CompactLog.
        curr_region.handleWriteRaftCmd({}, index, term, tmt);
    }

    if (!tryFlushRegionCacheInStorage(tmt, curr_region, log, try_until_succeed))
    {
        return false;
    }

    // flush cache in storage level is done, persist the region info
    persistRegion(curr_region, &region_task_lock, PersistRegionReason::Flush, "");
    curr_region.markCompactLog();
    curr_region.cleanApproxMemCacheInfo();
    GET_METRIC(tiflash_raft_apply_write_command_duration_seconds, type_flush_region).Observe(watch.elapsedSeconds());
    return true;
}

EngineStoreApplyRes KVStore::handleUselessAdminRaftCmd(
    raft_cmdpb::AdminCmdType cmd_type,
    UInt64 curr_region_id,
    UInt64 index,
    UInt64 term,
    TMTContext & tmt) const
{
    auto region_task_lock = region_manager.genRegionTaskLock(curr_region_id);
    const RegionPtr curr_region_ptr = getRegion(curr_region_id);
    if (curr_region_ptr == nullptr)
    {
        return EngineStoreApplyRes::NotFound;
    }

    auto & curr_region = *curr_region_ptr;

    LOG_DEBUG(
        log,
        "{} handle ignorable admin command {} at [term: {}, index: {}]",
        curr_region.toString(false),
        raft_cmdpb::AdminCmdType_Name(cmd_type),
        term,
        index);

    if (curr_region.getClusterRaftstoreVer() == RaftstoreVer::V2)
    {
        curr_region.orphanKeysInfo().advanceAppliedIndex(index);
    }

    if (cmd_type == raft_cmdpb::AdminCmdType::CompactLog)
    {
        // Before CompactLog, we ought to make sure all data of this region are persisted.
        // So proxy will firstly call an FFI `fn_try_flush_data` to trigger a attempt to flush data on TiFlash's side.
        // The advance of apply index aka `handleWriteRaftCmd` is executed in `fn_try_flush_data`.
        // If the attempt fails, Proxy will filter execution of this CompactLog, which means every CompactLog observed by TiFlash can ALWAYS succeed now.
        // ref. https://github.com/pingcap/tidb-engine-ext/blob/1253b471ae6204170fa3917e32e41bac1b4dc583/proxy_components/engine_store_ffi/src/core/forward_raft/command.rs#L162
        return EngineStoreApplyRes::Persist;
    }

    curr_region.handleWriteRaftCmd({}, index, term, tmt);
    if (cmd_type == raft_cmdpb::AdminCmdType::PrepareFlashback || cmd_type == raft_cmdpb::AdminCmdType::FinishFlashback
        || cmd_type == raft_cmdpb::AdminCmdType::BatchSwitchWitness)
    {
        tryFlushRegionCacheInStorage(tmt, curr_region, log);
        persistRegion(
            curr_region,
            &region_task_lock,
            PersistRegionReason::UselessAdminCommand,
            fmt::format("{}", cmd_type).c_str());
        return EngineStoreApplyRes::Persist;
    }
    return EngineStoreApplyRes::None;
}

EngineStoreApplyRes KVStore::handleAdminRaftCmd(
    raft_cmdpb::AdminRequest && request,
    raft_cmdpb::AdminResponse && response,
    UInt64 curr_region_id,
    UInt64 index,
    UInt64 term,
    TMTContext & tmt)
{
    Stopwatch watch;
    SCOPE_EXIT(
        { GET_METRIC(tiflash_raft_apply_write_command_duration_seconds, type_admin).Observe(watch.elapsedSeconds()); });
    auto type = request.cmd_type();
    switch (request.cmd_type())
    {
    // CompactLog | VerifyHash | ComputeHash won't change region meta, there is no need to occupy task lock of kvstore.
    case raft_cmdpb::AdminCmdType::CompactLog:
    case raft_cmdpb::AdminCmdType::VerifyHash:
    case raft_cmdpb::AdminCmdType::ComputeHash:
    case raft_cmdpb::AdminCmdType::PrepareFlashback:
    case raft_cmdpb::AdminCmdType::FinishFlashback:
    case raft_cmdpb::AdminCmdType::BatchSwitchWitness:
        return handleUselessAdminRaftCmd(type, curr_region_id, index, term, tmt);
    default:
        break;
    }

    RegionTable & region_table = tmt.getRegionTable();

    // Lock the whole kvstore.
    auto task_lock = genTaskLock();

    {
        auto region_task_lock = region_manager.genRegionTaskLock(curr_region_id);
        const RegionPtr curr_region_ptr = getRegion(curr_region_id);
        if (curr_region_ptr == nullptr)
        {
            LOG_WARNING(
                log,
                "region not found, might be removed already, region_id={} term={} index={} cmd={}",
                curr_region_id,
                term,
                index,
                raft_cmdpb::AdminCmdType_Name(type));
            return EngineStoreApplyRes::NotFound;
        }

        auto & curr_region = *curr_region_ptr;

        // Admin cmd contains no normal data, we can advance orphan keys info just before handling.
        if (curr_region.getClusterRaftstoreVer() == RaftstoreVer::V2)
        {
            curr_region.orphanKeysInfo().advanceAppliedIndex(index);
        }

        curr_region.makeRaftCommandDelegate(task_lock)
            .handleAdminRaftCmd(request, response, index, term, *this, region_table, *raft_cmd_res);
        RaftCommandResult & result = *raft_cmd_res;

        // After region split / merge, try to flush it
        const auto try_to_flush_region = [&tmt](const RegionPtr & region) {
            try
            {
                tmt.getRegionTable().tryWriteBlockByRegionAndFlush(region);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        };

        const auto persist_and_sync = [&](const Region & region) {
            tryFlushRegionCacheInStorage(tmt, region, log);
            persistRegion(region, &region_task_lock, PersistRegionReason::AdminCommand, "");
        };

        const auto handle_batch_split = [&](Regions & split_regions) {
            {
                // `split_regions` doesn't include the derived region.
                auto manage_lock = genRegionMgrWriteLock(task_lock);

                for (auto & new_region : split_regions)
                {
                    auto [it, ok] = manage_lock.regions.emplace(new_region->id(), new_region);
                    if (!ok)
                    {
                        // definitely, any region's index is greater or equal than the initial one.

                        // if there is already a region with same id, it means program crashed while persisting.
                        // just use the previous one.
                        new_region = it->second;
                    }
                }

                manage_lock.index.remove(result.ori_region_range->comparableKeys(), curr_region_id);
                manage_lock.index.add(curr_region_ptr);

                for (auto & new_region : split_regions)
                    manage_lock.index.add(new_region);
            }

            {
                // update region_table first is safe, because the core rule is established: the range in RegionTable
                // is always >= range in KVStore.
                for (const auto & new_region : split_regions)
                    region_table.updateRegion(*new_region);
                region_table.shrinkRegionRange(curr_region);
            }

            {
                for (const auto & new_region : split_regions)
                    try_to_flush_region(new_region);
            }

            {
                // persist curr_region at last. if program crashed after split_region is persisted, curr_region can
                // continue to complete split operation.
                for (const auto & new_region : split_regions)
                {
                    // no need to lock those new regions, because they don't have middle state.
                    persist_and_sync(*new_region);
                }
                persist_and_sync(curr_region);
            }
        };

        const auto handle_change_peer = [&]() {
            if (curr_region.isPendingRemove())
            {
                // remove `curr_region` from this node, we can remove its data.
                removeRegion(curr_region_id, /* remove_data */ true, region_table, task_lock, region_task_lock);
            }
            else
                persist_and_sync(curr_region);
        };

        const auto handle_commit_merge = [&](const RegionID source_region_id) {
            region_table.shrinkRegionRange(curr_region);
            try_to_flush_region(curr_region_ptr);
            persist_and_sync(curr_region);
            {
                auto source_region = getRegion(source_region_id);
                // `source_region` is merged, don't remove its data in storage.
                removeRegion(
                    source_region_id,
                    /* remove_data */ false,
                    region_table,
                    task_lock,
                    region_manager.genRegionTaskLock(source_region_id));
            }
            {
                auto manage_lock = genRegionMgrWriteLock(task_lock);
                manage_lock.index.remove(result.ori_region_range->comparableKeys(), curr_region_id);
                manage_lock.index.add(curr_region_ptr);
            }
        };

        switch (result.type)
        {
        case RaftCommandResult::Type::IndexError:
        {
            if (type == raft_cmdpb::AdminCmdType::CommitMerge)
            {
                if (auto source_region = getRegion(request.commit_merge().source().id()); source_region)
                {
                    LOG_WARNING(
                        log,
                        "Admin cmd {} has been applied, try to remove source {}",
                        raft_cmdpb::AdminCmdType_Name(type),
                        source_region->toString(false));
                    source_region->setPendingRemove();
                    // `source_region` is merged, don't remove its data in storage.
                    removeRegion(
                        source_region->id(),
                        /* remove_data */ false,
                        region_table,
                        task_lock,
                        region_manager.genRegionTaskLock(source_region->id()));
                }
            }
            break;
        }
        case RaftCommandResult::Type::BatchSplit:
            handle_batch_split(result.split_regions);
            break;
        case RaftCommandResult::Type::Default:
            persist_and_sync(curr_region);
            break;
        case RaftCommandResult::Type::ChangePeer:
            handle_change_peer();
            break;
        case RaftCommandResult::Type::CommitMerge:
            handle_commit_merge(result.source_region_id);
            break;
        }

        return EngineStoreApplyRes::Persist;
    }
}

void WaitCheckRegionReady(
    const TMTContext & tmt,
    KVStore & kvstore,
    const std::atomic_size_t & terminate_signals_counter,
    double wait_tick_time,
    double max_wait_tick_time,
    double get_wait_region_ready_timeout_sec)
{
    constexpr double batch_read_index_time_rate = 0.2; // part of time for waiting shall be assigned to batch-read-index
    auto log = Logger::get(__FUNCTION__);

    LOG_INFO(
        log,
        "start to check regions ready, min-wait-tick {}s, max-wait-tick {}s, wait-region-ready-timeout {:.3f}s",
        wait_tick_time,
        max_wait_tick_time,
        get_wait_region_ready_timeout_sec);

    std::unordered_set<RegionID> remain_regions;
    std::unordered_map<RegionID, uint64_t> regions_to_check;
    Stopwatch region_check_watch;
    size_t total_regions_cnt = 0;
    {
        kvstore.traverseRegions(
            [&remain_regions](RegionID region_id, const RegionPtr &) { remain_regions.emplace(region_id); });
        total_regions_cnt = remain_regions.size();
    }
    while (region_check_watch.elapsedSeconds() < get_wait_region_ready_timeout_sec * batch_read_index_time_rate
           && terminate_signals_counter.load(std::memory_order_relaxed) == 0)
    {
        std::vector<kvrpcpb::ReadIndexRequest> batch_read_index_req;
        for (auto it = remain_regions.begin(); it != remain_regions.end();)
        {
            auto region_id = *it;
            if (auto region = kvstore.getRegion(region_id); region)
            {
                batch_read_index_req.emplace_back(GenRegionReadIndexReq(*region));
                it++;
            }
            else
            {
                it = remain_regions.erase(it);
            }
        }
        auto read_index_res = kvstore.batchReadIndex(batch_read_index_req, tmt.batchReadIndexTimeout());
        for (auto && [resp, region_id] : read_index_res)
        {
            bool need_retry = resp.read_index() == 0;
            if (resp.has_region_error())
            {
                const auto & region_error = resp.region_error();
                if (region_error.has_region_not_found() || region_error.has_epoch_not_match())
                    need_retry = false;
                LOG_DEBUG(
                    log,
                    "neglect error region_id={} not found {} epoch not match {}",
                    region_id,
                    region_error.has_region_not_found(),
                    region_error.has_epoch_not_match());
            }
            if (!need_retry)
            {
                // `read_index` can be zero if region error happens.
                // It is not worthy waiting applying and reading index again.
                // if region is able to get latest commit-index from TiKV, we should make it available only after it has caught up.
                regions_to_check.emplace(region_id, resp.read_index());
                remain_regions.erase(region_id);
            }
            else
            {
                // retry in next round
            }
        }
        if (remain_regions.empty())
            break;

        LOG_INFO(
            log,
            "{} regions need to fetch latest commit-index in next round, sleep for {:.3f}s",
            remain_regions.size(),
            wait_tick_time);
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<Int64>(wait_tick_time * 1000)));
        wait_tick_time = std::min(max_wait_tick_time, wait_tick_time * 2);
    }

    if (!remain_regions.empty())
    {
        FmtBuffer buffer;
        buffer.joinStr(
            remain_regions.begin(),
            remain_regions.end(),
            [&](const auto & e, FmtBuffer & b) { b.fmtAppend("{}", e); },
            " ");
        LOG_WARNING(
            log,
            "{} regions CANNOT fetch latest commit-index from TiKV, (region-id): {}",
            remain_regions.size(),
            buffer.toString());
    }
    do
    {
        for (auto it = regions_to_check.begin(); it != regions_to_check.end();)
        {
            auto [region_id, latest_index] = *it;
            if (auto region = kvstore.getRegion(region_id); region)
            {
                if (region->appliedIndex() >= latest_index)
                {
                    it = regions_to_check.erase(it);
                }
                else
                {
                    ++it;
                }
            }
            else
            {
                it = regions_to_check.erase(it);
            }
        }

        if (regions_to_check.empty())
            break;

        LOG_INFO(
            log,
            "{} regions need to apply to latest index, sleep for {:.3f}s",
            regions_to_check.size(),
            wait_tick_time);
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<Int64>(wait_tick_time * 1000)));
        wait_tick_time = std::min(max_wait_tick_time, wait_tick_time * 2);
    } while (region_check_watch.elapsedSeconds() < get_wait_region_ready_timeout_sec
             && terminate_signals_counter.load(std::memory_order_relaxed) == 0);

    if (!regions_to_check.empty())
    {
        FmtBuffer buffer;
        buffer.joinStr(
            regions_to_check.begin(),
            regions_to_check.end(),
            [&](const auto & e, FmtBuffer & b) {
                if (auto r = kvstore.getRegion(e.first); r)
                {
                    b.fmtAppend("{},{},{}", e.first, e.second, r->appliedIndex());
                }
                else
                {
                    b.fmtAppend("{},{},none", e.first, e.second);
                }
            },
            " ");
        LOG_WARNING(
            log,
            "{} regions CANNOT catch up with latest index, (region-id,latest-index,apply-index): {}",
            regions_to_check.size(),
            buffer.toString());
    }

    LOG_INFO(
        log,
        "finish to check {} regions, time cost {:.3f}s",
        total_regions_cnt,
        region_check_watch.elapsedSeconds());
}

void WaitCheckRegionReady(
    const TMTContext & tmt,
    KVStore & kvstore,
    const std::atomic_size_t & terminate_signals_counter)
{
    // wait interval to check region ready, not recommended to modify only if for tesing
    auto wait_region_ready_tick = tmt.getContext().getConfigRef().getUInt64("flash.wait_region_ready_tick", 0);
    auto wait_region_ready_timeout_sec = static_cast<double>(tmt.waitRegionReadyTimeout());
    const double max_wait_tick_time = 0 == wait_region_ready_tick ? 20.0 : wait_region_ready_timeout_sec;
    double min_wait_tick_time = 0 == wait_region_ready_tick
        ? 2.5
        : static_cast<double>(wait_region_ready_tick); // default tick in TiKV is about 2s (without hibernate-region)
    return WaitCheckRegionReady(
        tmt,
        kvstore,
        terminate_signals_counter,
        min_wait_tick_time,
        max_wait_tick_time,
        wait_region_ready_timeout_sec);
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

metapb::Store KVStore::getStoreMeta() const
{
    return getStore().getMeta();
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
    releaseReadIndexWorkers();
}

FileUsageStatistics KVStore::getFileUsageStatistics() const
{
    if (!region_persister)
    {
        return {};
    }

    return region_persister->getFileUsageStatistics();
}
} // namespace DB