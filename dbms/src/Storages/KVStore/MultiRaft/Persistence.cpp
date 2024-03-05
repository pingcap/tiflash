// Copyright 2024 PingCAP, Inc.
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

#include <Common/SyncPoint/SyncPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/RegionPersister.h>
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
        region.getDebugString(),
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
    // If region not found, throws.
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
} // namespace DB