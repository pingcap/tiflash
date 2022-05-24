// Copyright 2022 PingCAP, Ltd.
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
#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>
#include <Interpreters/Context.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>
#include <Storages/Transaction/BackgroundService.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/ReadIndexWorker.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionExecutionResult.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/likely.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int TABLE_IS_DROPPED;
} // namespace ErrorCodes

KVStore::KVStore(Context & context, TiDB::SnapshotApplyMethod snapshot_apply_method_)
    : region_persister(context, region_manager)
    , raft_cmd_res(std::make_unique<RaftCommandResult>())
    , snapshot_apply_method(snapshot_apply_method_)
    , log(&Poco::Logger::get("KVStore"))
{
    // default config about compact-log: period 120s, rows 40k, bytes 32MB.
    REGION_COMPACT_LOG_PERIOD = 120;
    REGION_COMPACT_LOG_MIN_ROWS = 40 * 1024;
    REGION_COMPACT_LOG_MIN_BYTES = 32 * 1024 * 1024;
}

void KVStore::restore(const TiFlashRaftProxyHelper * proxy_helper)
{
    auto task_lock = genTaskLock();
    auto manage_lock = genRegionWriteLock(task_lock);

    this->proxy_helper = proxy_helper;
    manage_lock.regions = region_persister.restore(proxy_helper);

    LOG_FMT_INFO(log, "Restored {} regions", manage_lock.regions.size());

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
                LOG_FMT_INFO(log, "{}", str);
        }
    }
}

RegionPtr KVStore::getRegion(RegionID region_id) const
{
    auto manage_lock = genRegionReadLock();
    if (auto it = manage_lock.regions.find(region_id); it != manage_lock.regions.end())
        return it->second;
    return nullptr;
}
RegionMap KVStore::getRegionsByRangeOverlap(const RegionRange & range) const
{
    auto manage_lock = genRegionReadLock();
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
    auto manage_lock = genRegionReadLock();
    return manage_lock.regions.size();
}

void KVStore::traverseRegions(std::function<void(RegionID, const RegionPtr &)> && callback) const
{
    auto manage_lock = genRegionReadLock();
    for (const auto & region : manage_lock.regions)
        callback(region.first, region.second);
}

void KVStore::tryFlushRegionCacheInStorage(TMTContext & tmt, const Region & region, Poco::Logger * log)
{
    auto table_id = region.getMappedTableID();
    auto storage = tmt.getStorages().get(table_id);
    if (unlikely(storage == nullptr))
    {
        LOG_FMT_WARNING(log,
                        "tryFlushRegionCacheInStorage can not get table for region {} with table id {}, ignored",
                        region.toString(),
                        table_id);
        return;
    }

    try
    {
        // Acquire `drop_lock` so that no other threads can drop the storage during `flushCache`. `alter_lock` is not required.
        auto storage_lock = storage->lockForShare(getThreadName());
        auto rowkey_range = DM::RowKeyRange::fromRegionRange(
            region.getRange(),
            region.getRange()->getMappedTableID(),
            storage->isCommonHandle(),
            storage->getRowKeyColumnSize());
        storage->flushCache(tmt.getContext(), rowkey_range);
    }
    catch (DB::Exception & e)
    {
        // We can ignore if storage is already dropped.
        if (e.code() != ErrorCodes::TABLE_IS_DROPPED)
            throw;
    }
}

void KVStore::tryPersist(RegionID region_id)
{
    auto region = getRegion(region_id);
    if (region)
    {
        LOG_FMT_INFO(log, "Try to persist {}", region->toString(false));
        region_persister.persist(*region);
        LOG_FMT_INFO(log, "After persisted {}, cache {} bytes", region->toString(false), region->dataSize());
    }
}

void KVStore::gcRegionPersistedCache(Seconds gc_persist_period)
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
    region_persister.gc();
}

void KVStore::removeRegion(RegionID region_id, bool remove_data, RegionTable & region_table, const KVStoreTaskLock & task_lock, const RegionTaskLock & region_lock)
{
    LOG_FMT_INFO(log, "Start to remove [region {}]", region_id);

    {
        auto manage_lock = genRegionWriteLock(task_lock);
        auto it = manage_lock.regions.find(region_id);
        manage_lock.index.remove(it->second->makeRaftCommandDelegate(task_lock).getRange().comparableKeys(), region_id); // remove index
        manage_lock.regions.erase(it);
    }
    {
        if (read_index_worker_manager) //std::atomic_thread_fence will protect it
        {
            // remove cache & read-index task
            read_index_worker_manager->getWorkerByRegion(region_id).removeRegion(region_id);
        }
    }

    region_persister.drop(region_id, region_lock);
    LOG_FMT_INFO(log, "Persisted [region {}] deleted", region_id);

    region_table.removeRegion(region_id, remove_data, region_lock);

    LOG_FMT_INFO(log, "Remove [region {}] done", region_id);
}

KVStoreTaskLock KVStore::genTaskLock() const
{
    return KVStoreTaskLock(task_mutex);
}

RegionManager::RegionReadLock KVStore::genRegionReadLock() const
{
    return region_manager.genRegionReadLock();
}

RegionManager::RegionWriteLock KVStore::genRegionWriteLock(const KVStoreTaskLock &)
{
    return region_manager.genRegionWriteLock();
}

EngineStoreApplyRes KVStore::handleWriteRaftCmd(
    raft_cmdpb::RaftCmdRequest && request,
    UInt64 region_id,
    UInt64 index,
    UInt64 term,
    TMTContext & tmt)
{
    std::vector<BaseBuffView> keys;
    std::vector<BaseBuffView> vals;
    std::vector<WriteCmdType> cmd_types;
    std::vector<ColumnFamilyType> cmd_cf;
    keys.reserve(request.requests_size());
    vals.reserve(request.requests_size());
    cmd_types.reserve(request.requests_size());
    cmd_cf.reserve(request.requests_size());

    for (const auto & req : request.requests())
    {
        auto type = req.cmd_type();

        switch (type)
        {
        case raft_cmdpb::CmdType::Put:
            keys.push_back({req.put().key().data(), req.put().key().size()});
            vals.push_back({req.put().value().data(), req.put().value().size()});
            cmd_types.push_back(WriteCmdType::Put);
            cmd_cf.push_back(NameToCF(req.put().cf()));
            break;
        case raft_cmdpb::CmdType::Delete:
            keys.push_back({req.delete_().key().data(), req.delete_().key().size()});
            vals.push_back({nullptr, 0});
            cmd_types.push_back(WriteCmdType::Del);
            cmd_cf.push_back(NameToCF(req.delete_().cf()));
            break;
        default:
            throw Exception(fmt::format("Unsupport raft cmd {}", raft_cmdpb::CmdType_Name(type)), ErrorCodes::LOGICAL_ERROR);
        }
    }
    return handleWriteRaftCmd(
        WriteCmdsView{.keys = keys.data(), .vals = vals.data(), .cmd_types = cmd_types.data(), .cmd_cf = cmd_cf.data(), .len = keys.size()},
        region_id,
        index,
        term,
        tmt);
}

EngineStoreApplyRes KVStore::handleWriteRaftCmd(const WriteCmdsView & cmds, UInt64 region_id, UInt64 index, UInt64 term, TMTContext & tmt)
{
    auto region_persist_lock = region_manager.genRegionTaskLock(region_id);

    const RegionPtr region = getRegion(region_id);
    if (region == nullptr)
    {
        return EngineStoreApplyRes::NotFound;
    }

    auto res = region->handleWriteRaftCmd(cmds, index, term, tmt);
    return res;
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
        LOG_FMT_INFO(log, "[region {}] is not found, might be removed already", region_id);
        return;
    }
    LOG_FMT_INFO(log, "Handle destroy {}", region->toString());
    region->setPendingRemove();
    removeRegion(region_id, /* remove_data */ true, tmt.getRegionTable(), task_lock, region_manager.genRegionTaskLock(region_id));
}

void KVStore::setRegionCompactLogConfig(UInt64 sec, UInt64 rows, UInt64 bytes)
{
    REGION_COMPACT_LOG_PERIOD = sec;
    REGION_COMPACT_LOG_MIN_ROWS = rows;
    REGION_COMPACT_LOG_MIN_BYTES = bytes;

    LOG_FMT_INFO(
        log,
        "threshold config: period {}, rows {}, bytes {}",
        sec,
        rows,
        bytes);
}

void KVStore::persistRegion(const Region & region, const RegionTaskLock & region_task_lock, const char * caller)
{
    LOG_FMT_INFO(log, "Start to persist {}, cache size: {} bytes for `{}`", region.toString(true), region.dataSize(), caller);
    region_persister.persist(region, region_task_lock);
    LOG_FMT_DEBUG(log, "Persist {} done", region.toString(false));
}

EngineStoreApplyRes KVStore::handleUselessAdminRaftCmd(
    raft_cmdpb::AdminCmdType cmd_type,
    UInt64 curr_region_id,
    UInt64 index,
    UInt64 term,
    TMTContext & tmt)
{
    auto region_task_lock = region_manager.genRegionTaskLock(curr_region_id);
    const RegionPtr curr_region_ptr = getRegion(curr_region_id);
    if (curr_region_ptr == nullptr)
    {
        return EngineStoreApplyRes::NotFound;
    }

    auto & curr_region = *curr_region_ptr;

    LOG_FMT_DEBUG(log,
                  "{} handle ignorable admin command {} at [term: {}, index: {}]",
                  curr_region.toString(false),
                  raft_cmdpb::AdminCmdType_Name(cmd_type),
                  term,
                  index);

    curr_region.handleWriteRaftCmd({}, index, term, tmt);

    const auto check_sync_log = [&]() {
        if (cmd_type != raft_cmdpb::AdminCmdType::CompactLog)
        {
            // ignore ComputeHash, VerifyHash or other useless cmd.
            return false;
        }
        else
        {
            auto [rows, size_bytes] = curr_region.getApproxMemCacheInfo();

            LOG_FMT_DEBUG(log, "{} approx mem cache info: rows {}, bytes {}", curr_region.toString(false), rows, size_bytes);

            if (rows >= REGION_COMPACT_LOG_MIN_ROWS.load(std::memory_order_relaxed)
                || size_bytes >= REGION_COMPACT_LOG_MIN_BYTES.load(std::memory_order_relaxed))
            {
                // if rows or bytes more than threshold, flush cache and perist mem data.
                return true;
            }
            else
            {
                // if thhere is little data in mem, wait until time interval reached threshold.
                // use random period so that lots of regions will not be persisted at same time.
                auto compact_log_period = std::rand() % REGION_COMPACT_LOG_PERIOD.load(std::memory_order_relaxed); // NOLINT
                return !(curr_region.lastCompactLogTime() + Seconds{compact_log_period} > Clock::now());
            }
        }
    };

    if (check_sync_log())
    {
        tryFlushRegionCacheInStorage(tmt, curr_region, log);
        persistRegion(curr_region, region_task_lock, "compact raft log");
        curr_region.markCompactLog();
        curr_region.cleanApproxMemCacheInfo();
        return EngineStoreApplyRes::Persist;
    }
    return EngineStoreApplyRes::None;
}

EngineStoreApplyRes KVStore::handleAdminRaftCmd(raft_cmdpb::AdminRequest && request,
                                                raft_cmdpb::AdminResponse && response,
                                                UInt64 curr_region_id,
                                                UInt64 index,
                                                UInt64 term,
                                                TMTContext & tmt)
{
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_raft_apply_write_command_duration_seconds, type_admin).Observe(watch.elapsedSeconds());
    });
    auto type = request.cmd_type();
    switch (request.cmd_type())
    {
    // CompactLog | VerifyHash | ComputeHash won't change region meta, there is no need to occupy task lock of kvstore.
    case raft_cmdpb::AdminCmdType::CompactLog:
    case raft_cmdpb::AdminCmdType::VerifyHash:
    case raft_cmdpb::AdminCmdType::ComputeHash:
        return handleUselessAdminRaftCmd(type, curr_region_id, index, term, tmt);
    default:
        break;
    }

    RegionTable & region_table = tmt.getRegionTable();

    auto task_lock = genTaskLock();

    {
        auto region_task_lock = region_manager.genRegionTaskLock(curr_region_id);
        const RegionPtr curr_region_ptr = getRegion(curr_region_id);
        if (curr_region_ptr == nullptr)
        {
            LOG_FMT_WARNING(log,
                            "[region {}] is not found at [term {}, index {}, cmd {}], might be removed already",
                            curr_region_id,
                            term,
                            index,
                            raft_cmdpb::AdminCmdType_Name(type));
            return EngineStoreApplyRes::NotFound;
        }

        auto & curr_region = *curr_region_ptr;
        curr_region.makeRaftCommandDelegate(task_lock).handleAdminRaftCmd(
            request,
            response,
            index,
            term,
            *this,
            region_table,
            *raft_cmd_res);
        RaftCommandResult & result = *raft_cmd_res;

        // After region split / merge, try to flush it
        const auto try_to_flush_region = [&tmt](const RegionPtr & region) {
            try
            {
                tmt.getRegionTable().tryFlushRegion(region, false);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        };

        const auto persist_and_sync = [&](const Region & region) {
            tryFlushRegionCacheInStorage(tmt, region, log);
            persistRegion(region, region_task_lock, "admin raft cmd");
        };

        const auto handle_batch_split = [&](Regions & split_regions) {
            {
                auto manage_lock = genRegionWriteLock(task_lock);

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
                auto manage_lock = genRegionWriteLock(task_lock);
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
                    LOG_FMT_WARNING(log,
                                    "Admin cmd {} has been applied, try to remove source {}",
                                    raft_cmdpb::AdminCmdType_Name(type),
                                    source_region->toString(false));
                    source_region->setPendingRemove();
                    // `source_region` is merged, don't remove its data in storage.
                    removeRegion(source_region->id(), /* remove_data */ false, region_table, task_lock, region_manager.genRegionTaskLock(source_region->id()));
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
    const std::atomic_size_t & terminate_signals_counter,
    double wait_tick_time,
    double max_wait_tick_time,
    double get_wait_region_ready_timeout_sec)
{
    constexpr double batch_read_index_time_rate = 0.2; // part of time for waiting shall be assigned to batch-read-index
    Poco::Logger * log = &Poco::Logger::get(__FUNCTION__);

    LOG_FMT_INFO(log,
                 "start to check regions ready, min-wait-tick {}s, max-wait-tick {}s, wait-region-ready-timeout {:.3f}s",
                 wait_tick_time,
                 max_wait_tick_time,
                 get_wait_region_ready_timeout_sec);

    std::unordered_set<RegionID> remain_regions;
    std::unordered_map<RegionID, uint64_t> regions_to_check;
    Stopwatch region_check_watch;
    size_t total_regions_cnt = 0;
    {
        tmt.getKVStore()->traverseRegions([&remain_regions](RegionID region_id, const RegionPtr &) { remain_regions.emplace(region_id); });
        total_regions_cnt = remain_regions.size();
    }
    while (region_check_watch.elapsedSeconds() < get_wait_region_ready_timeout_sec * batch_read_index_time_rate
           && terminate_signals_counter.load(std::memory_order_relaxed) == 0)
    {
        std::vector<kvrpcpb::ReadIndexRequest> batch_read_index_req;
        for (auto it = remain_regions.begin(); it != remain_regions.end();)
        {
            auto region_id = *it;
            if (auto region = tmt.getKVStore()->getRegion(region_id); region)
            {
                batch_read_index_req.emplace_back(GenRegionReadIndexReq(*region));
                it++;
            }
            else
            {
                it = remain_regions.erase(it);
            }
        }
        auto read_index_res = tmt.getKVStore()->batchReadIndex(batch_read_index_req, tmt.batchReadIndexTimeout());
        for (auto && [resp, region_id] : read_index_res)
        {
            bool need_retry = resp.read_index() == 0;
            if (resp.has_region_error())
            {
                const auto & region_error = resp.region_error();
                if (region_error.has_region_not_found() || region_error.has_epoch_not_match())
                    need_retry = false;
            }
            if (!need_retry)
            {
                // if region is able to get latest commit-index from TiKV, we should make it available only after it has caught up.
                assert(resp.read_index() != 0);
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

        LOG_FMT_INFO(log,
                     "{} regions need to fetch latest commit-index in next round, sleep for {:.3f}s",
                     remain_regions.size(),
                     wait_tick_time);
        std::this_thread::sleep_for(std::chrono::milliseconds(int64_t(wait_tick_time * 1000)));
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
        LOG_FMT_WARNING(
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
            if (auto region = tmt.getKVStore()->getRegion(region_id); region)
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

        LOG_FMT_INFO(log,
                     "{} regions need to apply to latest index, sleep for {:.3f}s",
                     regions_to_check.size(),
                     wait_tick_time);
        std::this_thread::sleep_for(std::chrono::milliseconds(int64_t(wait_tick_time * 1000)));
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
                if (auto r = tmt.getKVStore()->getRegion(e.first); r)
                {
                    b.fmtAppend("{},{},{}", e.first, e.second, r->appliedIndex());
                }
                else
                {
                    b.fmtAppend("{},{},none", e.first, e.second);
                }
            },
            " ");
        LOG_FMT_WARNING(log, "{} regions CANNOT catch up with latest index, (region-id,latest-index,apply-index): {}", regions_to_check.size(), buffer.toString());
    }

    LOG_FMT_INFO(log,
                 "finish to check {} regions, time cost {:.3f}s",
                 total_regions_cnt,
                 region_check_watch.elapsedSeconds());
}

void WaitCheckRegionReady(const TMTContext & tmt, const std::atomic_size_t & terminate_signals_counter)
{
    // wait interval to check region ready, not recommended to modify only if for tesing
    auto wait_region_ready_tick = tmt.getContext().getConfigRef().getUInt64("flash.wait_region_ready_tick", 0);
    auto wait_region_ready_timeout_sec = static_cast<double>(tmt.waitRegionReadyTimeout());
    const double max_wait_tick_time = 0 == wait_region_ready_tick ? 20.0 : wait_region_ready_timeout_sec;
    double min_wait_tick_time = 0 == wait_region_ready_tick ? 2.5 : static_cast<double>(wait_region_ready_tick); // default tick in TiKV is about 2s (without hibernate-region)
    return WaitCheckRegionReady(tmt, terminate_signals_counter, min_wait_tick_time, max_wait_tick_time, wait_region_ready_timeout_sec);
}

void KVStore::setStore(metapb::Store store_)
{
    getStore().update(std::move(store_));
    LOG_FMT_INFO(log, "Set store info {}", getStore().base.ShortDebugString());
}

uint64_t KVStore::getStoreID(std::memory_order memory_order) const
{
    return getStore().store_id.load(memory_order);
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
    base = std::move(base_);
    store_id = base.id();
}

KVStore::~KVStore()
{
    releaseReadIndexWorkers();
}

} // namespace DB
