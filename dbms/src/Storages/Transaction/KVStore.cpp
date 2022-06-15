#include <Common/setThreadName.h>
#include <Interpreters/Context.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>
#include <Storages/Transaction/BackgroundService.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionExecutionResult.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTContext.h>

#include <chrono>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int TABLE_IS_DROPPED;
} // namespace ErrorCodes

KVStore::KVStore(Context & context, TiDB::SnapshotApplyMethod snapshot_apply_method_)
    : region_persister(context, region_manager),
      raft_cmd_res(std::make_unique<RaftCommandResult>()),
      snapshot_apply_method(snapshot_apply_method_),
      log(&Logger::get("KVStore"))
{}

void KVStore::restore(const TiFlashRaftProxyHelper * proxy_helper)
{
    auto task_lock = genTaskLock();
    auto manage_lock = genRegionManageLock();

    this->proxy_helper = proxy_helper;
    regionsMut() = region_persister.restore(proxy_helper);

    std::stringstream ss;
    ss << "Restored " << regions().size() << " regions. ";

    // init range index
    for (const auto & [id, region] : regions())
    {
        std::ignore = id;
        region_range_index.add(region);
        ss << region->toString() << "; ";
    }

    LOG_INFO(log, ss.str());
}

RegionPtr KVStore::getRegion(const RegionID region_id) const
{
    auto manage_lock = genRegionManageLock();
    if (auto it = regions().find(region_id); it != regions().end())
        return it->second;
    return nullptr;
}

void KVStore::handleRegionsByRangeOverlap(
    const RegionRange & range, std::function<void(RegionMap, const KVStoreTaskLock &)> && callback) const
{
    auto task_lock = genTaskLock();
    callback(region_range_index.findByRangeOverlap(range), task_lock);
}

const RegionManager::RegionTaskElement & RegionManager::getRegionTaskCtrl(const RegionID region_id) const
{
    std::lock_guard<std::mutex> lock(mutex);

    if (auto it = regions_ctrl.find(region_id); it != regions_ctrl.end())
        return it->second;

    return regions_ctrl.try_emplace(region_id).first->second;
}

RegionTaskLock RegionManager::genRegionTaskLock(const RegionID region_id) const
{
    return RegionTaskLock(getRegionTaskCtrl(region_id).mutex);
}

size_t KVStore::regionSize() const
{
    auto manage_lock = genRegionManageLock();
    return regions().size();
}

void KVStore::traverseRegions(std::function<void(RegionID, const RegionPtr &)> && callback) const
{
    auto manage_lock = genRegionManageLock();
    for (auto it = regions().begin(); it != regions().end(); ++it)
        callback(it->first, it->second);
}

void KVStore::tryFlushRegionCacheInStorage(TMTContext & tmt, const Region & region, Poco::Logger * log)
{
    if (tmt.isBgFlushDisabled())
    {
        auto table_id = region.getMappedTableID();
        auto storage = tmt.getStorages().get(table_id);
        if (storage == nullptr)
        {
            LOG_WARNING(log,
                "tryFlushRegionCacheInStorage can not get table for region:" + region.toString()
                    + " with table id: " + DB::toString(table_id) + ", ignored");
            return;
        }

        try
        {
            // Acquire `drop_lock` so that no other threads can drop the storage during `flushCache`. `alter_lock` is not required.
            auto storage_lock = storage->lockForShare(getThreadName());
            auto rowkey_range = DM::RowKeyRange::fromRegionRange(
                region.getRange(), region.getRange()->getMappedTableID(), storage->isCommonHandle(), storage->getRowKeyColumnSize());
            storage->flushCache(tmt.getContext(), rowkey_range);
        }
        catch (DB::Exception & e)
        {
            // We can ignore if storage is already dropped.
            if (e.code() != ErrorCodes::TABLE_IS_DROPPED)
                throw;
        }
    }
}

void KVStore::tryPersist(const RegionID region_id)
{
    auto region = getRegion(region_id);
    if (region)
    {
        LOG_INFO(log, "Try to persist " << region->toString(false));
        region_persister.persist(*region);
        LOG_INFO(log, "After persisted " << region->toString(false) << ", cache " << region->dataSize() << " bytes");
    }
}

void KVStore::gcRegionPersistedCache(Seconds gc_persist_period)
{
    {
        decltype(bg_gc_region_data) tmp;
        std::lock_guard<std::mutex> lock(bg_gc_region_data_mutex);
        tmp.swap(bg_gc_region_data);
    }
    Timepoint now = Clock::now();
    if (now < (last_gc_time.load() + gc_persist_period))
        return;
    last_gc_time = now;
    region_persister.gc();
}

void KVStore::mockRemoveRegion(const DB::RegionID region_id, RegionTable & region_table)
{
    auto task_lock = genTaskLock();
    auto region_lock = region_manager.genRegionTaskLock(region_id);
    // mock remove region should remove data by default
    removeRegion(region_id, /* remove_data */ true, region_table, task_lock, region_lock);
}

void KVStore::removeRegion(const RegionID region_id, bool remove_data, RegionTable & region_table, const KVStoreTaskLock & task_lock,
    const RegionTaskLock & region_lock)
{
    LOG_INFO(log, "Start to remove [region " << region_id << "]");

    RegionPtr region;
    {
        auto manage_lock = genRegionManageLock();
        auto it = regions().find(region_id);
        region = it->second;
        regionsMut().erase(it);
    }
    {
        // remove index
        region_range_index.remove(region->makeRaftCommandDelegate(task_lock).getRange().comparableKeys(), region_id);
    }

    region_persister.drop(region_id, region_lock);
    LOG_INFO(log, "Persisted [region " << region_id << "] deleted");

    region_table.removeRegion(region_id, remove_data, region_lock);

    LOG_INFO(log, "Remove [region " << region_id << "] done");
}

KVStoreTaskLock KVStore::genTaskLock() const { return KVStoreTaskLock(task_mutex); }
RegionMap & KVStore::regionsMut() { return region_manager.regions; }
const RegionMap & KVStore::regions() const { return region_manager.regions; }
KVStore::RegionManageLock KVStore::genRegionManageLock() const { return RegionManageLock(region_manager.mutex); }

EngineStoreApplyRes KVStore::handleWriteRaftCmd(
    raft_cmdpb::RaftCmdRequest && request, UInt64 region_id, UInt64 index, UInt64 term, TMTContext & tmt)
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
                break;
        }
    }
    return handleWriteRaftCmd(
        WriteCmdsView{.keys = keys.data(), .vals = vals.data(), .cmd_types = cmd_types.data(), .cmd_cf = cmd_cf.data(), .len = keys.size()},
        region_id, index, term, tmt);
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

void KVStore::handleDestroy(UInt64 region_id, TMTContext & tmt) { handleDestroy(region_id, tmt, genTaskLock()); }

void KVStore::handleDestroy(UInt64 region_id, TMTContext & tmt, const KVStoreTaskLock & task_lock)
{
    const auto region = getRegion(region_id);
    if (region == nullptr)
    {
        LOG_INFO(log, __PRETTY_FUNCTION__ << ": [region " << region_id << "] is not found, might be removed already");
        return;
    }
    LOG_INFO(log, "Handle destroy " << region->toString());
    region->setPendingRemove();
    removeRegion(region_id, /* remove_data */ true, tmt.getRegionTable(), task_lock, region_manager.genRegionTaskLock(region_id));
}

void KVStore::setRegionCompactLogConfig(UInt64 sec, UInt64 rows, UInt64 bytes)
{
    REGION_COMPACT_LOG_PERIOD = sec;
    REGION_COMPACT_LOG_MIN_ROWS = rows;
    REGION_COMPACT_LOG_MIN_BYTES = bytes;

    LOG_INFO(log,
        __FUNCTION__ << ": threshold config: "
                     << "period " << sec << ", rows " << rows << ", bytes " << bytes);
}

void KVStore::persistRegion(const Region & region, const RegionTaskLock & region_task_lock, const char * caller)
{
    LOG_INFO(log, "Start to persist " << region.toString(true) << ", cache size: " << region.dataSize() << " bytes for `" << caller << '`');
    region_persister.persist(region, region_task_lock);
    LOG_DEBUG(log, "Persist " << region.toString(false) << " done");
}

EngineStoreApplyRes KVStore::handleUselessAdminRaftCmd(
    raft_cmdpb::AdminCmdType cmd_type, UInt64 curr_region_id, UInt64 index, UInt64 term, TMTContext & tmt)
{
    auto region_task_lock = region_manager.genRegionTaskLock(curr_region_id);
    const RegionPtr curr_region_ptr = getRegion(curr_region_id);
    if (curr_region_ptr == nullptr)
    {
        return EngineStoreApplyRes::NotFound;
    }

    auto & curr_region = *curr_region_ptr;

    LOG_DEBUG(log,
        curr_region.toString(false) << " handle ignorable admin command " << raft_cmdpb::AdminCmdType_Name(cmd_type)
                                    << " at [term: " << term << ", index: " << index << "]");

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

            LOG_DEBUG(log, curr_region.toString(false) << " approx mem cache info: rows " << rows << ", bytes " << size_bytes);

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
                auto compact_log_period = std::rand() % REGION_COMPACT_LOG_PERIOD.load(std::memory_order_relaxed);
                if (curr_region.lastCompactLogTime() + Seconds{compact_log_period} > Clock::now())
                    return false;
                else
                    return true;
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
            LOG_WARNING(log,
                __PRETTY_FUNCTION__ << ": [region " << curr_region_id << "] is not found at [term " << term << ", index " << index
                                    << ", cmd " << raft_cmdpb::AdminCmdType_Name(type) << "], might be removed already");
            return EngineStoreApplyRes::NotFound;
        }

        auto & curr_region = *curr_region_ptr;
        curr_region.makeRaftCommandDelegate(task_lock).handleAdminRaftCmd(
            request, response, index, term, *this, region_table, *raft_cmd_res);
        RaftCommandResult & result = *raft_cmd_res;

        // After region split / merge, try to flush it
        const auto try_to_flush_region = [&tmt](const RegionPtr & region) {
            if (tmt.isBgFlushDisabled())
            {
                try
                {
                    tmt.getRegionTable().tryFlushRegion(region, false);
                }
                catch (const Exception & e)
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                }
            }
            else
            {
                if (region->writeCFCount() >= 8192)
                    tmt.getBackgroundService().addRegionToFlush(region);
            }
        };

        const auto persist_and_sync = [&](const Region & region) {
            tryFlushRegionCacheInStorage(tmt, region, log);
            persistRegion(region, region_task_lock, "admin raft cmd");
        };

        const auto handle_batch_split = [&](Regions & split_regions) {
            {
                auto manage_lock = genRegionManageLock();

                for (auto & new_region : split_regions)
                {
                    auto [it, ok] = regionsMut().emplace(new_region->id(), new_region);
                    if (!ok)
                    {
                        // definitely, any region's index is greater or equal than the initial one.

                        // if there is already a region with same id, it means program crashed while persisting.
                        // just use the previous one.
                        new_region = it->second;
                    }
                }
            }

            {
                region_range_index.remove(result.ori_region_range->comparableKeys(), curr_region_id);
                region_range_index.add(curr_region_ptr);

                for (auto & new_region : split_regions)
                    region_range_index.add(new_region);
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
                    source_region_id, /* remove_data */ false, region_table, task_lock, region_manager.genRegionTaskLock(source_region_id));
            }
            region_range_index.remove(result.ori_region_range->comparableKeys(), curr_region_id);
            region_range_index.add(curr_region_ptr);
        };

        switch (result.type)
        {
            case RaftCommandResult::Type::IndexError:
            {
                if (type == raft_cmdpb::AdminCmdType::CommitMerge)
                {
                    if (auto source_region = getRegion(request.commit_merge().source().id()); source_region)
                    {
                        LOG_WARNING(log,
                            "Admin cmd " << raft_cmdpb::AdminCmdType_Name(type) << " has been applied, try to remove source "
                                         << source_region->toString(false));
                        source_region->setPendingRemove();
                        // `source_region` is merged, don't remove its data in storage.
                        removeRegion(source_region->id(), /* remove_data */ false, region_table, task_lock,
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
            default:
                throw Exception("Unsupported RaftCommandResult", ErrorCodes::LOGICAL_ERROR);
        }

        return EngineStoreApplyRes::Persist;
    }
}

void WaitCheckRegionReady(const TMTContext & tmt, const std::atomic_size_t & terminate_signals_counter)
{
    constexpr double BATCH_READ_INDEX_TIME_RATE = 0.2; // part of time for waiting shall be assigned to batch-read-index
    constexpr double MAX_SLEEP_TIME = 20;

    Logger * log = &Logger::get(__FUNCTION__);

    LOG_INFO(log, "start to check regions ready");

    std::unordered_set<RegionID> remain_regions;
    std::unordered_map<RegionID, uint64_t> regions_to_check;
    Stopwatch region_check_watch;
    size_t total_regions_cnt = 0;
    double sleep_time = 2.5; // default tick in TiKV is about 2s (without hibernate-region)
    {
        tmt.getKVStore()->traverseRegions([&remain_regions](RegionID region_id, const RegionPtr &) { remain_regions.emplace(region_id); });
        total_regions_cnt = remain_regions.size();
    }
    while (region_check_watch.elapsedSeconds() < tmt.waitRegionReadyTimeout() * BATCH_READ_INDEX_TIME_RATE
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
        auto read_index_res = tmt.getKVStore()->getProxyHelper()->batchReadIndex(batch_read_index_req, tmt.batchReadIndexTimeout());
        for (auto && [resp, region_id] : read_index_res)
        {
            bool need_retry = resp.read_index() == 0;
            if (resp.has_region_error())
            {
                auto & region_error = resp.region_error();
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

        LOG_INFO(log, remain_regions.size() << " regions need to fetch latest commit-index in next round, sleep for " << sleep_time << "s");
        std::this_thread::sleep_for(std::chrono::milliseconds(int64_t(sleep_time * 1000)));
        sleep_time = std::min(MAX_SLEEP_TIME, sleep_time * 2);
    }

    if (!remain_regions.empty())
    {
        LOG_WARNING(
            log, remain_regions.size() << " regions CANNOT fetch latest commit-index from TiKV, (region-id): "; do {
                for (auto && r : remain_regions)
                {
                    oss_internal_rare << "(" << r << ") ";
                }
            } while (0));
    }
    while (region_check_watch.elapsedSeconds() < (double)tmt.waitRegionReadyTimeout()
        && terminate_signals_counter.load(std::memory_order_relaxed) == 0)
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

        LOG_INFO(log, regions_to_check.size() << " regions need to apply to latest index, sleep for " << sleep_time << "s");
        std::this_thread::sleep_for(std::chrono::milliseconds(int64_t(sleep_time * 1000)));
        sleep_time = std::min(MAX_SLEEP_TIME, sleep_time * 2);
    }
    if (!regions_to_check.empty())
    {
        LOG_WARNING(
            log, regions_to_check.size() << " regions CANNOT catch up with latest index, (region-id,latest-index): "; do {
                for (auto && [region_id, latest_index] : regions_to_check)
                {
                    oss_internal_rare << "(" << region_id << "," << latest_index << ") ";
                }
            } while (0));
    }

    LOG_INFO(log, "finish to check " << total_regions_cnt << " regions, time cost " << region_check_watch.elapsedSeconds() << "s");
}

} // namespace DB
