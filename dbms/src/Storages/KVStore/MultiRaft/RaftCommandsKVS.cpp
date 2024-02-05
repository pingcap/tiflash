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
#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>
#include <Interpreters/Context.h>
#include <RaftStoreProxyFFI/ProxyFFI.h>
#include <Storages/KVStore/Decode/RegionTable.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/RegionExecutionResult.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Types.h>
#include <Storages/StorageDeltaMergeHelpers.h>
#include <common/likely.h>

namespace DB
{

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

EngineStoreApplyRes KVStore::handleWriteRaftCmdInner(
    const WriteCmdsView & cmds,
    UInt64 region_id,
    UInt64 index,
    UInt64 term,
    TMTContext & tmt,
    DM::WriteResult & write_result)
{
    EngineStoreApplyRes apply_res;
    {
        auto region_persist_lock = region_manager.genRegionTaskLock(region_id);

        const RegionPtr region = getRegion(region_id);
        if (region == nullptr)
        {
            return EngineStoreApplyRes::NotFound;
        }

        std::tie(apply_res, write_result) = region->handleWriteRaftCmd(cmds, index, term, tmt);

        if unlikely (region->getClusterRaftstoreVer() == RaftstoreVer::V2)
        {
            region->orphanKeysInfo().advanceAppliedIndex(index);
        }

        if (tryRegisterEagerRaftLogGCTask(region, region_persist_lock))
        {
            /// We should execute eager RaftLog GC, persist the Region in both TiFlash and proxy
            // Persist RegionMeta on the storage engine
            tryFlushRegionCacheInStorage(tmt, *region, Logger::get());
            persistRegion(*region, region_persist_lock, PersistRegionReason::EagerRaftGc, "");
            // return "Persist" to proxy for persisting the RegionMeta
            apply_res = EngineStoreApplyRes::Persist;
        }
    }
    /// Safety:
    /// This call is from Proxy's applying thread of this region, so:
    /// 1. No other thread can write from raft to this region even if we unlocked here.
    /// 2. If `proactiveFlushCacheAndRegion` causes a write stall, it will be forwarded to raft layer.
    // TODO(proactive flush)
    return apply_res;
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

    if unlikely (curr_region.getClusterRaftstoreVer() == RaftstoreVer::V2)
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
    if (cmd_type == raft_cmdpb::AdminCmdType::PrepareFlashback //
        || cmd_type == raft_cmdpb::AdminCmdType::FinishFlashback
        || cmd_type == raft_cmdpb::AdminCmdType::BatchSwitchWitness)
    {
        tryFlushRegionCacheInStorage(tmt, curr_region, log);
        persistRegion(
            curr_region,
            region_task_lock,
            PersistRegionReason::UselessAdminCommand,
            raft_cmdpb::AdminCmdType_Name(cmd_type).c_str());
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
    auto type = request.cmd_type();
    SCOPE_EXIT({
        GET_METRIC(tiflash_raft_apply_write_command_duration_seconds, type_admin).Observe(watch.elapsedSeconds());
        switch (type)
        {
        case raft_cmdpb::AdminCmdType::ChangePeer:
        case raft_cmdpb::AdminCmdType::ChangePeerV2:
            GET_METRIC(tiflash_raft_apply_write_command_duration_seconds, type_admin_change_peer)
                .Observe(watch.elapsedSeconds());
            break;
        case raft_cmdpb::AdminCmdType::BatchSplit:
            GET_METRIC(tiflash_raft_apply_write_command_duration_seconds, type_admin_batch_split)
                .Observe(watch.elapsedSeconds());
            break;
        case raft_cmdpb::AdminCmdType::PrepareMerge:
            GET_METRIC(tiflash_raft_apply_write_command_duration_seconds, type_admin_prepare_merge)
                .Observe(watch.elapsedSeconds());
            break;
        case raft_cmdpb::AdminCmdType::CommitMerge:
            GET_METRIC(tiflash_raft_apply_write_command_duration_seconds, type_admin_commit_merge)
                .Observe(watch.elapsedSeconds());
            break;
        default:
            break;
        }
    });
    switch (type)
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
        if unlikely (curr_region.getClusterRaftstoreVer() == RaftstoreVer::V2)
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
                tmt.getRegionTable().tryWriteBlockByRegion(region);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        };

        const auto persist_and_sync = [&](const Region & region) {
            tryFlushRegionCacheInStorage(tmt, region, log);
            persistRegion(region, region_task_lock, PersistRegionReason::AdminCommand, "");
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
} // namespace DB
