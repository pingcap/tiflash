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

#include <Common/FailPoint.h>
#include <Common/FmtUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/DeltaMergeInterfaces.h>
#include <Storages/KVStore/Decode/RegionTable.h>
#include <Storages/KVStore/Decode/TiKVRange.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/FFI/SSTReader.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/RegionExecutionResult.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <common/logger_useful.h>

namespace DB
{
Region::CommittedScanner Region::createCommittedScanner(bool use_lock, bool need_value)
{
    return Region::CommittedScanner(this->shared_from_this(), use_lock, need_value);
}
Region::CommittedRemover Region::createCommittedRemover(bool use_lock)
{
    return Region::CommittedRemover(this->shared_from_this(), use_lock);
}
const RegionRangeKeys & RegionRaftCommandDelegate::getRange()
{
    return *meta.makeRaftCommandDelegate().regionState().getRange();
}
UInt64 RegionRaftCommandDelegate::appliedIndex()
{
    return meta.makeRaftCommandDelegate().applyState().applied_index();
}

static const metapb::Peer & findPeerByStore(const metapb::Region & region, UInt64 store_id)
{
    for (const auto & peer : region.peers())
    {
        if (peer.store_id() == store_id)
            return peer;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "{}: peer not found, store_id={}", __PRETTY_FUNCTION__, store_id);
}

void RegionRaftCommandDelegate::execChangePeer(
    const raft_cmdpb::AdminRequest & request,
    const raft_cmdpb::AdminResponse & response,
    const UInt64 index,
    const UInt64 term)
{
    LOG_INFO(
        log,
        "{} execute change peer cmd: {}",
        toString(false),
        (request.has_change_peer_v2() ? request.change_peer_v2().ShortDebugString()
                                      : request.change_peer().ShortDebugString()));
    meta.makeRaftCommandDelegate().execChangePeer(request, response, index, term);
    LOG_INFO(log, "After execute change peer cmd, current region info: {}", getDebugString());
}

Regions RegionRaftCommandDelegate::execBatchSplit(
    const raft_cmdpb::AdminRequest &,
    const raft_cmdpb::AdminResponse & response,
    const UInt64 index,
    const UInt64 term)
{
    const auto & new_region_infos = response.splits().regions();

    if (new_region_infos.empty())
        throw Exception(std::string(__PRETTY_FUNCTION__) + ": got no new region", ErrorCodes::LOGICAL_ERROR);

    std::vector<RegionPtr> split_regions;

    {
        std::unique_lock<std::shared_mutex> lock(mutex);

        int new_region_index = -1;
        for (int i = 0; i < new_region_infos.size(); ++i)
        {
            const auto & region_info = new_region_infos[i];
            if (region_info.id() != meta.regionId())
            {
                const auto & peer = findPeerByStore(region_info, meta.storeId());
                RegionMeta new_meta(peer, region_info, initialApplyState());
                auto split_region = splitInto(std::move(new_meta));
                split_regions.emplace_back(split_region);
            }
            else
            {
                if (new_region_index == -1)
                    new_region_index = i;
                else
                    throw Exception(
                        std::string(__PRETTY_FUNCTION__) + ": duplicate region index",
                        ErrorCodes::LOGICAL_ERROR);
            }
        }

        if (new_region_index == -1)
            throw Exception(std::string(__PRETTY_FUNCTION__) + ": region index not found", ErrorCodes::LOGICAL_ERROR);

        RegionMeta new_meta(meta.getPeer(), new_region_infos[new_region_index], meta.clonedApplyState());
        new_meta.setApplied(index, term);
        meta.assignRegionMeta(std::move(new_meta));
    }

    {
        std::stringstream ss;
        for (const auto & region : split_regions)
        {
            ss << region->getDebugString();
            ss << ' ';
        }
        ss << getDebugString();
        LOG_INFO(log, "{} split into {}", toString(false), ss.str());
    }

    return split_regions;
}

void RegionRaftCommandDelegate::execPrepareMerge(
    const raft_cmdpb::AdminRequest & request,
    const raft_cmdpb::AdminResponse & response,
    const UInt64 index,
    const UInt64 term)
{
    const auto & prepare_merge_request = request.prepare_merge();

    const auto & target = prepare_merge_request.target();

    LOG_INFO(
        log,
        "{} execute prepare merge, min_index {}, target region_id={}",
        toString(false),
        prepare_merge_request.min_index(),
        target.id());

    meta.makeRaftCommandDelegate().execPrepareMerge(request, response, index, term);
}

void RegionRaftCommandDelegate::execRollbackMerge(
    const raft_cmdpb::AdminRequest & request,
    const raft_cmdpb::AdminResponse & response,
    const UInt64 index,
    const UInt64 term)
{
    const auto & rollback_request = request.rollback_merge();

    LOG_INFO(log, "{} execute rollback merge, commit index {}", toString(false), rollback_request.commit());
    meta.makeRaftCommandDelegate().execRollbackMerge(request, response, index, term);
}

RegionID RegionRaftCommandDelegate::execCommitMerge(
    const raft_cmdpb::AdminRequest & request,
    const raft_cmdpb::AdminResponse & response,
    const UInt64 index,
    const UInt64 term,
    const KVStore & kvstore,
    RegionTable & region_table)
{
    const auto & commit_merge_request = request.commit_merge();
    auto & meta_delegate = meta.makeRaftCommandDelegate();
    const auto & source_meta = commit_merge_request.source();
    auto source_region = kvstore.getRegion(source_meta.id());
    LOG_INFO(
        log,
        "{} execute commit merge, source region_id={}, commit index={}",
        toString(false),
        source_meta.id(),
        commit_merge_request.commit());

    const auto & source_region_meta_delegate = source_region->meta.makeRaftCommandDelegate();
    const auto res = meta_delegate.checkBeforeCommitMerge(request, source_region_meta_delegate);

    source_region->setPendingRemove();

    {
        const std::string & new_start_key = res.source_at_left
            ? source_region_meta_delegate.regionState().getRegion().start_key()
            : meta_delegate.regionState().getRegion().start_key();
        const std::string & new_end_key = res.source_at_left
            ? meta_delegate.regionState().getRegion().end_key()
            : source_region_meta_delegate.regionState().getRegion().end_key();

        region_table.extendRegionRange(
            id(),
            RegionRangeKeys(TiKVKey::copyFrom(new_start_key), TiKVKey::copyFrom(new_end_key)));
    }

    {
        std::unique_lock<std::shared_mutex> lock(mutex);

        { // Only operation region merge will lock 2 regions at same time. We have made it safe under task lock in KVStore.
            std::shared_lock<std::shared_mutex> lock2(source_region->mutex);
            data.mergeFrom(source_region->data);
        }

        meta_delegate.execCommitMerge(res, index, term, source_region_meta_delegate, response);
    }

    return source_meta.id();
}

void RegionRaftCommandDelegate::handleAdminRaftCmd(
    const raft_cmdpb::AdminRequest & request,
    const raft_cmdpb::AdminResponse & response,
    UInt64 index,
    UInt64 term,
    const KVStore & kvstore,
    RegionTable & region_table,
    RaftCommandResult & result)
{
    result.type = RaftCommandResult::Type::Default;
    if (index <= appliedIndex())
    {
        result.type = RaftCommandResult::Type::IndexError;
        return;
    }

    auto type = request.cmd_type();

    LOG_INFO(
        log,
        "{} execute admin command {} at [term: {}, index: {}]",
        toString(),
        raft_cmdpb::AdminCmdType_Name(type),
        term,
        index);

    switch (type)
    {
    case raft_cmdpb::AdminCmdType::ChangePeer:
    case raft_cmdpb::AdminCmdType::ChangePeerV2:
    {
        execChangePeer(request, response, index, term);
        result.type = RaftCommandResult::Type::ChangePeer;

        break;
    }
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    case raft_cmdpb::AdminCmdType::Split:
    case raft_cmdpb::AdminCmdType::BatchSplit:
    {
        result.ori_region_range = meta.makeRaftCommandDelegate().regionState().getRange();
        Regions split_regions = execBatchSplit(request, response, index, term);
        result.type = RaftCommandResult::Type::BatchSplit;
        result.split_regions = std::move(split_regions);
        break;
    }
    case raft_cmdpb::AdminCmdType::PrepareMerge:
        execPrepareMerge(request, response, index, term);
        break;
    case raft_cmdpb::AdminCmdType::CommitMerge:
    {
        result.ori_region_range = meta.makeRaftCommandDelegate().regionState().getRange();
        result.type = RaftCommandResult::Type::CommitMerge;
        result.source_region_id = execCommitMerge(request, response, index, term, kvstore, region_table);
        break;
    }
    case raft_cmdpb::AdminCmdType::RollbackMerge:
        execRollbackMerge(request, response, index, term);
        break;
    default:
        throw Exception(
            fmt::format("unsupported admin command type {}", raft_cmdpb::AdminCmdType_Name(type)),
            ErrorCodes::LOGICAL_ERROR);
    }

    switch (type)
    {
    case raft_cmdpb::AdminCmdType::PrepareMerge:
    case raft_cmdpb::AdminCmdType::CommitMerge:
    case raft_cmdpb::AdminCmdType::RollbackMerge:
    {
        LOG_INFO(log, "After execute merge cmd, current region info: {}", getDebugString());
        break;
    }
    default:
        break;
    }

    meta.notifyAll();
}


std::pair<EngineStoreApplyRes, DM::WriteResult> Region::handleWriteRaftCmd(
    const WriteCmdsView & cmds,
    UInt64 index,
    UInt64 term,
    TMTContext & tmt)
{
    if (index <= appliedIndex())
    {
        return std::make_pair(EngineStoreApplyRes::None, std::nullopt);
    }
    auto & context = tmt.getContext();
    Stopwatch watch;

    size_t write_put_key_count = 0;
    size_t lock_put_key_count = 0;
    size_t default_put_key_count = 0;
    size_t lock_del_key_count = 0;
    size_t write_del_key_count = 0;
    // Considering short value embeded in lock cf, it's necessary to record deletion from default cf.
    size_t default_del_key_count = 0;
    // How many bytes has been written to KVStore(and maybe then been moved to underlying DeltaTree).
    // We don't count DEL because it is only used to delete LOCK, which is small and not count in doInsert.
    size_t write_size = 0;
    size_t prev_size = dataSize();

    SCOPE_EXIT({
        GET_METRIC(tiflash_raft_apply_write_command_duration_seconds, type_write).Observe(watch.elapsedSeconds());
        // Relate to tiflash_system_profile_event_DMWriteBlock, but with uncommitted writes.
        // Relate to tikv_storage_command_total, which is not available on proxy.
        GET_METRIC(tiflash_raft_raft_frequent_events_count, type_write).Increment(1);
        GET_METRIC(tiflash_raft_process_keys, type_write_put).Increment(write_put_key_count);
        GET_METRIC(tiflash_raft_process_keys, type_lock_put).Increment(lock_put_key_count);
        GET_METRIC(tiflash_raft_process_keys, type_default_put).Increment(default_put_key_count);
        GET_METRIC(tiflash_raft_process_keys, type_lock_del).Increment(lock_del_key_count);
        GET_METRIC(tiflash_raft_process_keys, type_write_del).Increment(write_del_key_count);
        GET_METRIC(tiflash_raft_process_keys, type_default_del).Increment(default_del_key_count);
        auto after_size = dataSize();
        if (after_size > prev_size + RAFT_REGION_BIG_WRITE_THRES)
            GET_METRIC(tiflash_raft_write_flow_bytes, type_big_write_to_region).Observe(after_size - prev_size);
        GET_METRIC(tiflash_raft_throughput_bytes, type_write).Increment(write_size);
    });

    if (cmds.len)
    {
        GET_METRIC(tiflash_raft_entry_size, type_normal).Observe(cmds.len);
    }

    auto is_v2 = this->getClusterRaftstoreVer() == RaftstoreVer::V2;

    const auto handle_by_index_func = [&](auto i) {
        auto type = cmds.cmd_types[i];
        auto cf = cmds.cmd_cf[i];
        switch (type)
        {
        case WriteCmdType::Put:
        {
            auto tikv_key = TiKVKey(cmds.keys[i].data, cmds.keys[i].len);
            auto tikv_value = TiKVValue(cmds.vals[i].data, cmds.vals[i].len);
            if (cf == ColumnFamilyType::Write)
            {
                write_put_key_count++;
            }
            else if (cf == ColumnFamilyType::Lock)
            {
                lock_put_key_count++;
            }
            else if (cf == ColumnFamilyType::Default)
            {
                default_put_key_count++;
            }
            try
            {
                if unlikely (is_v2)
                {
                    // There may be orphan default key in a snapshot.
                    write_size += doInsert(cf, std::move(tikv_key), std::move(tikv_value), DupCheck::AllowSame);
                }
                else
                {
                    write_size += doInsert(cf, std::move(tikv_key), std::move(tikv_value), DupCheck::Deny);
                }
            }
            catch (Exception & e)
            {
                LOG_ERROR(
                    log,
                    "{} catch exception: {}, while applying `CmdType::Put` on [term {}, index {}], CF {}",
                    toString(),
                    e.message(),
                    term,
                    index,
                    CFToName(cf));
                e.rethrow();
            }
            break;
        }
        case WriteCmdType::Del:
        {
            auto tikv_key = TiKVKey(cmds.keys[i].data, cmds.keys[i].len);
            if unlikely (cf == ColumnFamilyType::Write)
            {
                write_del_key_count++;
            }
            else if (cf == ColumnFamilyType::Lock)
            {
                lock_del_key_count++;
            }
            else if (cf == ColumnFamilyType::Default)
            {
                default_del_key_count++;
            }
            try
            {
                doRemove(cf, tikv_key);
            }
            catch (Exception & e)
            {
                LOG_ERROR(
                    log,
                    "{} catch exception: {}, while applying `CmdType::Delete` on [term {}, index {}], key in hex: {}, "
                    "CF {}",
                    toString(),
                    e.message(),
                    term,
                    index,
                    tikv_key.toDebugString(),
                    CFToName(cf));
                e.rethrow();
            }
            break;
        }
        }
    };

    const auto handle_write_cmd_func = [&]() {
        size_t cmd_write_cf_cnt = 0, cache_written_size = 0;
        auto ori_cache_size = dataSize();
        for (UInt64 i = 0; i < cmds.len; ++i)
        {
            if (cmds.cmd_cf[i] == ColumnFamilyType::Write)
                cmd_write_cf_cnt++;
            else
                handle_by_index_func(i);
        }

        if (cmd_write_cf_cnt)
        {
            for (UInt64 i = 0; i < cmds.len; ++i)
            {
                if (cmds.cmd_cf[i] == ColumnFamilyType::Write)
                    handle_by_index_func(i);
            }
        }
        cache_written_size = dataSize() - ori_cache_size;
        approx_mem_cache_rows += cmd_write_cf_cnt;
        approx_mem_cache_bytes += cache_written_size;
    };

    DM::WriteResult write_result = std::nullopt;
    {
        {
            // RegionTable::writeCommittedByRegion may lead to persistRegion when flush proactively.
            // So we can't lock here.
            // Safety: Mutations to a region come from raft applying and bg flushing of storage layer.
            // 1. A raft applying process should acquire the region task lock.
            // 2. While bg/fg flushing, applying raft logs should also be prevented with region task lock.
            // So between here and RegionTable::writeCommittedByRegion, there will be no new data applied.
            std::unique_lock<std::shared_mutex> lock(mutex);
            handle_write_cmd_func();
        }

        // If transfer-leader happened during ingest-sst, there might be illegal data.
        if (0 != cmds.len)
        {
            /// Flush data right after they are committed.
            RegionDataReadInfoList data_list_to_remove;
            try
            {
                write_result
                    = RegionTable::writeCommittedByRegion(context, shared_from_this(), data_list_to_remove, log, true);
            }
            catch (DB::Exception & e)
            {
                std::vector<std::string> entry_infos;
                for (UInt64 i = 0; i < cmds.len; ++i)
                {
                    auto cf = cmds.cmd_cf[i];
                    auto type = cmds.cmd_types[i];
                    auto tikv_key = TiKVKey(cmds.keys[i].data, cmds.keys[i].len);
                    entry_infos.emplace_back(fmt::format(
                        "{}|{}|{}",
                        type == DB::WriteCmdType::Put ? "PUT" : "DEL",
                        CFToName(cf),
                        tikv_key.toDebugString()));
                }
                LOG_ERROR(
                    log,
                    "{} catch exception: {}, while applying `RegionTable::writeCommittedByRegion` on [term {}, index "
                    "{}], "
                    "entries {}",
                    toString(),
                    e.message(),
                    term,
                    index,
                    fmt::join(entry_infos.begin(), entry_infos.end(), ":"));
                e.rethrow();
            }
        }

        meta.setApplied(index, term);
    }

    meta.notifyAll();

    return std::make_pair(EngineStoreApplyRes::None, std::move(write_result));
}

RegionRaftCommandDelegate & Region::makeRaftCommandDelegate(const KVStoreTaskLock & lock)
{
    static_assert(sizeof(RegionRaftCommandDelegate) == sizeof(Region));
    // lock is useless, just to make sure the task mutex of KVStore is locked
    std::ignore = lock;
    return static_cast<RegionRaftCommandDelegate &>(*this);
}
} // namespace DB