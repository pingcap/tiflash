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
#include <Storages/KVStore/Utils/SerializationHelper.h>
#include <common/logger_useful.h>

#include <ext/scope_guard.h>
#include <memory>

namespace ProfileEvents
{
extern const Event RaftWaitIndexTimeout;
} // namespace ProfileEvents

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_FORMAT_VERSION;
} // namespace ErrorCodes
namespace FailPoints
{
extern const char force_region_persist_version[];
} // namespace FailPoints

enum class RegionPersistVersion
{
    V1 = 1,
    V2 = 2,
};

const UInt32 Region::CURRENT_VERSION = static_cast<UInt64>(RegionPersistVersion::V2);

RegionData::WriteCFIter Region::removeDataByWriteIt(const RegionData::WriteCFIter & write_it)
{
    return data.removeDataByWriteIt(write_it);
}

std::optional<RegionDataReadInfo> Region::readDataByWriteIt(
    const RegionData::ConstWriteCFIter & write_it,
    bool need_value,
    bool hard_error)
{
    try
    {
        return data.readDataByWriteIt(write_it, need_value, id(), appliedIndex(), hard_error);
    }
    catch (DB::Exception & e)
    {
        e.addMessage(fmt::format("(applied_term: {})", appliedIndexTerm()));
        throw;
    }
}

DecodedLockCFValuePtr Region::getLockInfo(const RegionLockReadQuery & query) const
{
    return data.getLockInfo(query);
}

void Region::insert(const std::string & cf, TiKVKey && key, TiKVValue && value, DupCheck mode)
{
    return insert(NameToCF(cf), std::move(key), std::move(value), mode);
}

void Region::insert(ColumnFamilyType type, TiKVKey && key, TiKVValue && value, DupCheck mode)
{
    std::unique_lock<std::shared_mutex> lock(mutex);
    return doInsert(type, std::move(key), std::move(value), mode);
}

void Region::doInsert(ColumnFamilyType type, TiKVKey && key, TiKVValue && value, DupCheck mode)
{
    if (getClusterRaftstoreVer() == RaftstoreVer::V2)
    {
        if (type == ColumnFamilyType::Write)
        {
            if (orphanKeysInfo().observeKeyFromNormalWrite(key))
            {
                // We can't assert the key exists in write_cf here,
                // since it may be already written into DeltaTree.
                return;
            }
        }
    }
    data.insert(type, std::move(key), std::move(value), mode);
}

void Region::remove(const std::string & cf, const TiKVKey & key)
{
    std::unique_lock<std::shared_mutex> lock(mutex);
    doRemove(NameToCF(cf), key);
}

void Region::doRemove(ColumnFamilyType type, const TiKVKey & key)
{
    data.remove(type, key);
}

void Region::clearAllData()
{
    std::unique_lock lock(mutex);
    data = RegionData();
}

UInt64 Region::appliedIndex() const
{
    return meta.appliedIndex();
}

UInt64 Region::appliedIndexTerm() const
{
    return meta.appliedIndexTerm();
}

void Region::setApplied(UInt64 index, UInt64 term)
{
    std::unique_lock lock(mutex);
    meta.setApplied(index, term);
}

RegionPtr Region::splitInto(RegionMeta && meta)
{
    RegionPtr new_region = std::make_shared<Region>(std::move(meta), proxy_helper);

    const auto range = new_region->getRange();
    data.splitInto(range->comparableKeys(), new_region->data);

    return new_region;
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

static const metapb::Peer & findPeerByStore(const metapb::Region & region, UInt64 store_id)
{
    for (const auto & peer : region.peers())
    {
        if (peer.store_id() == store_id)
            return peer;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "{}: peer not found, store_id={}", __PRETTY_FUNCTION__, store_id);
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

namespace RegionPersistFormat
{
static constexpr UInt32 HAS_EAGER_TRUNCATE_INDEX = 0x01;
}

std::tuple<size_t, UInt64> Region::serialize(WriteBuffer & buf) const
{
    auto binary_version = Region::CURRENT_VERSION;
    fiu_do_on(FailPoints::force_region_persist_version, {
        if (auto v = FailPointHelper::getFailPointVal(FailPoints::force_region_persist_version); v)
        {
            binary_version = std::any_cast<UInt64>(v.value());
            LOG_WARNING(
                Logger::get(),
                "Failpoint force_region_persist_version set region binary version, value={}",
                binary_version);
        }
    });
    size_t total_size = writeBinary2(binary_version, buf);
    UInt64 applied_index = -1;

    {
        std::shared_lock<std::shared_mutex> lock(mutex);

        // serialize meta
        const auto [meta_size, index] = meta.serialize(buf);
        total_size += meta_size;
        applied_index = index;

        // try serialize extra flags
        if (binary_version >= 2)
        {
            static_assert(sizeof(eager_truncated_index) == sizeof(UInt64));
            UInt32 flags = RegionPersistFormat::HAS_EAGER_TRUNCATE_INDEX;
            total_size += writeBinary2(flags, buf);
            total_size += writeBinary2(eager_truncated_index, buf);
        }

        // serialize data
        total_size += data.serialize(buf);
    }

    return {total_size, applied_index};
}

RegionPtr Region::deserialize(ReadBuffer & buf, const TiFlashRaftProxyHelper * proxy_helper)
{
    const auto binary_version = readBinary2<UInt32>(buf);
    if (binary_version != static_cast<UInt64>(RegionPersistVersion::V1)
        && binary_version != static_cast<UInt64>(RegionPersistVersion::V2))
    {
        throw Exception(
            ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "{}: unexpected version: {}, expected: {}",
            binary_version,
            __PRETTY_FUNCTION__,
            CURRENT_VERSION);
    }

    // deserialize meta
    RegionPtr region = std::make_shared<Region>(RegionMeta::deserialize(buf), proxy_helper);

    // try deserialize flags
    if (binary_version >= 2)
    {
        auto flags = readBinary2<UInt32>(buf);
        if ((flags & RegionPersistFormat::HAS_EAGER_TRUNCATE_INDEX) != 0)
        {
            region->eager_truncated_index = readBinary2<UInt64>(buf);
        }
    }

    // deserialize data
    RegionData::deserialize(buf, region->data);

    // restore other var according to meta
    region->last_restart_log_applied = region->appliedIndex();
    region->setLastCompactLogApplied(region->appliedIndex());
    return region;
}

std::string Region::getDebugString() const
{
    const auto & meta_snap = meta.dumpRegionMetaSnapshot();
    return fmt::format(
        "[region_id={} index={} table_id={} ver={} conf_ver={} state={} peer={}]",
        id(),
        meta.appliedIndex(),
        mapped_table_id,
        meta_snap.ver,
        meta_snap.conf_ver,
        raft_serverpb::PeerState_Name(peerState()),
        meta_snap.peer.ShortDebugString());
}

std::string Region::toString(bool dump_status) const
{
    return meta.toString(dump_status);
}

RegionID Region::id() const
{
    return meta.regionId();
}

bool Region::isPendingRemove() const
{
    return peerState() == raft_serverpb::PeerState::Tombstone;
}

bool Region::isMerging() const
{
    return peerState() == raft_serverpb::PeerState::Merging;
}

void Region::setPendingRemove()
{
    setPeerState(raft_serverpb::PeerState::Tombstone);
}

void Region::setStateApplying()
{
    setPeerState(raft_serverpb::PeerState::Applying);
    snapshot_event_flag++;
}

raft_serverpb::PeerState Region::peerState() const
{
    return meta.peerState();
}

size_t Region::dataSize() const
{
    return data.dataSize();
}

size_t Region::writeCFCount() const
{
    std::shared_lock<std::shared_mutex> lock(mutex);
    return data.writeCF().getSize();
}

std::string Region::dataInfo() const
{
    std::shared_lock<std::shared_mutex> lock(mutex);

    FmtBuffer buff;
    buff.append("[");
    auto write_size = data.writeCF().getSize();
    auto lock_size = data.lockCF().getSize();
    auto default_size = data.defaultCF().getSize();
    if (write_size)
        buff.fmtAppend("write {} ", write_size);
    if (lock_size)
        buff.fmtAppend("lock {} ", lock_size);
    if (default_size)
        buff.fmtAppend("default {} ", default_size);
    buff.append("]");
    return buff.toString();
}

std::pair<UInt64, UInt64> Region::getRaftLogEagerGCRange() const
{
    std::unique_lock lock(mutex);
    auto applied_index = appliedIndex();
    return {eager_truncated_index, applied_index};
}

void Region::updateRaftLogEagerIndex(UInt64 new_truncate_index)
{
    std::unique_lock lock(mutex);
    eager_truncated_index = new_truncate_index;
}

UInt64 Region::lastRestartLogApplied() const
{
    return last_restart_log_applied;
}

UInt64 Region::lastCompactLogApplied() const
{
    return last_compact_log_applied;
}

void Region::setLastCompactLogApplied(UInt64 new_value) const
{
    last_compact_log_applied = new_value;
}

// Everytime the region is persisted, we update the `last_compact_log_applied`
void Region::updateLastCompactLogApplied(const RegionTaskLock &) const
{
    const UInt64 current_applied_index = appliedIndex();
    if (last_compact_log_applied != 0)
    {
        UInt64 gap = current_applied_index > last_compact_log_applied //
            ? current_applied_index - last_compact_log_applied
            : 0;
        GET_METRIC(tiflash_raft_raft_log_gap_count, type_applied_index).Observe(gap);
    }
    last_compact_log_applied = current_applied_index;
}

Region::CommittedScanner Region::createCommittedScanner(bool use_lock, bool need_value)
{
    return Region::CommittedScanner(this->shared_from_this(), use_lock, need_value);
}

Region::CommittedRemover Region::createCommittedRemover(bool use_lock)
{
    return Region::CommittedRemover(this->shared_from_this(), use_lock);
}

ImutRegionRangePtr Region::getRange() const
{
    return meta.getRange();
}

RaftstoreVer Region::getClusterRaftstoreVer()
{
    // In non-debug/test mode, we should assert the proxy_ptr be always not null.
    if (likely(proxy_helper != nullptr))
    {
        if (likely(proxy_helper->fn_get_cluster_raftstore_version))
        {
            // Make debug funcs happy.
            return proxy_helper->fn_get_cluster_raftstore_version(proxy_helper->proxy_ptr, 0, 0);
        }
    }
    return RaftstoreVer::Uncertain;
}

kvrpcpb::ReadIndexRequest GenRegionReadIndexReq(const Region & region, UInt64 start_ts)
{
    auto meta_snap = region.dumpRegionMetaSnapshot();
    kvrpcpb::ReadIndexRequest request;
    {
        auto * context = request.mutable_context();
        context->set_region_id(region.id());
        *context->mutable_peer() = meta_snap.peer;
        context->mutable_region_epoch()->set_version(meta_snap.ver);
        context->mutable_region_epoch()->set_conf_ver(meta_snap.conf_ver);
        // if start_ts is 0, only send read index request to proxy
        if (start_ts)
        {
            request.set_start_ts(start_ts);
            auto * key_range = request.add_ranges();
            // use original tikv key
            key_range->set_start_key(meta_snap.range->comparableKeys().first.key);
            key_range->set_end_key(meta_snap.range->comparableKeys().second.key);
        }
    }
    return request;
}

bool Region::checkIndex(UInt64 index) const
{
    return meta.checkIndex(index);
}

std::tuple<WaitIndexResult, double> Region::waitIndex(
    UInt64 index,
    const UInt64 timeout_ms,
    std::function<bool(void)> && check_running)
{
    if (proxy_helper != nullptr)
    {
        if (!meta.checkIndex(index))
        {
            Stopwatch wait_index_watch;
            LOG_DEBUG(log, "{} need to wait learner index {} timeout {}", toString(), index, timeout_ms);
            auto wait_idx_res = meta.waitIndex(index, timeout_ms, std::move(check_running));
            auto elapsed_secs = wait_index_watch.elapsedSeconds();
            switch (wait_idx_res)
            {
            case WaitIndexResult::Finished:
            {
                LOG_DEBUG(log, "{} wait learner index {} done", toString(false), index);
                return {wait_idx_res, elapsed_secs};
            }
            case WaitIndexResult::Terminated:
            {
                return {wait_idx_res, elapsed_secs};
            }
            case WaitIndexResult::Timeout:
            {
                ProfileEvents::increment(ProfileEvents::RaftWaitIndexTimeout);
                LOG_WARNING(
                    log,
                    "{} wait learner index {} timeout current {} state {}",
                    toString(false),
                    index,
                    appliedIndex(),
                    peerState());
                return {wait_idx_res, elapsed_secs};
            }
            }
        }
    }
    return {WaitIndexResult::Finished, 0};
}

UInt64 Region::version() const
{
    return meta.version();
}

UInt64 Region::confVer() const
{
    return meta.confVer();
}

void Region::assignRegion(Region && new_region)
{
    std::unique_lock<std::shared_mutex> lock(mutex);

    data.assignRegionData(std::move(new_region.data));
    meta.assignRegionMeta(std::move(new_region.meta));
    meta.notifyAll();
    eager_truncated_index = meta.truncateIndex();
}

/// try to clean illegal data because of feature `compaction filter`
void Region::tryCompactionFilter(const Timestamp safe_point)
{
    size_t del_write = 0;
    auto & write_map = data.writeCF().getDataMut();
    auto & default_map = data.defaultCF().getDataMut();
    for (auto write_map_it = write_map.begin(); write_map_it != write_map.end();)
    {
        const auto & decoded_val = std::get<2>(write_map_it->second);
        const auto & [pk, ts] = write_map_it->first;

        if (decoded_val.write_type == RecordKVFormat::CFModifyFlag::PutFlag)
        {
            if (!decoded_val.short_value)
            {
                if (auto data_it = default_map.find({pk, decoded_val.prewrite_ts}); data_it == default_map.end())
                {
                    // if key-val in write cf can not find matched data in default cf and its commit-ts < gc-safe-point, we can clean it safely.
                    if (ts < safe_point)
                    {
                        del_write += 1;
                        data.cf_data_size -= RegionWriteCFData::calcTiKVKeyValueSize(write_map_it->second);
                        write_map_it = write_map.erase(write_map_it);
                        continue;
                    }
                }
            }
        }
        ++write_map_it;
    }
    // No need to check default cf. Because tikv will gc default cf before write cf.
    if (del_write)
    {
        LOG_INFO(log, "delete {} records in write cf for region_id={}", del_write, meta.regionId());
    }
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

    size_t put_key_count = 0;
    size_t del_key_count = 0;

    SCOPE_EXIT({
        GET_METRIC(tiflash_raft_apply_write_command_duration_seconds, type_write).Observe(watch.elapsedSeconds());
        // Relate to tiflash_system_profile_event_DMWriteBlock, but with uncommitted writes.
        // Relate to tikv_storage_command_total, which is not available on proxy.
        GET_METRIC(tiflash_raft_raft_frequent_events_count, type_write).Increment(1);
        GET_METRIC(tiflash_raft_process_keys, type_write_put).Increment(put_key_count);
        GET_METRIC(tiflash_raft_process_keys, type_write_del).Increment(del_key_count);
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
                put_key_count++;
            }
            try
            {
                if (is_v2)
                {
                    // There may be orphan default key in a snapshot.
                    doInsert(cf, std::move(tikv_key), std::move(tikv_value), DupCheck::AllowSame);
                }
                else
                {
                    doInsert(cf, std::move(tikv_key), std::move(tikv_value), DupCheck::Deny);
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
            if (unlikely(cf == ColumnFamilyType::Write))
            {
                del_key_count++;
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
            // RegionTable::writeBlockByRegion may lead to persistRegion when flush proactively.
            // So we can't lock here.
            // Safety: Mutations to a region come from raft applying and bg flushing of storage layer.
            // 1. A raft applying process should acquire the region task lock.
            // 2. While bg/fg flushing, applying raft logs should also be prevented with region task lock.
            // So between here and RegionTable::writeBlockByRegion, there will be no new data applied.
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
                    = RegionTable::writeBlockByRegion(context, shared_from_this(), data_list_to_remove, log, true);
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
                    "{} catch exception: {}, while applying `RegionTable::writeBlockByRegion` on [term {}, index {}], "
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

RegionMetaSnapshot Region::dumpRegionMetaSnapshot() const
{
    return meta.dumpRegionMetaSnapshot();
}

Region::Region(RegionMeta && meta_)
    : Region(std::move(meta_), nullptr)
{}

Region::Region(DB::RegionMeta && meta_, const TiFlashRaftProxyHelper * proxy_helper_)
    : meta(std::move(meta_))
    , eager_truncated_index(meta.truncateIndex())
    , log(Logger::get())
    , keyspace_id(meta.getRange()->getKeyspaceID())
    , mapped_table_id(meta.getRange()->getMappedTableID())
    , proxy_helper(proxy_helper_)
{}

TableID Region::getMappedTableID() const
{
    return mapped_table_id;
}

KeyspaceID Region::getKeyspaceID() const
{
    return keyspace_id;
}

void Region::setPeerState(raft_serverpb::PeerState state)
{
    meta.setPeerState(state);
    meta.notifyAll();
}

const RegionRangeKeys & RegionRaftCommandDelegate::getRange()
{
    return *meta.makeRaftCommandDelegate().regionState().getRange();
}
UInt64 RegionRaftCommandDelegate::appliedIndex()
{
    return meta.makeRaftCommandDelegate().applyState().applied_index();
}
metapb::Region Region::cloneMetaRegion() const
{
    return meta.cloneMetaRegion();
}
const metapb::Region & Region::getMetaRegion() const
{
    return meta.getMetaRegion();
}
raft_serverpb::MergeState Region::cloneMergeState() const
{
    return meta.cloneMergeState();
}
const raft_serverpb::MergeState & Region::getMergeState() const
{
    return meta.getMergeState();
}

std::pair<size_t, size_t> Region::getApproxMemCacheInfo() const
{
    return {
        approx_mem_cache_rows.load(std::memory_order_relaxed),
        approx_mem_cache_bytes.load(std::memory_order_relaxed)};
}

void Region::cleanApproxMemCacheInfo() const
{
    approx_mem_cache_rows = 0;
    approx_mem_cache_bytes = 0;
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

} // namespace DB
