#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionExecutionResult.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/SSTReader.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiKVRange.h>

#include <ext/scope_guard.h>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_FORMAT_VERSION;
} // namespace ErrorCodes

const UInt32 Region::CURRENT_VERSION = 1;

RegionData::WriteCFIter Region::removeDataByWriteIt(const RegionData::WriteCFIter & write_it) { return data.removeDataByWriteIt(write_it); }

RegionDataReadInfo Region::readDataByWriteIt(const RegionData::ConstWriteCFIter & write_it, bool need_value) const
{
    return data.readDataByWriteIt(write_it, need_value);
}

DecodedLockCFValuePtr Region::getLockInfo(const RegionLockReadQuery & query) const { return data.getLockInfo(query); }

void Region::insert(const std::string & cf, TiKVKey && key, TiKVValue && value)
{
    return insert(NameToCF(cf), std::move(key), std::move(value));
}

void Region::insert(ColumnFamilyType type, TiKVKey && key, TiKVValue && value)
{
    std::unique_lock<std::shared_mutex> lock(mutex);
    return doInsert(type, std::move(key), std::move(value));
}

void Region::doInsert(ColumnFamilyType type, TiKVKey && key, TiKVValue && value) { data.insert(type, std::move(key), std::move(value)); }

void Region::doCheckTable(const DB::DecodedTiKVKey & raw_key) const
{
    auto table_id = RecordKVFormat::getTableId(raw_key);
    if (table_id != getMappedTableID())
    {
        LOG_ERROR(log, __FUNCTION__ << ": table id not match, expect " << getMappedTableID() << ", got " << table_id);
        throw Exception(std::string(__PRETTY_FUNCTION__) + ": table id not match", ErrorCodes::LOGICAL_ERROR);
    }
}

void Region::remove(const std::string & cf, const TiKVKey & key)
{
    std::unique_lock<std::shared_mutex> lock(mutex);
    doRemove(NameToCF(cf), key);
}

void Region::doRemove(ColumnFamilyType type, const TiKVKey & key) { data.remove(type, key); }

void Region::clearAllData()
{
    std::unique_lock lock(mutex);
    data = RegionData();
}

UInt64 Region::appliedIndex() const { return meta.appliedIndex(); }

RegionPtr Region::splitInto(RegionMeta && meta)
{
    RegionPtr new_region = std::make_shared<Region>(std::move(meta), proxy_helper);

    const auto range = new_region->getRange();
    data.splitInto(range->comparableKeys(), new_region->data);

    return new_region;
}

void RegionRaftCommandDelegate::execChangePeer(
    const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, const UInt64 index, const UInt64 term)
{
    LOG_INFO(log,
        toString(false) << " execute change peer cmd {"
                        << (request.has_change_peer_v2() ? request.change_peer_v2().ShortDebugString()
                                                         : request.change_peer().ShortDebugString())
                        << "}");
    meta.makeRaftCommandDelegate().execChangePeer(request, response, index, term);
    LOG_INFO(log, "After execute change peer cmd, current region info: "; getDebugString(oss_internal_rare));
}

static const metapb::Peer & findPeerByStore(const metapb::Region & region, UInt64 store_id)
{
    for (const auto & peer : region.peers())
    {
        if (peer.store_id() == store_id)
            return peer;
    }

    throw Exception(
        std::string(__PRETTY_FUNCTION__) + ": peer with store_id " + DB::toString(store_id) + " not found", ErrorCodes::LOGICAL_ERROR);
}

Regions RegionRaftCommandDelegate::execBatchSplit(
    const raft_cmdpb::AdminRequest &, const raft_cmdpb::AdminResponse & response, const UInt64 index, const UInt64 term)
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
                    throw Exception(std::string(__PRETTY_FUNCTION__) + ": duplicate region index", ErrorCodes::LOGICAL_ERROR);
            }
        }

        if (new_region_index == -1)
            throw Exception(std::string(__PRETTY_FUNCTION__) + ": region index not found", ErrorCodes::LOGICAL_ERROR);

        RegionMeta new_meta(meta.getPeer(), new_region_infos[new_region_index], meta.getApplyState());
        new_meta.setApplied(index, term);
        meta.assignRegionMeta(std::move(new_meta));
    }

    {
        std::stringstream ss;
        for (const auto & region : split_regions)
        {
            region->getDebugString(ss);
            ss << ' ';
        }
        getDebugString(ss);
        LOG_INFO(log, toString(false) << " split into " << ss.str());
    }

    return split_regions;
}

void RegionRaftCommandDelegate::execPrepareMerge(
    const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, const UInt64 index, const UInt64 term)
{
    const auto & prepare_merge_request = request.prepare_merge();

    auto & target = prepare_merge_request.target();

    LOG_INFO(log,
        toString(false) << " execute prepare merge, min_index: " << prepare_merge_request.min_index() << ", target: [region " << target.id()
                        << "]");

    meta.makeRaftCommandDelegate().execPrepareMerge(request, response, index, term);
}

void RegionRaftCommandDelegate::execRollbackMerge(
    const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, const UInt64 index, const UInt64 term)
{
    auto & rollback_request = request.rollback_merge();

    LOG_INFO(log, toString(false) << " execute rollback merge, commit index: " << rollback_request.commit());
    meta.makeRaftCommandDelegate().execRollbackMerge(request, response, index, term);
}

RegionID RegionRaftCommandDelegate::execCommitMerge(const raft_cmdpb::AdminRequest & request,
    const raft_cmdpb::AdminResponse & response,
    const UInt64 index,
    const UInt64 term,
    const KVStore & kvstore,
    RegionTable & region_table)
{
    const auto & commit_merge_request = request.commit_merge();
    auto & meta_delegate = meta.makeRaftCommandDelegate();
    auto & source_meta = commit_merge_request.source();
    auto source_region = kvstore.getRegion(source_meta.id());
    if (source_region == nullptr)
    {
        LOG_ERROR(log,
            __FUNCTION__ << ": target " << toString(false) << " can not find source [region " << source_meta.id()
                         << "], commit index: " << commit_merge_request.commit());
        throw Exception(std::string(__PRETTY_FUNCTION__) + ": region not found");
    }
    else
        LOG_INFO(log,
            toString(false) << " execute commit merge, source [region " << source_meta.id()
                            << "], commit index: " << commit_merge_request.commit());

    const auto & source_region_meta_delegate = source_region->meta.makeRaftCommandDelegate();
    const auto res = meta_delegate.checkBeforeCommitMerge(request, source_region_meta_delegate);

    source_region->setPendingRemove();

    {
        const std::string & new_start_key = res.source_at_left ? source_region_meta_delegate.regionState().getRegion().start_key()
                                                               : meta_delegate.regionState().getRegion().start_key();
        const std::string & new_end_key = res.source_at_left ? meta_delegate.regionState().getRegion().end_key()
                                                             : source_region_meta_delegate.regionState().getRegion().end_key();

        region_table.extendRegionRange(id(), RegionRangeKeys(TiKVKey::copyFrom(new_start_key), TiKVKey::copyFrom(new_end_key)));
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

void RegionRaftCommandDelegate::handleAdminRaftCmd(const raft_cmdpb::AdminRequest & request,
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
        LOG_TRACE(log, toString() << " ignore outdated raft log [term: " << term << ", index: " << index << "]");
        result.type = RaftCommandResult::Type::IndexError;
        return;
    }

    auto type = request.cmd_type();

    switch (type)
    {
        case raft_cmdpb::AdminCmdType::ComputeHash:
        case raft_cmdpb::AdminCmdType::VerifyHash:
            break;
        default:
            LOG_INFO(log,
                toString() << " execute admin command " << raft_cmdpb::AdminCmdType_Name(type) << " at [term: " << term
                           << ", index: " << index << "]");
            break;
    }

    switch (type)
    {
        case raft_cmdpb::AdminCmdType::ChangePeer:
        case raft_cmdpb::AdminCmdType::ChangePeerV2:
        {
            execChangePeer(request, response, index, term);
            result.type = RaftCommandResult::Type::ChangePeer;

            break;
        }
        case raft_cmdpb::AdminCmdType::Split:
        case raft_cmdpb::AdminCmdType::BatchSplit:
        {
            result.ori_region_range = meta.makeRaftCommandDelegate().regionState().getRange();
            Regions split_regions = execBatchSplit(request, response, index, term);
            result.type = RaftCommandResult::Type::BatchSplit;
            result.split_regions = std::move(split_regions);
            break;
        }
        case raft_cmdpb::AdminCmdType::CompactLog:
        case raft_cmdpb::AdminCmdType::ComputeHash:
        case raft_cmdpb::AdminCmdType::VerifyHash:
            // Ignore
            meta.setApplied(index, term);
            break;
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
            throw Exception(std::string(__PRETTY_FUNCTION__) + ": unsupported admin command type " + raft_cmdpb::AdminCmdType_Name(type),
                ErrorCodes::LOGICAL_ERROR);
            break;
    }

    switch (type)
    {
        case raft_cmdpb::AdminCmdType::PrepareMerge:
        case raft_cmdpb::AdminCmdType::CommitMerge:
        case raft_cmdpb::AdminCmdType::RollbackMerge:
        {
            std::stringstream ss;
            ss << "After execute merge cmd, current region info: ";
            getDebugString(ss);
            LOG_INFO(log, ss.str());
            break;
        }
        default:
            break;
    }

    meta.notifyAll();
}

std::tuple<size_t, UInt64> Region::serialize(WriteBuffer & buf) const
{
    size_t total_size = writeBinary2(Region::CURRENT_VERSION, buf);
    UInt64 applied_index = -1;

    {
        std::shared_lock<std::shared_mutex> lock(mutex);

        {
            auto [size, index] = meta.serialize(buf);
            total_size += size;
            applied_index = index;
        }

        total_size += data.serialize(buf);
    }

    return {total_size, applied_index};
}

RegionPtr Region::deserialize(ReadBuffer & buf, const TiFlashRaftProxyHelper * proxy_helper)
{
    auto version = readBinary2<UInt32>(buf);
    if (version != Region::CURRENT_VERSION)
        throw Exception(std::string(__PRETTY_FUNCTION__) + ": unexpected version: " + DB::toString(version)
                + ", expected: " + DB::toString(CURRENT_VERSION),
            ErrorCodes::UNKNOWN_FORMAT_VERSION);

    auto meta = RegionMeta::deserialize(buf);
    auto region = std::make_shared<Region>(std::move(meta), proxy_helper);

    RegionData::deserialize(buf, region->data);
    return region;
}

std::string Region::getDebugString(std::stringstream & ss) const
{
    ss << "{region " << id();
    {
        UInt64 index = meta.appliedIndex();
        const auto & meta_snap = meta.dumpRegionMetaSnapshot();
        ss << ", index " << index << ", table " << mapped_table_id << ", ver " << meta_snap.ver << " conf_ver " << meta_snap.conf_ver
           << ", state " << raft_serverpb::PeerState_Name(peerState()) << ", peer " << meta_snap.peer.ShortDebugString();
    }
    ss << "}";
    return ss.str();
}

RegionID Region::id() const { return meta.regionId(); }

bool Region::isPendingRemove() const { return peerState() == raft_serverpb::PeerState::Tombstone; }

bool Region::isMerging() const { return peerState() == raft_serverpb::PeerState::Merging; }

void Region::setPendingRemove() { setPeerState(raft_serverpb::PeerState::Tombstone); }

void Region::setStateApplying()
{
    setPeerState(raft_serverpb::PeerState::Applying);
    snapshot_event_flag++;
}

raft_serverpb::PeerState Region::peerState() const { return meta.peerState(); }

size_t Region::dataSize() const { return data.dataSize(); }

size_t Region::writeCFCount() const
{
    std::shared_lock<std::shared_mutex> lock(mutex);
    return data.writeCF().getSize();
}

std::string Region::dataInfo() const
{
    std::shared_lock<std::shared_mutex> lock(mutex);

    std::stringstream ss;
    auto write_size = data.writeCF().getSize(), lock_size = data.lockCF().getSize(), default_size = data.defaultCF().getSize();
    ss << "[";
    if (write_size)
        ss << "write " << write_size << " ";
    if (lock_size)
        ss << "lock " << lock_size << " ";
    if (default_size)
        ss << "default " << default_size << " ";
    ss << "]";
    return ss.str();
}

void Region::markCompactLog() const { last_compact_log_time = Clock::now(); }

Timepoint Region::lastCompactLogTime() const { return last_compact_log_time; }

Region::CommittedScanner Region::createCommittedScanner(bool use_lock)
{
    return Region::CommittedScanner(this->shared_from_this(), use_lock);
}

Region::CommittedRemover Region::createCommittedRemover(bool use_lock)
{
    return Region::CommittedRemover(this->shared_from_this(), use_lock);
}

std::string Region::toString(bool dump_status) const { return meta.toString(dump_status); }

ImutRegionRangePtr Region::getRange() const { return meta.getRange(); }

ReadIndexResult::ReadIndexResult(RegionException::RegionReadStatus status_, UInt64 read_index_, kvrpcpb::LockInfo * lock_info_)
    : status(status_), read_index(read_index_), lock_info(lock_info_)
{}

kvrpcpb::ReadIndexRequest GenRegionReadIndexReq(const Region & region, UInt64 start_ts)
{
    auto meta_snap = region.dumpRegionMetaSnapshot();
    kvrpcpb::ReadIndexRequest request;
    {
        auto context = request.mutable_context();
        context->set_region_id(region.id());
        *context->mutable_peer() = meta_snap.peer;
        context->mutable_region_epoch()->set_version(meta_snap.ver);
        context->mutable_region_epoch()->set_conf_ver(meta_snap.conf_ver);
        // if start_ts is 0, only send read index request to proxy
        if (start_ts)
        {
            request.set_start_ts(start_ts);
            auto key_range = request.add_ranges();
            key_range->set_start_key(*meta_snap.range->rawKeys().first);
            key_range->set_end_key(*meta_snap.range->rawKeys().second);
        }
    }
    return request;
}

ReadIndexResult Region::learnerRead(UInt64 start_ts)
{
    if (proxy_helper != nullptr)
    {
        kvrpcpb::ReadIndexRequest request = GenRegionReadIndexReq(*this, start_ts);
        auto response = proxy_helper->readIndex(request);
        LOG_TRACE(log,
            toString(false) << " send ReadIndexRequest { " << request.context().ShortDebugString() << " start_ts: " << start_ts << " }"
                            << ", got ReadIndexResponse { " << response.ShortDebugString() << " }");

        if (!start_ts)
            return {};

        if (response.has_region_error())
        {
            auto & region_error = response.region_error();
            LOG_WARNING(log, toString(false) << " find error during ReadIndex: " << region_error.message());
            auto status = region_error.has_epoch_not_match() ? RegionException::RegionReadStatus::EPOCH_NOT_MATCH
                                                             : RegionException::RegionReadStatus::NOT_FOUND;
            return ReadIndexResult(status);
        }
        return ReadIndexResult(RegionException::RegionReadStatus::OK, response.read_index(), response.release_locked());
    }
    return {};
}

double Region::waitIndex(UInt64 index, const TMTContext & tmt)
{
    if (proxy_helper != nullptr)
    {
        if (!meta.checkIndex(index))
        {
            Stopwatch wait_index_watch;
            LOG_DEBUG(log, toString() << " need to wait learner index: " << index);
            if (meta.waitIndex(index, [&tmt]() { return tmt.checkRunning(); }))
                return wait_index_watch.elapsedSeconds();
            LOG_DEBUG(log, toString(false) << " wait learner index " << index << " done");
            return wait_index_watch.elapsedSeconds();
        }
    }
    return 0;
}

UInt64 Region::version() const { return meta.version(); }

UInt64 Region::confVer() const { return meta.confVer(); }

void Region::assignRegion(Region && new_region)
{
    std::unique_lock<std::shared_mutex> lock(mutex);

    data.assignRegionData(std::move(new_region.data));

    meta.assignRegionMeta(std::move(new_region.meta));
    meta.notifyAll();
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
        LOG_INFO(log, __FUNCTION__ << ": delete " << del_write << " in write cf for region " << meta.regionId());
    }
}

void Region::compareAndCompleteSnapshot(HandleMap & handle_map, const Timestamp safe_point)
{
    std::unique_lock<std::shared_mutex> lock(mutex);

    if (handle_map.empty())
        return;

    auto table_id = getMappedTableID();
    auto & write_map = data.writeCF().getDataMut();

    size_t deleted_gc_cnt = 0, ori_write_map_size = write_map.size();

    // first check, remove duplicate data in current region.
    for (auto write_map_it = write_map.begin(); write_map_it != write_map.end(); ++write_map_it)
    {
        const auto & [pk, ts] = write_map_it->first;

        if (auto it = handle_map.find(pk); it != handle_map.end())
        {
            const auto & [ori_ts, ori_del] = it->second;

            if (ori_ts > ts)
                continue;
            else if (ori_ts == ts)
            {
                UInt8 is_deleted = RegionData::getWriteType(write_map_it) == DelFlag;
                if (is_deleted != ori_del)
                {
                    LOG_ERROR(log,
                        __FUNCTION__ << ": WriteType is not equal, handle: " << it->first << ", tso: " << ts << ", original: " << ori_del
                                     << " , current: " << is_deleted);
                    throw Exception(std::string(__PRETTY_FUNCTION__) + ": original ts >= gc safe point", ErrorCodes::LOGICAL_ERROR);
                }
                handle_map.erase(it);
            }
            else
                handle_map.erase(it);
        }
    }

    // second check, remove same data in current region and handle map. remove deleted data by add a record with DelFlag.
    for (auto it = handle_map.begin(); it != handle_map.end(); ++it)
    {
        const auto & handle = it->first;
        const auto & [ori_ts, ori_del] = it->second;
        std::ignore = ori_del;

        if (ori_ts >= safe_point)
            throw Exception(std::string(__PRETTY_FUNCTION__) + ": original ts >= gc safe point", ErrorCodes::LOGICAL_ERROR);

        auto raw_key = RecordKVFormat::genRawKey(table_id, handle);
        TiKVKey key = RecordKVFormat::encodeAsTiKVKey(raw_key);
        TiKVKey commit_key = RecordKVFormat::appendTs(key, ori_ts);
        TiKVValue value = RecordKVFormat::encodeWriteCfValue(DelFlag, 0);

        data.insert(ColumnFamilyType::Write, std::move(commit_key), std::move(value));
        ++deleted_gc_cnt;
    }

    LOG_DEBUG(log,
        __FUNCTION__ << ": table " << table_id << ", gc safe point " << safe_point << ", original write map size " << ori_write_map_size
                     << ", remain size " << write_map.size());
    if (deleted_gc_cnt)
        LOG_INFO(log, __FUNCTION__ << ": add deleted gc: " << deleted_gc_cnt);
}

EngineStoreApplyRes Region::handleWriteRaftCmd(const WriteCmdsView & cmds, UInt64 index, UInt64 term, TMTContext & tmt)
{
    if (index <= appliedIndex())
    {
        LOG_TRACE(log, toString() << " ignore outdated raft log [term: " << term << ", index: " << index << "]");
        return EngineStoreApplyRes::None;
    }

    auto & context = tmt.getContext();
    Stopwatch watch;
    SCOPE_EXIT({
        auto metrics = context.getTiFlashMetrics();
        GET_METRIC(metrics, tiflash_raft_apply_write_command_duration_seconds, type_write).Observe(watch.elapsedSeconds());
    });

    const auto handle_by_index_func = [&](auto i) {
        auto type = cmds.cmd_types[i];
        auto cf = cmds.cmd_cf[i];
        switch (type)
        {
            case WriteCmdType::Put:
            {
                auto tikv_key = TiKVKey(cmds.keys[i].data, cmds.keys[i].len);
                auto tikv_value = TiKVValue(cmds.vals[i].data, cmds.vals[i].len);
                try
                {
                    doInsert(cf, std::move(tikv_key), std::move(tikv_value));
                }
                catch (Exception & e)
                {
                    LOG_ERROR(log,
                        toString() << " catch exception: " << e.message() << ", while applying CmdType::Put on [term: " << term
                                   << ", index: " << index << "], CF: " << CFToName(cf));
                    e.rethrow();
                }
                break;
            }
            case WriteCmdType::Del:
            {
                auto tikv_key = TiKVKey(cmds.keys[i].data, cmds.keys[i].len);
                try
                {
                    doRemove(cf, tikv_key);
                }
                catch (Exception & e)
                {
                    LOG_ERROR(log,
                        toString() << " catch exception: " << e.message() << ", while applying CmdType::Delete on [term: " << term
                                   << ", index: " << index << "], key in hex: " << tikv_key.toDebugString() << ", CF: " << CFToName(cf));
                    e.rethrow();
                }
                break;
            }
            default:
                throw Exception(
                    std::string(__PRETTY_FUNCTION__) + ": unsupported command type " + std::to_string(static_cast<uint8_t>(type)),
                    ErrorCodes::LOGICAL_ERROR);
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

    {

        std::unique_lock<std::shared_mutex> lock(mutex);
        { // make sure no more write cmd after region is destroyed or merged into other.
            if (auto state = peerState(); state == raft_serverpb::PeerState::Tombstone)
            {
                throw Exception(std::string(__PRETTY_FUNCTION__) + ": " + toString(false) + " execute normal raft cmd at index "
                        + std::to_string(index) + " under state Tombstone, should not happen",
                    ErrorCodes::LOGICAL_ERROR);
            }
        }

        handle_write_cmd_func();

        // If transfer-leader happened during ingest-sst, there might be illegal data.
        if (0 != cmds.len)
        {
            if (tmt.isBgFlushDisabled())
            {
                /// Flush data right after they are committed.
                RegionDataReadInfoList data_list_to_remove;
                RegionTable::writeBlockByRegion(context, shared_from_this(), data_list_to_remove, log, false);
            }
        }

        meta.setApplied(index, term);
    }

    meta.notifyAll();

    return EngineStoreApplyRes::None;
}

void Region::handleIngestSSTInMemory(const SSTViewVec snaps, UInt64 index, UInt64 term, TMTContext & tmt)
{
    if (index <= appliedIndex())
        return;

    {
        auto & ctx = tmt.getContext();

        std::unique_lock<std::shared_mutex> lock(mutex);

        for (UInt64 i = 0; i < snaps.len; ++i)
        {
            auto & snapshot = snaps.views[i];
            auto sst_reader = SSTReader{proxy_helper, snapshot};

            LOG_INFO(log,
                __FUNCTION__ << ": " << this->toString(false) << " begin to ingest sst of cf " << CFToName(snapshot.type)
                             << " at [term: " << term << ", index: " << index << "]");

            uint64_t kv_size = 0;
            while (sst_reader.remained())
            {
                auto key = sst_reader.key();
                auto value = sst_reader.value();
                doInsert(snaps.views[i].type, TiKVKey(key.data, key.len), TiKVValue(value.data, value.len));
                ++kv_size;
                sst_reader.next();
            }

            LOG_INFO(log, __FUNCTION__ << ": " << this->toString(false) << " finish to ingest sst of kv count " << kv_size);
            GET_METRIC(ctx.getTiFlashMetrics(), tiflash_raft_process_keys, type_ingest_sst).Increment(kv_size);
        }
        meta.setApplied(index, term);
    }
    meta.notifyAll();
}

void Region::finishIngestSSTByDTFile(RegionPtr && rhs, UInt64 index, UInt64 term)
{
    if (index <= appliedIndex())
        return;

    {
        std::unique_lock<std::shared_mutex> lock(mutex);

        if (rhs)
        {
            // Merge the uncommitted data from `rhs`
            // (we have taken the ownership of `rhs`, so don't acquire lock on `rhs.mutex`)
            data.mergeFrom(rhs->data);
        }

        meta.setApplied(index, term);
    }
    LOG_INFO(log,
        __FUNCTION__ << ": " << this->toString(false) << " finish to ingest sst by DTFile [write_cf_keys=" << data.write_cf.getSize()
                     << "] [default_cf_keys=" << data.default_cf.getSize() << "] [lock_cf_keys=" << data.lock_cf.getSize() << "]");
    meta.notifyAll();
}

RegionRaftCommandDelegate & Region::makeRaftCommandDelegate(const KVStoreTaskLock & lock)
{
    static_assert(sizeof(RegionRaftCommandDelegate) == sizeof(Region));
    // lock is useless, just to make sure the task mutex of KVStore is locked
    std::ignore = lock;
    return static_cast<RegionRaftCommandDelegate &>(*this);
}

RegionMetaSnapshot Region::dumpRegionMetaSnapshot() const { return meta.dumpRegionMetaSnapshot(); }

Region::Region(RegionMeta && meta_) : Region(std::move(meta_), nullptr) {}

Region::Region(DB::RegionMeta && meta_, const TiFlashRaftProxyHelper * proxy_helper_)
    : meta(std::move(meta_)), log(&Logger::get("Region")), mapped_table_id(meta.getRange()->getMappedTableID()), proxy_helper(proxy_helper_)
{}

TableID Region::getMappedTableID() const { return mapped_table_id; }

void Region::setPeerState(raft_serverpb::PeerState state)
{
    meta.setPeerState(state);
    meta.notifyAll();
}

const RegionRangeKeys & RegionRaftCommandDelegate::getRange() { return *meta.makeRaftCommandDelegate().regionState().getRange(); }
UInt64 RegionRaftCommandDelegate::appliedIndex() { return meta.makeRaftCommandDelegate().applyState().applied_index(); }
metapb::Region Region::getMetaRegion() const { return meta.getMetaRegion(); }
raft_serverpb::MergeState Region::getMergeState() const { return meta.getMergeState(); }

std::pair<size_t, size_t> Region::getApproxMemCacheInfo() const
{
    return {approx_mem_cache_rows.load(std::memory_order_relaxed), approx_mem_cache_bytes.load(std::memory_order_relaxed)};
}

void Region::cleanApproxMemCacheInfo() const
{
    approx_mem_cache_rows = 0;
    approx_mem_cache_bytes = 0;
}

} // namespace DB
