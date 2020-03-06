#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/PDTiKVClient.h>
#include <Storages/Transaction/RaftCommandResult.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TiKVRange.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_FORMAT_VERSION;
} // namespace ErrorCodes

const UInt32 Region::CURRENT_VERSION = 1;

const std::string Region::lock_cf_name = "lock";
const std::string Region::default_cf_name = "default";
const std::string Region::write_cf_name = "write";
const std::string Region::log_name = "Region";

RegionData::WriteCFIter Region::removeDataByWriteIt(const RegionData::WriteCFIter & write_it) { return data.removeDataByWriteIt(write_it); }

RegionDataReadInfo Region::readDataByWriteIt(const RegionData::ConstWriteCFIter & write_it, bool need_value) const
{
    return data.readDataByWriteIt(write_it, need_value);
}

LockInfoPtr Region::getLockInfo(UInt64 start_ts) const { return data.getLockInfo(start_ts); }

void Region::insert(const std::string & cf, TiKVKey && key, TiKVValue && value)
{
    return insert(getCfType(cf), std::move(key), std::move(value));
}

void Region::insert(ColumnFamilyType type, TiKVKey && key, TiKVValue && value)
{
    std::unique_lock<std::shared_mutex> lock(mutex);
    return doInsert(type, std::move(key), std::move(value));
}

void Region::doInsert(ColumnFamilyType type, TiKVKey && key, TiKVValue && value)
{
    auto raw_key = RecordKVFormat::decodeTiKVKey(key);
    doCheckTable(raw_key);

    data.insert(type, std::move(key), raw_key, std::move(value));
}

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
    doRemove(getCfType(cf), key);
}

void Region::doRemove(ColumnFamilyType type, const TiKVKey & key)
{
    auto raw_key = RecordKVFormat::decodeTiKVKey(key);
    doCheckTable(raw_key);

    switch (type)
    {
        case Lock:
            data.removeLockCF(raw_key);
            break;
        case Default:
        {
            // there may be some prewrite data, may not exist, don't throw exception.
            data.removeDefaultCF(key, raw_key);
            break;
        }
        case Write:
        {
            // removed by gc, may not exist.
            data.removeWriteCF(key, raw_key);
            break;
        }
    }
}

UInt64 Region::appliedIndex() const { return meta.appliedIndex(); }

RegionPtr Region::splitInto(RegionMeta && meta)
{
    RegionPtr new_region;
    if (index_reader != nullptr)
    {
        new_region = std::make_shared<Region>(std::move(meta), [&]() {
            return std::make_shared<IndexReader>(index_reader->cluster);
        });
    }
    else
        new_region = std::make_shared<Region>(std::move(meta));

    const auto range = new_region->getRange();
    data.splitInto(range->comparableKeys(), new_region->data);

    return new_region;
}

void RegionRaftCommandDelegate::execChangePeer(
    const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, const UInt64 index, const UInt64 term)
{
    const auto & change_peer_request = request.change_peer();

    LOG_INFO(log, toString(false) << " execute change peer type: " << eraftpb::ConfChangeType_Name(change_peer_request.change_type()));

    meta.makeRaftCommandDelegate().execChangePeer(request, response, index, term);
}

static const metapb::Peer & findPeer(const metapb::Region & region, UInt64 store_id)
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
                const auto & peer = findPeer(region_info, meta.storeId());
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

    std::stringstream ids;
    for (const auto & region : split_regions)
        ids << region->id() << ",";
    ids << id();
    LOG_INFO(log, toString(false) << " split into [" << ids.str() << "]");

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

RegionID RegionRaftCommandDelegate::execCommitMerge(const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse &,
    const UInt64 index, const UInt64 term, const KVStore & kvstore, RegionTable & region_table)
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

        meta_delegate.execCommitMerge(res, index, term, source_region_meta_delegate);
    }

    return source_meta.id();
}

void RegionRaftCommandDelegate::handleAdminRaftCmd(const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response,
    UInt64 index, UInt64 term, const KVStore & kvstore, RegionTable & region_table, RaftCommandResult & result)
{
    result.type = RaftCommandResult::Type::Default;
    if (index <= appliedIndex())
    {
        LOG_WARNING(log, toString() << " ignore outdated raft log [term: " << term << ", index: " << index << "]");
        result.type = RaftCommandResult::Type::IndexError;
        return;
    }

    auto type = request.cmd_type();

    switch (type)
    {
        case raft_cmdpb::AdminCmdType::ComputeHash:
        case raft_cmdpb::AdminCmdType::VerifyHash:
        case raft_cmdpb::AdminCmdType::CompactLog:
            LOG_INFO(log,
                toString(false) << " useless admin command " << raft_cmdpb::AdminCmdType_Name(type) << " at [term: " << term
                                << ", index: " << index << "]");
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
        {
            execChangePeer(request, response, index, term);
            result.type = RaftCommandResult::Type::ChangePeer;

            break;
        }
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

RegionPtr Region::deserialize(ReadBuffer & buf, const IndexReaderCreateFunc * index_reader_create)
{
    auto version = readBinary2<UInt32>(buf);
    if (version != Region::CURRENT_VERSION)
        throw Exception(std::string(__PRETTY_FUNCTION__) + ": unexpected version: " + DB::toString(version)
                + ", expected: " + DB::toString(CURRENT_VERSION),
            ErrorCodes::UNKNOWN_FORMAT_VERSION);

    auto meta = RegionMeta::deserialize(buf);
    auto region = index_reader_create == nullptr ? std::make_shared<Region>(std::move(meta))
                                                 : std::make_shared<Region>(std::move(meta), *index_reader_create);

    RegionData::deserialize(buf, region->data);
    return region;
}

ColumnFamilyType Region::getCfType(const std::string & cf)
{
    if (cf.empty() || cf == default_cf_name)
        return ColumnFamilyType::Default;
    else if (cf == write_cf_name)
        return ColumnFamilyType::Write;
    else if (cf == lock_cf_name)
        return ColumnFamilyType::Lock;
    else
        throw Exception("Illegal cf: " + cf, ErrorCodes::LOGICAL_ERROR);
}

RegionID Region::id() const { return meta.regionId(); }

bool Region::isPendingRemove() const { return peerState() == raft_serverpb::PeerState::Tombstone; }

void Region::setPendingRemove()
{
    meta.setPeerState(raft_serverpb::PeerState::Tombstone);
    meta.notifyAll();
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

void Region::markPersisted() const { last_persist_time = Clock::now(); }

Timepoint Region::lastPersistTime() const { return last_persist_time; }

void Region::markCompactLog() const { last_compact_log_time = Clock::now(); }

Timepoint Region::lastCompactLogTime() const { return last_compact_log_time; }

Region::CommittedScanner Region::createCommittedScanner() { return Region::CommittedScanner(this->shared_from_this()); }

Region::CommittedRemover Region::createCommittedRemover() { return Region::CommittedRemover(this->shared_from_this()); }

std::string Region::toString(bool dump_status) const { return meta.toString(dump_status); }

ImutRegionRangePtr Region::getRange() const { return meta.getRange(); }

ReadIndexResult Region::learnerRead()
{
    if (index_reader != nullptr)
        return index_reader->getReadIndex(meta.getRegionVerID());
    return {};
}

void Region::waitIndex(UInt64 index)
{
    if (index_reader != nullptr)
    {
        if (!meta.checkIndex(index))
        {
            LOG_DEBUG(log, toString() << " need to wait learner index: " << index);
            meta.waitIndex(index);
            LOG_DEBUG(log, toString(false) << " wait learner index " << index << " done");
        }
    }
}

UInt64 Region::version() const { return meta.version(); }

UInt64 Region::confVer() const { return meta.confVer(); }

HandleRange<HandleID> Region::getHandleRangeByTable(TableID table_id) const { return getRange()->getHandleRangeByTable(table_id); }

void Region::assignRegion(Region && new_region)
{
    std::unique_lock<std::shared_mutex> lock(mutex);

    data.assignRegionData(std::move(new_region.data));

    meta.assignRegionMeta(std::move(new_region.meta));
    meta.notifyAll();
}

bool Region::isPeerRemoved() const { return meta.isPeerRemoved(); }

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
        const auto & [handle, ts] = write_map_it->first;

        if (auto it = handle_map.find(handle); it != handle_map.end())
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
                        __FUNCTION__ << ": WriteType is not equal, handle: " << handle << ", tso: " << ts << ", original: " << ori_del
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

        data.insert(Write, std::move(commit_key), raw_key, std::move(value));
        ++deleted_gc_cnt;
    }

    LOG_DEBUG(log,
        __FUNCTION__ << ": table " << table_id << ", gc safe point " << safe_point << ", original write map size " << ori_write_map_size
                     << ", remain size " << write_map.size());
    if (deleted_gc_cnt)
        LOG_INFO(log, __FUNCTION__ << ": add deleted gc: " << deleted_gc_cnt);
}

void Region::handleWriteRaftCmd(raft_cmdpb::RaftCmdRequest && request, UInt64 index, UInt64 term, bool set_applied)
{

    if (index <= appliedIndex())
    {
        LOG_WARNING(log, toString() << " ignore outdated raft log [term: " << term << ", index: " << index << "]");
        return;
    }

    {
        std::unique_lock<std::shared_mutex> lock(mutex);
        std::lock_guard<std::mutex> predecode_lock(predecode_mutex);

        for (auto && req : *request.mutable_requests())
        {
            auto type = req.cmd_type();

            switch (type)
            {
                case raft_cmdpb::CmdType::Put:
                {
                    auto & put = *req.mutable_put();

                    auto & key = *put.mutable_key();
                    auto & value = *put.mutable_value();

                    auto tikv_key = TiKVKey(std::move(key));
                    auto tikv_value = TiKVValue(std::move(value));

                    try
                    {
                        doInsert(getCfType(put.cf()), std::move(tikv_key), std::move(tikv_value));
                    }
                    catch (Exception & e)
                    {
                        LOG_ERROR(log,
                            toString() << " catch exception: " << e.message() << ", while applying CmdType::Put on [term: " << term
                                       << ", index: " << index << "], CF: " << put.cf());
                        e.rethrow();
                    }

                    break;
                }
                case raft_cmdpb::CmdType::Delete:
                {
                    auto & del = *req.mutable_delete_();

                    auto & key = *del.mutable_key();
                    auto tikv_key = TiKVKey(std::move(key));

                    try
                    {
                        doRemove(getCfType(del.cf()), tikv_key);
                    }
                    catch (Exception & e)
                    {
                        LOG_ERROR(log,
                            toString() << " catch exception: " << e.message() << ", while applying CmdType::Delete on [term: " << term
                                       << ", index: " << index << "], key in hex: " << tikv_key.toHex() << ", CF: " << del.cf());
                        e.rethrow();
                    }

                    break;
                }
                case raft_cmdpb::CmdType::Snap:
                case raft_cmdpb::CmdType::Get:
                case raft_cmdpb::CmdType::ReadIndex:
                    LOG_WARNING(log, toString(false) << " skip unsupported command: " << raft_cmdpb::CmdType_Name(type));
                    break;
                case raft_cmdpb::CmdType::DeleteRange:
                {
                    auto & delete_range = *req.mutable_delete_range();
                    const auto & cf = delete_range.cf();
                    auto start = TiKVKey(std::move(*delete_range.mutable_start_key()));
                    auto end = TiKVKey(std::move(*delete_range.mutable_end_key()));

                    LOG_INFO(log,
                        toString(false) << " start to execute " << raft_cmdpb::CmdType_Name(type) << ", CF: " << cf
                                        << ", start key in hex: " << start.toHex() << ", end key in hex: " << end.toHex());
                    doDeleteRange(cf, RegionRangeKeys::makeComparableKeys(std::move(start), std::move(end)));
                    break;
                }
                default:
                {
                    throw Exception(std::string(__PRETTY_FUNCTION__) + ": unsupported command type " + raft_cmdpb::CmdType_Name(type),
                        ErrorCodes::LOGICAL_ERROR);
                    break;
                }
            }
        }

        if (set_applied_index)
            meta.setApplied(index, term);
    }

    if (set_applied_index)
        meta.notifyAll();
}

RegionRaftCommandDelegate & Region::makeRaftCommandDelegate(const KVStoreTaskLock & lock)
{
    static_assert(sizeof(RegionRaftCommandDelegate) == sizeof(Region));
    // lock is useless, just to make sure the task mutex of KVStore is locked
    std::ignore = lock;
    return static_cast<RegionRaftCommandDelegate &>(*this);
}

void Region::compareAndUpdateHandleMaps(const Region & source_region, HandleMap & handle_map)
{
    const auto range = getRange();
    const auto & [start_key, end_key] = range->comparableKeys();
    {
        std::shared_lock<std::shared_mutex> source_lock(source_region.mutex);

        const auto & write_map = source_region.data.writeCF().getData();
        if (write_map.empty())
            return;

        for (auto write_map_it = write_map.begin(); write_map_it != write_map.end(); ++write_map_it)
        {
            const auto & key = RegionWriteCFData::getTiKVKey(write_map_it->second);

            if (start_key.compare(key) <= 0 && end_key.compare(key) > 0)
                ;
            else
                continue;

            const auto & [handle, ts] = write_map_it->first;
            const HandleMap::mapped_type cur_ele = {ts, RegionData::getWriteType(write_map_it) == DelFlag};
            auto [it, ok] = handle_map.emplace(handle, cur_ele);
            if (!ok)
            {
                auto & ele = it->second;
                ele = std::max(ele, cur_ele);
            }
        }

        LOG_DEBUG(log, __FUNCTION__ << ": memory cache: source " << source_region.toString(false) << ", record size " << write_map.size());
    }
}

void Region::doDeleteRange(const std::string & cf, const RegionRange & range)
{
    auto type = getCfType(cf);
    return data.deleteRange(type, range);
}

std::tuple<RegionVersion, RegionVersion, ImutRegionRangePtr> Region::dumpVersionRange() const { return meta.dumpVersionRange(); }

Region::Region(RegionMeta && meta_) : Region(std::move(meta_), []() { return nullptr; }) {}

Region::Region(DB::RegionMeta && meta_, const DB::IndexReaderCreateFunc & index_reader_create)
    : meta(std::move(meta_)),
      index_reader(index_reader_create()),
      log(&Logger::get(log_name)),
      mapped_table_id(meta.getRange()->getMappedTableID())
{}

TableID Region::getMappedTableID() const { return mapped_table_id; }

const RegionRangeKeys & RegionRaftCommandDelegate::getRange() { return *meta.makeRaftCommandDelegate().regionState().getRange(); }
UInt64 RegionRaftCommandDelegate::appliedIndex() { return meta.makeRaftCommandDelegate().applyState().applied_index(); }
metapb::Region Region::getMetaRegion() const { return meta.getMetaRegion(); }
raft_serverpb::MergeState Region::getMergeState() const { return meta.getMergeState(); }

} // namespace DB
