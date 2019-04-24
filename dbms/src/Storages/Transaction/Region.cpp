#include <memory>

#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TiKVRange.h>
#include <tikv/RegionClient.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_FORMAT_VERSION;
} // namespace ErrorCodes

const UInt32 Region::CURRENT_VERSION = 0;

// TODO REVIEW: this names are part of the protocol, we can move these to lower module, such as RegionMeta
const String Region::lock_cf_name = "lock";
const String Region::default_cf_name = "default";
const String Region::write_cf_name = "write";
const String Region::log_name = "Region";

RegionData::WriteCFIter Region::removeDataByWriteIt(const TableID & table_id, const RegionData::WriteCFIter & write_it)
{
    return data.removeDataByWriteIt(table_id, write_it);
}

RegionDataReadInfo Region::readDataByWriteIt(const TableID & table_id, const RegionData::ConstWriteCFIter & write_it) const
{
    return data.readDataByWriteIt(table_id, write_it);
}

LockInfoPtr Region::getLockInfo(TableID expected_table_id, UInt64 start_ts) const { return data.getLockInfo(expected_table_id, start_ts); }

TableID Region::insert(const std::string & cf, const TiKVKey & key, const TiKVValue & value)
{
    std::unique_lock<std::shared_mutex> lock(mutex);
    return doInsert(cf, key, value);
}

void Region::batchInsert(std::function<bool(BatchInsertNode &)> && f)
{
    std::unique_lock<std::shared_mutex> lock(mutex);
    for (;;)
    {
        if (BatchInsertNode p; f(p))
        {
            auto && [k, v, cf] = p;
            doInsert(*cf, *k, *v);
        }
        else
            break;
    }
}

TableID Region::doInsert(const String & cf, const TiKVKey & key, const TiKVValue & value)
{
    // Ignoring all keys other than records.
    String raw_key = RecordKVFormat::decodeTiKVKey(key);
    if (!RecordKVFormat::isRecord(raw_key))
        return InvalidTableID;

    auto table_id = RecordKVFormat::getTableId(raw_key);
    if (isTiDBSystemTable(table_id))
        return InvalidTableID;

    auto type = getCf(cf);
    return data.insert(type, key, raw_key, value);
}

TableID Region::remove(const String & cf, const TiKVKey & key)
{
    std::unique_lock<std::shared_mutex> lock(mutex);
    return doRemove(cf, key);
}

TableID Region::doRemove(const String & cf, const TiKVKey & key)
{
    // Ignoring all keys other than records.
    String raw_key = RecordKVFormat::decodeTiKVKey(key);
    if (!RecordKVFormat::isRecord(raw_key))
        return InvalidTableID;

    auto table_id = RecordKVFormat::getTableId(raw_key);
    if (isTiDBSystemTable(table_id))
        return InvalidTableID;

    auto type = getCf(cf);
    if (type == Lock)
        data.removeLockCF(table_id, raw_key);
    else
    {
        // removed by gc, just ignore.
    }
    return table_id;
}

UInt64 Region::getIndex() const
{
    std::shared_lock<std::shared_mutex> lock(mutex);
    return meta.appliedIndex();
}

// - REVIEW: should lock for meta? what difference between getIndex and getProbableIndex?
UInt64 Region::getProbableIndex() const { return meta.appliedIndex(); }

// - REVIEW: should lock for meta? or remove lock in getIndex
RegionPtr Region::splitInto(const RegionMeta & meta)
{
    RegionPtr new_region;
    if (client != nullptr)
        new_region = std::make_shared<Region>(meta, [&](pingcap::kv::RegionVerID) {
            return std::make_shared<pingcap::kv::RegionClient>(client->cache, client->client, meta.getRegionVerID());
        });
    else
        new_region = std::make_shared<Region>(meta);

    data.splitInto(meta.getRange(), new_region->data);

    return new_region;
}

// - REVIEW: should lock for meta? or remove lock in getIndex
void Region::execChangePeer(const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, UInt64 index, UInt64 term)
{
    const auto & change_peer_request = request.change_peer();

    LOG_INFO(log, toString() << " change peer " << eraftpb::ConfChangeType_Name(change_peer_request.change_type()));

    meta.execChangePeer(request, response, index, term);
}

// TODO REVIEW: Find => find
const metapb::Peer & FindPeer(const metapb::Region & region, UInt64 store_id)
{
    for (const auto & peer : region.peers())
    {
        if (peer.store_id() == store_id)
            return peer;
    }
    throw Exception("peer with store_id " + DB::toString(store_id) + " not found", ErrorCodes::LOGICAL_ERROR);
}

Regions Region::execBatchSplit(
    const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, UInt64 index, UInt64 term)
{
    const auto & split_reqs = request.splits();
    const auto & new_region_infos = response.splits().regions();

    if (split_reqs.requests().empty())
    {
        LOG_ERROR(log, "execBatchSplit: empty split requests");
        return {};
    }

    std::vector<RegionPtr> split_regions;

    {
        std::unique_lock<std::shared_mutex> lock(mutex);

        int new_region_index = 0;
        for (int i = 0; i < new_region_infos.size(); ++i)
        {
            const auto & region_info = new_region_infos[i];
            if (region_info.id() != meta.regionId())
            {
                const auto & peer = FindPeer(region_info, meta.storeId());
                RegionMeta new_meta(peer, region_info, initialApplyState());
                auto split_region = splitInto(new_meta);
                split_regions.emplace_back(split_region);
            }
            else
                new_region_index = i;
        }

        RegionMeta new_meta(meta.getPeer(), new_region_infos[new_region_index], meta.getApplyState());
        meta.reset(std::move(new_meta));
        meta.setApplied(index, term);
    }

    std::stringstream ids;
    for (const auto & region : split_regions)
        ids << region->id() << ",";
    ids << id();
    LOG_INFO(log, toString() << " split into [" << ids.str() << "]");

    return split_regions;
}

std::tuple<std::vector<RegionPtr>, TableIDSet, bool> Region::onCommand(const enginepb::CommandRequest & cmd)
{
    auto & header = cmd.header();
    RegionID region_id = id();
    UInt64 term = header.term();
    UInt64 index = header.index();
    bool sync_log = header.sync_log();

    std::vector<RegionPtr> split_regions;

    if (term == 0 && index == 0)
    {
        if (!sync_log)
            throw Exception("sync_log should be true", ErrorCodes::LOGICAL_ERROR);
        return {{}, {}, sync_log};
    }

    if (!checkIndex(index))
        return {{}, {}, false};

    TableIDSet table_ids;
    bool need_persist = false;

    if (cmd.has_admin_request())
    {
        const auto & request = cmd.admin_request();
        const auto & response = cmd.admin_response();
        auto type = request.cmd_type();

        LOG_INFO(log,
            "Region [" << region_id << "] execute admin command " << raft_cmdpb::AdminCmdType_Name(type) << " at [term: " << term
                       << ", index: " << index << "]");

        switch (type)
        {
            case raft_cmdpb::AdminCmdType::ChangePeer:
                execChangePeer(request, response, index, term);
                need_persist = true;
                break;
            case raft_cmdpb::AdminCmdType::BatchSplit:
                split_regions = execBatchSplit(request, response, index, term);
                need_persist = true;
                break;
            case raft_cmdpb::AdminCmdType::CompactLog:
            case raft_cmdpb::AdminCmdType::ComputeHash:
            case raft_cmdpb::AdminCmdType::VerifyHash:
                // Ignore
                break;
            default:
                LOG_ERROR(log, "Unsupported admin command type " << raft_cmdpb::AdminCmdType_Name(type));
                break;
        }
        // - REVIEW: lock?
        meta.setApplied(index, term);
    }
    else
    {
        std::unique_lock<std::shared_mutex> lock(mutex);

        for (const auto & req : cmd.requests())
        {
            auto type = req.cmd_type();

            switch (type)
            {
                case raft_cmdpb::CmdType::Put:
                {
                    const auto & put = req.put();
                    auto [key, value] = RecordKVFormat::genKV(put);
                    auto table_id = doInsert(put.cf(), key, value);
                    if (table_id != InvalidTableID)
                    {
                        table_ids.emplace(table_id);
                        need_persist = true;
                    }
                    break;
                }
                case raft_cmdpb::CmdType::Delete:
                {
                    const auto & del = req.delete_();
                    auto table_id = doRemove(del.cf(), RecordKVFormat::genKey(del));
                    if (table_id != InvalidTableID)
                    {
                        table_ids.emplace(table_id);
                        need_persist = true;
                    }
                    break;
                }
                case raft_cmdpb::CmdType::DeleteRange:
                case raft_cmdpb::CmdType::IngestSST:
                case raft_cmdpb::CmdType::Snap:
                case raft_cmdpb::CmdType::Get:
                    LOG_WARNING(log, "Region [" << region_id << "] skip unsupported command: " << raft_cmdpb::CmdType_Name(type));
                    break;
                case raft_cmdpb::CmdType::Prewrite:
                case raft_cmdpb::CmdType::Invalid:
                default:
                    LOG_ERROR(log, "Unsupported command type " << raft_cmdpb::CmdType_Name(type));
                    break;
            }
        }
        meta.setApplied(index, term);
    }

    // - REVIEW: notify when setApplied?
    meta.notifyAll();

    // - REVIEW: inc without checking need_persist, and then check this flag when we going to persist
    if (need_persist)
        incPersistParm();

    for (auto & region : split_regions)
        region->last_persist_time.store(last_persist_time);

    return {split_regions, table_ids, sync_log};
}

size_t Region::serialize(WriteBuffer & buf, enginepb::CommandResponse * response) const
{
    std::shared_lock<std::shared_mutex> lock(mutex);

    size_t total_size = writeBinary2(Region::CURRENT_VERSION, buf);

    total_size += meta.serialize(buf);

    total_size += data.serialize(buf);

    if (response != nullptr)
        *response = toCommandResponse();

    return total_size;
}

RegionPtr Region::deserialize(ReadBuffer & buf, const RegionClientCreateFunc * region_client_create)
{
    auto version = readBinary2<UInt32>(buf);
    if (version != Region::CURRENT_VERSION)
        throw Exception("Unexpected region version: " + DB::toString(version) + ", expected: " + DB::toString(CURRENT_VERSION),
            ErrorCodes::UNKNOWN_FORMAT_VERSION);

    auto region = region_client_create == nullptr ? std::make_shared<Region>(RegionMeta::deserialize(buf))
                                                  : std::make_shared<Region>(RegionMeta::deserialize(buf), *region_client_create);

    RegionData::deserialize(buf, region->data);

    region->persist_parm = 0;

    return region;
}

bool Region::checkIndex(UInt64 index)
{
    auto applied_index = meta.appliedIndex();
    if (index <= applied_index)
    {
        LOG_TRACE(log, toString() + " ignore outdated log [" << index << "]");
        return false;
    }
    auto expected = applied_index + 1;
    // TODO REVIEW: if this region receive a snapshot, the expected index will be not applied_index + 1
    if (index != expected)
    {
        LOG_WARNING(log, toString() << " expected index: " << DB::toString(expected) << ", got: " << DB::toString(index));
    }
    return true;
}

ColumnFamilyType Region::getCf(const std::string & cf)
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

bool Region::isPendingRemove() const { return meta.isPendingRemove(); }

void Region::setPendingRemove()
{
    meta.setPendingRemove();
    meta.notifyAll();
}

size_t Region::dataSize() const { return data.dataSize(); }

// - REVIEW: reset persist_parm here?
void Region::markPersisted() { last_persist_time = Clock::now(); }

Timepoint Region::lastPersistTime() const { return last_persist_time; }

size_t Region::persistParm() const { return persist_parm; }

void Region::decPersistParm(size_t x) { persist_parm -= x; }

void Region::incPersistParm() { persist_parm++; }

std::unique_ptr<Region::CommittedScanner> Region::createCommittedScanner(TableID expected_table_id)
{
    return std::make_unique<Region::CommittedScanner>(this->shared_from_this(), expected_table_id);
}

std::unique_ptr<Region::CommittedRemover> Region::createCommittedRemover(TableID expected_table_id)
{
    return std::make_unique<Region::CommittedRemover>(this->shared_from_this(), expected_table_id);
}

std::string Region::toString(bool dump_status) const { return meta.toString(dump_status); }

enginepb::CommandResponse Region::toCommandResponse() const { return meta.toCommandResponse(); }

RegionRange Region::getRange() const { return meta.getRange(); }

UInt64 Region::learnerRead()
{
    if (client != nullptr)
        return client->getReadIndex();
    return 0;
}

void Region::waitIndex(UInt64 index)
{
    if (client != nullptr)
    {
        LOG_TRACE(log, "Region " << id() << " begin to wait learner index: " << index);
        meta.waitIndex(index);
        LOG_TRACE(log, "Region " << id() << " wait learner index done");
    }
}

UInt64 Region::version() const { return meta.version(); }

UInt64 Region::confVer() const { return meta.confVer(); }

HandleRange<HandleID> Region::getHandleRangeByTable(TableID table_id) const
{
    return TiKVRange::getHandleRangeByTable(getRange(), table_id);
}

// - REVIEW: better method name, eg: assign
void Region::reset(Region && new_region)
{
    std::unique_lock<std::shared_mutex> lock(mutex);

    data.reset(std::move(new_region.data));

    incPersistParm();

    meta.reset(std::move(new_region.meta));
    meta.notifyAll();
}

bool Region::isPeerRemoved() const { return meta.isPeerRemoved(); }

TableIDSet Region::getCommittedRecordTableID() const
{
    std::shared_lock<std::shared_mutex> lock(mutex);
    return data.getCommittedRecordTableID();
}

} // namespace DB
