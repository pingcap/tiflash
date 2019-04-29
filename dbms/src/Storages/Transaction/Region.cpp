#include <memory>

#include <Storages/Transaction/RaftCommandResult.h>
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

const std::string Region::lock_cf_name = "lock";
const std::string Region::default_cf_name = "default";
const std::string Region::write_cf_name = "write";
const std::string Region::log_name = "Region";

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

void Region::batchInsert(std::function<bool(BatchInsertElement &)> && f)
{
    std::unique_lock<std::shared_mutex> lock(mutex);
    for (;;)
    {
        if (BatchInsertElement p; f(p))
        {
            auto && [k, v, cf] = p;
            doInsert(*cf, *k, *v);
        }
        else
            break;
    }
}

TableID Region::doInsert(const std::string & cf, const TiKVKey & key, const TiKVValue & value)
{
    // Ignoring all keys other than records.
    std::string raw_key = RecordKVFormat::decodeTiKVKey(key);
    if (!RecordKVFormat::isRecord(raw_key))
        return InvalidTableID;

    auto table_id = RecordKVFormat::getTableId(raw_key);
    if (isTiDBSystemTable(table_id))
        return InvalidTableID;

    auto type = getCf(cf);
    return data.insert(type, key, raw_key, value);
}

TableID Region::remove(const std::string & cf, const TiKVKey & key)
{
    std::unique_lock<std::shared_mutex> lock(mutex);
    return doRemove(cf, key);
}

TableID Region::doRemove(const std::string & cf, const TiKVKey & key)
{
    // Ignoring all keys other than records.
    std::string raw_key = RecordKVFormat::decodeTiKVKey(key);
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

UInt64 Region::getProbableIndex() const { return meta.appliedIndex(); }

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

void Region::execChangePeer(const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, UInt64 index, UInt64 term)
{
    const auto & change_peer_request = request.change_peer();

    LOG_INFO(log, toString() << " change peer " << eraftpb::ConfChangeType_Name(change_peer_request.change_type()));

    meta.execChangePeer(request, response, index, term);
}

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

        int new_region_index = -1;
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
            {
                if (new_region_index == -1)
                    new_region_index = i;
                else
                    throw Exception("Region::execBatchSplit duplicate region index", ErrorCodes::LOGICAL_ERROR);
            }
        }

        if (new_region_index == -1)
            throw Exception("Region::execBatchSplit region index not found", ErrorCodes::LOGICAL_ERROR);

        RegionMeta new_meta(meta.getPeer(), new_region_infos[new_region_index], meta.getApplyState());
        new_meta.setApplied(index, term);
        meta.assignRegionMeta(std::move(new_meta));
    }

    std::stringstream ids;
    for (const auto & region : split_regions)
        ids << region->id() << ",";
    ids << id();
    LOG_INFO(log, toString() << " split into [" << ids.str() << "]");

    return split_regions;
}

RaftCommandResult Region::onCommand(const enginepb::CommandRequest & cmd)
{
    auto & header = cmd.header();
    RegionID region_id = id();
    UInt64 term = header.term();
    UInt64 index = header.index();
    bool sync_log = header.sync_log();

    RaftCommandResult result{sync_log, DefaultResult{}};

    {
        auto applied_index = meta.appliedIndex();
        if (index <= applied_index)
        {
            result.inner = IndexError{};
            if (term == 0 && index == 0)
            {
                // special cmd, used to heart beat and sync log, just ignore
            }
            else
                LOG_WARNING(log, toString() + " ignore outdated raft log [term: " << term << ", index: " << index << "]");
            return result;
        }
    }

    bool is_dirty = false;

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
            {
                execChangePeer(request, response, index, term);
                result.inner = ChangePeer{};

                break;
            }
            case raft_cmdpb::AdminCmdType::BatchSplit:
            {
                Regions split_regions = execBatchSplit(request, response, index, term);
                for (auto & region : split_regions)
                    region->last_persist_time.store(last_persist_time);

                result.inner = BatchSplit{split_regions};

                is_dirty = true;
                break;
            }
            case raft_cmdpb::AdminCmdType::CompactLog:
            case raft_cmdpb::AdminCmdType::ComputeHash:
            case raft_cmdpb::AdminCmdType::VerifyHash:
                // Ignore
                meta.setApplied(index, term);
                break;
            default:
                throw Exception("Unsupported admin command type " + raft_cmdpb::AdminCmdType_Name(type), ErrorCodes::LOGICAL_ERROR);
                break;
        }
    }
    else
    {
        TableIDSet table_ids;

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
                        is_dirty = true;
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
                        is_dirty = true;
                    }
                    break;
                }
                case raft_cmdpb::CmdType::Snap:
                case raft_cmdpb::CmdType::Get:
                case raft_cmdpb::CmdType::ReadIndex:
                    LOG_WARNING(log, "Region [" << region_id << "] skip unsupported command: " << raft_cmdpb::CmdType_Name(type));
                    break;
                default:
                {
                    throw Exception("Unsupported command type " + raft_cmdpb::CmdType_Name(type), ErrorCodes::LOGICAL_ERROR);
                    break;
                }
            }
        }
        meta.setApplied(index, term);
        result.inner = UpdateTableID{table_ids};
    }

    meta.notifyAll();

    if (is_dirty)
        incDirtyFlag();

    return result;
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

    region->dirty_flag = 0;

    return region;
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

void Region::markPersisted() { last_persist_time = Clock::now(); }

Timepoint Region::lastPersistTime() const { return last_persist_time; }

size_t Region::dirtyFlag() const { return dirty_flag; }

void Region::decDirtyFlag(size_t x) { dirty_flag -= x; }

void Region::incDirtyFlag() { dirty_flag++; }

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
        if (!meta.checkIndex(index))
        {
            LOG_DEBUG(log, "Region " << id() << " need to wait learner index: " << index);
            meta.waitIndex(index);
            LOG_DEBUG(log, "Region " << id() << " wait learner index " << index << " done");
        }
    }
}

UInt64 Region::version() const { return meta.version(); }

UInt64 Region::confVer() const { return meta.confVer(); }

HandleRange<HandleID> Region::getHandleRangeByTable(TableID table_id) const
{
    return TiKVRange::getHandleRangeByTable(getRange(), table_id);
}

void Region::assignRegion(Region && new_region)
{
    std::unique_lock<std::shared_mutex> lock(mutex);

    data.assignRegionData(std::move(new_region.data));

    incDirtyFlag();

    meta.assignRegionMeta(std::move(new_region.meta));
    meta.notifyAll();
}

bool Region::isPeerRemoved() const { return meta.isPeerRemoved(); }

TableIDSet Region::getCommittedRecordTableID() const
{
    std::shared_lock<std::shared_mutex> lock(mutex);
    return data.getCommittedRecordTableID();
}

} // namespace DB
