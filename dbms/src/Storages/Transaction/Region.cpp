#include <memory>

#include <Storages/Transaction/Region.h>

#include <tikv/RegionClient.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

const String Region::lock_cf_name = "lock";
const String Region::default_cf_name = "default";
const String Region::write_cf_name = "write";

Region::KVMap::iterator Region::removeDataByWriteIt(const KVMap::iterator & write_it)
{
    auto & write_key = write_it->first;
    auto & write_value = write_it->second;
    auto [write_type, prewrite_ts, short_str] = RecordKVFormat::decodeWriteCfValue(write_value);

    auto bare_key = RecordKVFormat::truncateTs(write_key);
    auto data_key = RecordKVFormat::appendTs(bare_key, prewrite_ts);
    auto data_it = data_cf.find(data_key);

    if (write_type == PutFlag && !short_str)
    {
        if (unlikely(data_it == data_cf.end()))
        {
            throw Exception(
                toString() + " key [" + data_key.toString() + "] not found in data cf when removing", ErrorCodes::LOGICAL_ERROR);
        }

        cf_data_size -= data_it->first.dataSize() + data_it->second.dataSize();
        newly_added_rows--;
        data_cf.erase(data_it);
    }

    cf_data_size -= write_it->first.dataSize() + write_it->second.dataSize();
    newly_added_rows--;

    return write_cf.erase(write_it);
}

Region::ReadInfo Region::readDataByWriteIt(const KVMap::iterator & write_it)
{
    auto & write_key = write_it->first;
    auto & write_value = write_it->second;

    auto [write_type, prewrite_ts, short_value] = RecordKVFormat::decodeWriteCfValue(write_value);

    String decode_key = std::get<0>(RecordKVFormat::decodeTiKVKey(write_key));
    auto commit_ts = RecordKVFormat::getTs(write_key);
    auto handle = RecordKVFormat::getHandle(decode_key);

    if (write_type != PutFlag)
        return std::make_tuple(handle, write_type, commit_ts, TiKVValue());

    auto bare_key = RecordKVFormat::truncateTs(write_key);
    auto data_key = RecordKVFormat::appendTs(bare_key, prewrite_ts);

    if (short_value)
        return std::make_tuple(handle, write_type, commit_ts, TiKVValue(std::move(*short_value)));

    auto data_it = data_cf.find(data_key);
    if (unlikely(data_it == data_cf.end()))
    {
        throw Exception(toString() + " key [" + data_key.toString() + "] not found in data cf", ErrorCodes::LOGICAL_ERROR);
    }

    return std::make_tuple(handle, write_type, commit_ts, data_it->second);
}

Region::LockInfoPtr Region::getLockInfo(TableID expected_table_id, UInt64 start_ts)
{
    // TODO: Should we check data cf as well?
    for (auto && [key, value] : lock_cf)
    {
        auto decode_key = std::get<0>(RecordKVFormat::decodeTiKVKey(key));
        auto table_id = RecordKVFormat::getTableId(decode_key);
        if (expected_table_id != table_id)
        {
            continue;
        }
        auto [lock_type, primary, ts, ttl, data] = RecordKVFormat::decodeLockCfValue(value);
        std::ignore = data;
        if (lock_type == DelFlag || ts > start_ts)
        {
            continue;
        }
        return std::make_unique<LockInfo>(LockInfo{primary, ts, decode_key, ttl});
    }

    return nullptr;
}

void Region::insert(const std::string & cf, const TiKVKey & key, const TiKVValue & value)
{
    // TODO: this will be slow, use batch to speed up
    std::lock_guard<std::mutex> lock(mutex);
    doInsert(cf, key, value);
}

void Region::doInsert(const std::string & cf, const TiKVKey & key, const TiKVValue & value)
{
    // Ignoring all keys other than records.
    String raw_key = std::get<0>(RecordKVFormat::decodeTiKVKey(key));
    if (!RecordKVFormat::isRecord(raw_key))
        return;

    if (isTiDBSystemTable(RecordKVFormat::getTableId(raw_key)))
        return;

    auto & map = getCf(cf);
    auto p = map.try_emplace(key, value);
    if (!p.second)
        throw Exception(toString() + " found existing key [" + key.toString() + "]", ErrorCodes::LOGICAL_ERROR);

    cf_data_size += key.dataSize() + value.dataSize();
    newly_added_rows += 1;
}

void Region::remove(const std::string & cf, const TiKVKey & key)
{
    // TODO: this will be slow, use batch insert to speed up
    std::lock_guard<std::mutex> lock(mutex);
    doRemove(cf, key);
}

void Region::doRemove(const std::string & cf, const TiKVKey & key)
{
    // Ignoring all keys other than records.
    String raw_key = std::get<0>(RecordKVFormat::decodeTiKVKey(key));
    if (!RecordKVFormat::isRecord(raw_key))
        return;

    if (isTiDBSystemTable(RecordKVFormat::getTableId(raw_key)))
        return;

    auto & map = getCf(cf);
    auto it = map.find(key);
    // TODO following exception could throw currently.
    if (it == map.end())
    {
        // tikv gc will delete useless data in write & default cf.
        if (unlikely(&map == &lock_cf))
            LOG_WARNING(log, toString() << " key not found [" << key.toString() << "] in cf " << cf);
        return;
    }

    map.erase(it);

    cf_data_size -= key.dataSize() + it->second.dataSize();
    newly_added_rows--;
}

UInt64 Region::getIndex() {
    return meta.appliedIndex();
}

RegionPtr Region::splitInto(const RegionMeta & meta) const
{
    // TODO: remove data in corresponding partition

    auto [start_key, end_key] = meta.getRange();
    RegionPtr new_region;
    if (client != nullptr) {
        new_region = std::make_shared<Region>(meta, std::make_shared<pingcap::kv::RegionClient>(client->cache, client->client, meta.getRegionVerID()));
    } else {
        new_region = std::make_shared<Region>(meta);
    }

    for (auto && [key, value] : data_cf)
    {
        bool ok = start_key ? key >= start_key : true;
        ok = ok && (end_key ? key < end_key : true);
        if (ok)
        {
            new_region->data_cf.emplace(key, value);
            new_region->cf_data_size += key.dataSize() + value.dataSize();
            new_region->newly_added_rows += 1;
        }
    }

    for (auto && [key, value] : write_cf)
    {
        bool ok = start_key ? key >= start_key : true;
        ok = ok && (end_key ? key < end_key : true);
        if (ok)
        {
            new_region->write_cf.emplace(key, value);
            new_region->cf_data_size += key.dataSize() + value.dataSize();
            new_region->newly_added_rows += 1;
        }
    }

    for (auto && [key, value] : lock_cf)
    {
        bool ok = start_key ? key >= start_key : true;
        ok = ok && (end_key ? key < end_key : true);
        if (ok)
        {
            new_region->lock_cf.emplace(key, value);
            new_region->cf_data_size += key.dataSize() + value.dataSize();
            new_region->newly_added_rows += 1;
        }
    }

    return new_region;
}

void Region::execChangePeer(const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response)
{
    const auto & change_peer = request.change_peer();
    const auto & new_region = response.change_peer().region();

    LOG_INFO(log, toString() << " change peer " << eraftpb::ConfChangeType_Name(change_peer.change_type()));

    switch (change_peer.change_type())
    {
        case eraftpb::ConfChangeType::AddNode:
        case eraftpb::ConfChangeType::AddLearnerNode:
            meta.setRegion(new_region);
            return;
        case eraftpb::ConfChangeType::RemoveNode:
        {
            const auto & peer = change_peer.peer();
            if (meta.peerId() == peer.id())
            {
                // Remove ourself, we will destroy all region data later.
                // So we need not to apply following logs.
                meta.setPendingRemove();
            }
            return;
        }
        default:
            throw Exception("execChangePeer: unsupported cmd", ErrorCodes::LOGICAL_ERROR);
    }
}

const metapb::Peer & findPeer(const metapb::Region & region, UInt64 store_id)
{
    for (const auto & peer : region.peers())
    {
        if (peer.store_id() == store_id)
            return peer;
    }
    throw Exception("peer with store_id " + DB::toString(store_id) + " not found", ErrorCodes::LOGICAL_ERROR);
}

std::pair<RegionPtr, Regions> Region::execBatchSplit(const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response)
{
    const auto & split_reqs = request.splits();
    const auto & new_region_infos = response.splits().regions();

    if (split_reqs.requests().empty())
    {
        LOG_ERROR(log, "execBatchSplit: empty split requests");
        return {};
    }

    std::vector<RegionPtr> split_regions;
    auto store_id = meta.storeId();
    RegionPtr new_region;

    for (const auto & region_info : new_region_infos)
    {
        if (region_info.id() == meta.regionId())
        {
            RegionMeta new_meta(meta.getPeer(), region_info, meta.getApplyState());
            new_region = splitInto(new_meta);
        }
        else
        {
            const auto & peer = findPeer(region_info, store_id);
            RegionMeta new_meta(peer, region_info, initialApplyState());
            auto new_region = splitInto(new_meta);
            split_regions.emplace_back(new_region);
        }
    }

    std::string ids;
    for (const auto & region : split_regions)
        ids += DB::toString(region->id()) + ",";
    ids += DB::toString(new_region->id());
    LOG_INFO(log, toString() << " split into [" << ids << "]");

    return {new_region, split_regions};
}

std::tuple<RegionPtr, std::vector<RegionPtr>, bool> Region::onCommand(const enginepb::CommandRequest & cmd, CmdCallBack & /*callback*/)
{
    auto & header = cmd.header();
    RegionID region_id = header.region_id();
    UInt64 term = header.term();
    UInt64 index = header.index();
    bool sync_log = header.sync_log();

    RegionPtr new_region;
    std::vector<RegionPtr> split_regions;

    if (meta.isPendingRemove())
        throw Exception("Pending remove flag should be false", ErrorCodes::LOGICAL_ERROR);

    if (term == 0 && index == 0)
    {
        if (!sync_log)
            throw Exception("sync_log should be true", ErrorCodes::LOGICAL_ERROR);
        return {{}, {}, true};
    }

    if (!checkIndex(index))
        return {{}, {}, false};

    if (cmd.has_admin_request())
    {
        const auto & request = cmd.admin_request();
        const auto & response = cmd.admin_response();
        auto type = request.cmd_type();

        LOG_TRACE(log,
            "Region [" << region_id << "] execute admin command " << raft_cmdpb::AdminCmdType_Name(type) << " at [term: " << term
                       << ", index: " << index << "]");

        switch (type)
        {
            case raft_cmdpb::AdminCmdType::ChangePeer:
                execChangePeer(request, response);
                break;
            case raft_cmdpb::AdminCmdType::BatchSplit:
                std::tie(new_region, split_regions) = execBatchSplit(request, response);
                break;
            case raft_cmdpb::AdminCmdType::CompactLog:
                // Ignore
                break;
            case raft_cmdpb::AdminCmdType::ComputeHash:
            {
                // Currently hash is borken, because data in region will be flush and remove
                // auto & raft_local_state = header.context();
                // callback.compute_hash(this->shared_from_this(), index, raft_local_state);
                break;
            }
            case raft_cmdpb::AdminCmdType::VerifyHash:
            {
                // const auto & verify_req     = request.verify_hash();
                // auto         expected_index = verify_req.index();
                // const auto & expected_hash  = verify_req.hash();
                // callback.verify_hash(this->shared_from_this(), expected_index, expected_hash);
                break;
            }
            default:
                LOG_ERROR(log, "Unsupported admin command type " << raft_cmdpb::AdminCmdType_Name(type));
                break;
        }
    }
    else
    {
        for (const auto & req : cmd.requests())
        {
            auto type = req.cmd_type();

            LOG_TRACE(log,
                "Region [" << region_id << "] execute command " << raft_cmdpb::CmdType_Name(type) << " at [term: " << term
                           << ", index: " << index << "]");

            switch (type)
            {
                case raft_cmdpb::CmdType::Put:
                {
                    const auto & put = req.put();
                    auto [key, value] = RecordKVFormat::genKV(put);
                    insert(put.cf(), key, value);
                    break;
                }
                case raft_cmdpb::CmdType::Delete:
                {
                    const auto & del = req.delete_();
                    remove(del.cf(), RecordKVFormat::genKey(del));
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
                    throw Exception("Illegal cmd type: " + raft_cmdpb::CmdType_Name(type), ErrorCodes::LOGICAL_ERROR);
            }
        }
    }

    LOG_TRACE(log, toString() << " advance applied index " << index << " and term " << term);

    meta.setApplied(index, term);
    if (new_region)
        (*new_region).meta.setApplied(index, term);

    return {new_region, split_regions, sync_log};
}

size_t Region::serialize(WriteBuffer & buf)
{
    std::lock_guard<std::mutex> lock(mutex);

    size_t total_size = meta.serialize(buf);

    total_size += writeBinary2(data_cf.size(), buf);
    for (auto && [key, value] : data_cf)
    {
        total_size += key.serialize(buf);
        total_size += value.serialize(buf);
    }

    total_size += writeBinary2(write_cf.size(), buf);
    for (auto && [key, value] : write_cf)
    {
        total_size += key.serialize(buf);
        total_size += value.serialize(buf);
    }

    total_size += writeBinary2(lock_cf.size(), buf);
    for (auto && [key, value] : lock_cf)
    {
        total_size += key.serialize(buf);
        total_size += value.serialize(buf);
    }
    return total_size;
}

RegionPtr Region::deserialize(ReadBuffer & buf)
{
    auto region = std::make_shared<Region>(RegionMeta::deserialize(buf));

    auto size = readBinary2<KVMap::size_type>(buf);
    for (size_t i = 0; i < size; ++i)
    {
        auto key = TiKVKey::deserialize(buf);
        auto value = TiKVValue::deserialize(buf);
        region->data_cf.emplace(key, value);
        region->cf_data_size += key.dataSize() + value.dataSize();
        region->newly_added_rows += 1;
    }

    size = readBinary2<KVMap::size_type>(buf);
    for (size_t i = 0; i < size; ++i)
    {
        auto key = TiKVKey::deserialize(buf);
        auto value = TiKVValue::deserialize(buf);
        region->write_cf.emplace(key, value);
        region->cf_data_size += key.dataSize() + value.dataSize();
        region->newly_added_rows += 1;
    }

    size = readBinary2<KVMap::size_type>(buf);
    for (size_t i = 0; i < size; ++i)
    {
        auto key = TiKVKey::deserialize(buf);
        auto value = TiKVValue::deserialize(buf);
        region->lock_cf.emplace(key, value);
        region->cf_data_size += key.dataSize() + value.dataSize();
        region->newly_added_rows += 1;
    }

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
    if (index != expected)
    {
        LOG_WARNING(log, toString() << " expected index: " << DB::toString(expected) << ", got: " << DB::toString(index));
    }
    return true;
}

Region::KVMap & Region::getCf(const std::string & cf)
{
    if (cf.empty() || cf == default_cf_name)
        return data_cf;
    else if (cf == write_cf_name)
        return write_cf;
    else if (cf == lock_cf_name)
        return lock_cf;
    else
        throw Exception("Illegal cf: " + cf, ErrorCodes::LOGICAL_ERROR);
}

void Region::calculateCfCrc32(Crc32 & crc32) const
{
    std::lock_guard<std::mutex> lock1(mutex);

    // TODO: is it calculating key order need?
    auto crc_cal = [&](const Region::KVMap & map) {
        for (auto && [key, value] : map)
        {
            auto encoded_key = DataKVFormat::data_key(key);
            crc32.put(encoded_key.data(), encoded_key.size());
            crc32.put(value.data(), value.dataSize());
        }
    };
    crc_cal(data_cf);
    crc_cal(lock_cf);
    crc_cal(write_cf);
}

RegionID Region::id() const { return meta.regionId(); }

bool Region::isPendingRemove() const { return meta.isPendingRemove(); }

void Region::setPendingRemove() { meta.setPendingRemove(); }

void Region::markPersisted()
{
    std::lock_guard<std::mutex> lock(persist_time_mutex);
    last_persist_time = Poco::Timestamp();
}

const Poco::Timestamp & Region::lastPersistTime() const
{
    std::lock_guard<std::mutex> lock(persist_time_mutex);
    return last_persist_time;
}

std::unique_ptr<Region::CommittedScanRemover> Region::createCommittedScanRemover(TableID expected_table_id)
{
    return std::make_unique<Region::CommittedScanRemover>(this->shared_from_this(), expected_table_id);
}

size_t Region::getNewlyAddedRows() const { return newly_added_rows; }

void Region::resetNewlyAddedRows() { newly_added_rows = 0; }

std::string Region::toString(bool dump_status) const { return meta.toString(dump_status); }

enginepb::CommandResponse Region::toCommandResponse() const { return meta.toCommandResponse(); }

RegionRange Region::getRange() const { return meta.getRange(); }

UInt64 Region::learner_read() {
    if (client != nullptr)
        return client->getReadIndex();
    return 0;
}

void Region::wait_index(UInt64 index) {
    if (client != nullptr) {
        LOG_TRACE(log, "begin to wait learner index : " + std::to_string(index));
        meta.wait_index(index);
    }
}

} // namespace DB
