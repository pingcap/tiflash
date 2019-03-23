#include <memory>

#include <Storages/Transaction/Region.h>

#include <tikv/RegionClient.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_FORMAT_VERSION;
} // namespace ErrorCodes

const UInt32 Region::CURRENT_VERSION = 0;

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
        data_cf.erase(data_it);
    }

    cf_data_size -= write_it->first.dataSize() + write_it->second.dataSize();

    return write_cf.erase(write_it);
}

Region::ReadInfo Region::readDataByWriteIt(const KVMap::iterator & write_it, std::vector<TiKVKey> * keys)
{
    auto & write_key = write_it->first;
    auto & write_value = write_it->second;

    if (keys)
        (*keys).push_back(write_key);

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

TableID Region::insert(const std::string & cf, const TiKVKey & key, const TiKVValue & value)
{
    std::lock_guard<std::mutex> lock(mutex);
    return doInsert(cf, key, value);
}

void Region::batchInsert(std::function<bool(BatchInsertNode &)> f)
{
    std::lock_guard<std::mutex> lock(mutex);
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

TableID Region::doInsert(const std::string & cf, const TiKVKey & key, const TiKVValue & value)
{
    // Ignoring all keys other than records.
    String raw_key = std::get<0>(RecordKVFormat::decodeTiKVKey(key));
    if (!RecordKVFormat::isRecord(raw_key))
        return InvalidTableID;

    auto table_id = RecordKVFormat::getTableId(raw_key);
    if (isTiDBSystemTable(table_id))
        return InvalidTableID;

    auto & map = getCf(cf);
    auto p = map.try_emplace(key, value);
    if (!p.second)
        throw Exception(toString() + " found existing key [" + key.toString() + "]", ErrorCodes::LOGICAL_ERROR);

    if (cf != lock_cf_name)
        cf_data_size += key.dataSize() + value.dataSize();

    return table_id;
}

TableID Region::remove(const std::string & cf, const TiKVKey & key)
{
    std::lock_guard<std::mutex> lock(mutex);
    return doRemove(cf, key);
}

TableID Region::doRemove(const std::string & cf, const TiKVKey & key)
{
    // Ignoring all keys other than records.
    String raw_key = std::get<0>(RecordKVFormat::decodeTiKVKey(key));
    if (!RecordKVFormat::isRecord(raw_key))
        return InvalidTableID;

    auto table_id = RecordKVFormat::getTableId(raw_key);
    if (isTiDBSystemTable(table_id))
        return InvalidTableID;

    auto & map = getCf(cf);
    auto it = map.find(key);
    // TODO following exception could throw currently.
    if (it == map.end())
    {
        // tikv gc will delete useless data in write & default cf.
        if (unlikely(&map == &lock_cf))
            LOG_WARNING(log, toString() << " key not found [" << key.toString() << "] in cf " << cf);
        return table_id;
    }

    map.erase(it);

    if (cf != lock_cf_name)
        cf_data_size -= key.dataSize() + it->second.dataSize();
    return table_id;
}

UInt64 Region::getIndex() const { return meta.appliedIndex(); }

RegionPtr Region::splitInto(const RegionMeta & meta)
{
    auto [start_key, end_key] = meta.getRange();
    RegionPtr new_region;
    if (client != nullptr)
        new_region = std::make_shared<Region>(meta, [&](pingcap::kv::RegionVerID) {
            return std::make_shared<pingcap::kv::RegionClient>(client->cache, client->client, meta.getRegionVerID());
        });
    else
        new_region = std::make_shared<Region>(meta);

    for (auto it = data_cf.begin(); it != data_cf.end();)
    {
        bool ok = start_key ? it->first >= start_key : true;
        ok = ok && (end_key ? it->first < end_key : true);
        if (ok)
        {
            cf_data_size -= it->first.dataSize() + it->second.dataSize();
            new_region->cf_data_size += it->first.dataSize() + it->second.dataSize();

            new_region->data_cf.insert(std::move(*it));
            it = data_cf.erase(it);
        }
        else
            ++it;
    }

    for (auto it = write_cf.begin(); it != write_cf.end();)
    {
        bool ok = start_key ? it->first >= start_key : true;
        ok = ok && (end_key ? it->first < end_key : true);
        if (ok)
        {
            cf_data_size -= it->first.dataSize() + it->second.dataSize();
            new_region->cf_data_size += it->first.dataSize() + it->second.dataSize();

            new_region->write_cf.insert(std::move(*it));
            it = write_cf.erase(it);
        }
        else
            ++it;
    }

    for (auto it = lock_cf.begin(); it != lock_cf.end();)
    {
        bool ok = start_key ? it->first >= start_key : true;
        ok = ok && (end_key ? it->first < end_key : true);
        if (ok)
        {
            new_region->lock_cf.insert(std::move(*it));
            it = lock_cf.erase(it);
        }
        else
            ++it;
    }

    return new_region;
}

void Region::execChangePeer(const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response)
{
    const auto & change_peer_request = request.change_peer();
    const auto & new_region = response.change_peer().region();

    LOG_INFO(log, toString() << " change peer " << eraftpb::ConfChangeType_Name(change_peer_request.change_type()));

    switch (change_peer_request.change_type())
    {
        case eraftpb::ConfChangeType::AddNode:
        case eraftpb::ConfChangeType::AddLearnerNode:
        {
            // change the peers of region, add conf_ver.
            meta.setRegion(new_region);
            return;
        }
        case eraftpb::ConfChangeType::RemoveNode:
        {
            const auto & peer = change_peer_request.peer();
            auto store_id = peer.store_id();

            meta.removePeer(store_id);

            if (meta.peerId() == peer.id())
                setPendingRemove();
            return;
        }
        default:
            throw Exception("execChangePeer: unsupported cmd", ErrorCodes::LOGICAL_ERROR);
    }
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

Regions Region::execBatchSplit(const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response)
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
        std::lock_guard<std::mutex> lock(mutex);

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
        meta.swap(new_meta);
    }

    std::stringstream ids;
    for (const auto & region : split_regions)
        ids << region->id() << ",";
    ids << id();
    LOG_INFO(log, toString() << " split into [" << ids.str() << "]");

    return split_regions;
}

std::tuple<std::vector<RegionPtr>, TableIDSet, bool> Region::onCommand(const enginepb::CommandRequest & cmd, CmdCallBack & /*callback*/)
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
    bool need_persist = true;

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
                split_regions = execBatchSplit(request, response);
                break;
            case raft_cmdpb::AdminCmdType::CompactLog:
            case raft_cmdpb::AdminCmdType::ComputeHash:
            case raft_cmdpb::AdminCmdType::VerifyHash:
                // Ignore
                need_persist = false;
                break;
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

            switch (type)
            {
                case raft_cmdpb::CmdType::Put:
                {
                    const auto & put = req.put();
                    auto [key, value] = RecordKVFormat::genKV(put);
                    auto table_id = insert(put.cf(), key, value);
                    if (table_id != InvalidTableID)
                        table_ids.emplace(table_id);
                    break;
                }
                case raft_cmdpb::CmdType::Delete:
                {
                    const auto & del = req.delete_();
                    auto table_id = remove(del.cf(), RecordKVFormat::genKey(del));
                    if (table_id != InvalidTableID)
                        table_ids.emplace(table_id);
                    break;
                }
                case raft_cmdpb::CmdType::DeleteRange:
                case raft_cmdpb::CmdType::IngestSST:
                case raft_cmdpb::CmdType::Snap:
                case raft_cmdpb::CmdType::Get:
                    LOG_WARNING(log, "Region [" << region_id << "] skip unsupported command: " << raft_cmdpb::CmdType_Name(type));
                    need_persist = false;
                    break;
                case raft_cmdpb::CmdType::Prewrite:
                case raft_cmdpb::CmdType::Invalid:
                default:
                    LOG_ERROR(log, "Unsupported command type " << raft_cmdpb::CmdType_Name(type));
                    need_persist = false;
                    break;
            }
        }
    }

    meta.setApplied(index, term);

    if (need_persist)
        incPersistParm();

    for (auto & region : split_regions)
        region->last_persist_time.store(last_persist_time);

    return {split_regions, table_ids, sync_log};
}

size_t Region::serialize(WriteBuffer & buf)
{
    std::lock_guard<std::mutex> lock(mutex);

    size_t total_size = writeBinary2(Region::CURRENT_VERSION, buf);

    total_size += meta.serialize(buf);

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

RegionPtr Region::deserialize(ReadBuffer & buf, const RegionClientCreateFunc & region_client_create)
{
    auto version = readBinary2<UInt32>(buf);
    if (version != Region::CURRENT_VERSION)
        throw Exception("Unexpected region version: " + DB::toString(version) + ", expected: " + DB::toString(CURRENT_VERSION),
            ErrorCodes::UNKNOWN_FORMAT_VERSION);

    auto region = std::make_shared<Region>(RegionMeta::deserialize(buf), region_client_create);

    auto size = readBinary2<KVMap::size_type>(buf);
    for (size_t i = 0; i < size; ++i)
    {
        auto key = TiKVKey::deserialize(buf);
        auto value = TiKVValue::deserialize(buf);
        region->data_cf.emplace(key, value);
        region->cf_data_size += key.dataSize() + value.dataSize();
    }

    size = readBinary2<KVMap::size_type>(buf);
    for (size_t i = 0; i < size; ++i)
    {
        auto key = TiKVKey::deserialize(buf);
        auto value = TiKVValue::deserialize(buf);
        region->write_cf.emplace(key, value);
        region->cf_data_size += key.dataSize() + value.dataSize();
    }

    size = readBinary2<KVMap::size_type>(buf);
    for (size_t i = 0; i < size; ++i)
    {
        auto key = TiKVKey::deserialize(buf);
        auto value = TiKVValue::deserialize(buf);
        region->lock_cf.emplace(key, value);
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

size_t Region::dataSize() const { return cf_data_size; }

void Region::markPersisted() { last_persist_time = Clock::now(); }

Timepoint Region::lastPersistTime() const { return last_persist_time; }

size_t Region::persistParm() const { return persist_parm; }

void Region::decPersistParm(size_t x) { persist_parm -= x; }

void Region::incPersistParm() { persist_parm++; }

std::unique_ptr<Region::CommittedScanRemover> Region::createCommittedScanRemover(TableID expected_table_id)
{
    return std::make_unique<Region::CommittedScanRemover>(this->shared_from_this(), expected_table_id);
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

std::pair<HandleID, HandleID> Region::getHandleRangeByTable(TableID table_id) const
{
    return ::DB::getHandleRangeByTable(getRange(), table_id);
}

std::pair<HandleID, HandleID> getHandleRangeByTable(const std::pair<TiKVKey, TiKVKey> & range, TableID table_id)
{
    return getHandleRangeByTable(range.first, range.second, table_id);
}

std::pair<HandleID, HandleID> getHandleRangeByTable(const TiKVKey & start_key, const TiKVKey & end_key, TableID table_id)
{
    // Example:
    // Range: [100_10, 200_5), table_id: 100, then start_handle: 10, end_handle: MAX_HANDLE_ID

    HandleID start_handle = TiKVRange::getRangeHandle<true>(start_key, table_id);
    HandleID end_handle = TiKVRange::getRangeHandle<false>(end_key, table_id);

    return {start_handle, end_handle};
}

void Region::reset(Region && new_region)
{
    std::lock_guard<std::mutex> lock(mutex);

    data_cf = std::move(new_region.data_cf);
    write_cf = std::move(new_region.write_cf);
    lock_cf = std::move(new_region.lock_cf);

    cf_data_size = new_region.cf_data_size.load();

    incPersistParm();

    meta.swap(new_region.meta);
}

} // namespace DB
