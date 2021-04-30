#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/Transaction/ColumnFamily.h>
#include <Storages/Transaction/RegionData.h>
#include <Storages/Transaction/RegionLockInfo.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int ILLFORMAT_RAFT_ROW;
} // namespace ErrorCodes

HandleID RawTiDBPK::getHandleID() const
{
    auto & pk = *this;
    return RecordKVFormat::decodeInt64(RecordKVFormat::read<UInt64>(pk->data()));
}

void RegionData::insert(ColumnFamilyType cf, TiKVKey && key, TiKVValue && value)
{
    switch (cf)
    {
        case ColumnFamilyType::Write:
        {
            cf_data_size += write_cf.insert(std::move(key), std::move(value));
            return;
        }
        case ColumnFamilyType::Default:
        {
            cf_data_size += default_cf.insert(std::move(key), std::move(value));
            return;
        }
        case ColumnFamilyType::Lock:
        {
            lock_cf.insert(std::move(key), std::move(value));
            return;
        }
        default:
            throw Exception(std::string(__PRETTY_FUNCTION__) + " with undefined CF, should not happen", ErrorCodes::LOGICAL_ERROR);
    }
}

void RegionData::remove(ColumnFamilyType cf, const TiKVKey & key)
{
    switch (cf)
    {
        case ColumnFamilyType::Write:
        {
            auto raw_key = RecordKVFormat::decodeTiKVKey(key);
            auto pk = RecordKVFormat::getRawTiDBPK(raw_key);
            Timestamp ts = RecordKVFormat::getTs(key);
            // removed by gc, may not exist.
            cf_data_size -= write_cf.remove(RegionWriteCFData::Key{pk, ts}, true);
            return;
        }
        case ColumnFamilyType::Default:
        {
            auto raw_key = RecordKVFormat::decodeTiKVKey(key);
            auto pk = RecordKVFormat::getRawTiDBPK(raw_key);
            Timestamp ts = RecordKVFormat::getTs(key);
            // removed by gc, may not exist.
            cf_data_size -= default_cf.remove(RegionDefaultCFData::Key{pk, ts}, true);
            return;
        }
        case ColumnFamilyType::Lock:
        {
            lock_cf.remove(RegionLockCFDataTrait::Key{nullptr, std::string_view(key.data(), key.dataSize())}, true);
            return;
        }
        default:
            throw Exception(std::string(__PRETTY_FUNCTION__) + " with undefined CF, should not happen", ErrorCodes::LOGICAL_ERROR);
    }
}

RegionData::WriteCFIter RegionData::removeDataByWriteIt(const WriteCFIter & write_it)
{
    const auto & [key, value, decoded_val] = write_it->second;
    const auto & [pk, ts] = write_it->first;

    std::ignore = ts;
    std::ignore = value;
    std::ignore = key;

    if (decoded_val.write_type == RecordKVFormat::CFModifyFlag::PutFlag)
    {
        auto & map = default_cf.getDataMut();

        if (auto data_it = map.find({pk, decoded_val.prewrite_ts}); data_it != map.end())
        {
            cf_data_size -= RegionDefaultCFData::calcTiKVKeyValueSize(data_it->second);
            map.erase(data_it);
        }
    }

    cf_data_size -= RegionWriteCFData::calcTiKVKeyValueSize(write_it->second);

    return write_cf.getDataMut().erase(write_it);
}

RegionDataReadInfo RegionData::readDataByWriteIt(const ConstWriteCFIter & write_it, bool need_value) const
{
    const auto & [key, value, decoded_val] = write_it->second;
    const auto & [pk, ts] = write_it->first;

    std::ignore = value;
    std::ignore = value;

    if (!need_value)
        return std::make_tuple(pk, decoded_val.write_type, ts, nullptr);

    if (decoded_val.write_type != RecordKVFormat::CFModifyFlag::PutFlag)
        return std::make_tuple(pk, decoded_val.write_type, ts, nullptr);

    if (!decoded_val.short_value)
    {
        const auto & map = default_cf.getData();
        if (auto data_it = map.find({pk, decoded_val.prewrite_ts}); data_it != map.end())
            return std::make_tuple(pk, decoded_val.write_type, ts, RegionDefaultCFDataTrait::getTiKVValue(data_it));
        else
            throw Exception("Raw TiDB PK: " + (pk.toDebugString()) + ", Prewrite ts: " + std::to_string(decoded_val.prewrite_ts)
                    + " can not found in default cf for key: " + key->toDebugString(),
                ErrorCodes::ILLFORMAT_RAFT_ROW);
    }

    return std::make_tuple(pk, decoded_val.write_type, ts, decoded_val.short_value);
}

DecodedLockCFValuePtr RegionData::getLockInfo(const RegionLockReadQuery & query) const
{
    for (const auto & [pk, value] : lock_cf.getData())
    {
        std::ignore = pk;

        const auto & [tikv_key, tikv_val, lock_info_ptr] = value;
        std::ignore = tikv_key;
        std::ignore = tikv_val;
        auto & lock_info = *lock_info_ptr;

        if (lock_info.lock_version > query.read_tso || lock_info.lock_type == kvrpcpb::Op::Lock
            || lock_info.lock_type == kvrpcpb::Op::PessimisticLock)
            continue;
        if (lock_info.min_commit_ts > query.read_tso)
            continue;
        if (query.bypass_lock_ts && query.bypass_lock_ts->count(lock_info.lock_version))
            continue;
        return lock_info_ptr;
    }

    return nullptr;
}

void RegionData::splitInto(const RegionRange & range, RegionData & new_region_data)
{
    size_t size_changed = 0;
    size_changed += default_cf.splitInto(range, new_region_data.default_cf);
    size_changed += write_cf.splitInto(range, new_region_data.write_cf);
    size_changed += lock_cf.splitInto(range, new_region_data.lock_cf);
    cf_data_size -= size_changed;
    new_region_data.cf_data_size += size_changed;
}

void RegionData::mergeFrom(const RegionData & ori_region_data)
{
    size_t size_changed = 0;
    size_changed += default_cf.mergeFrom(ori_region_data.default_cf);
    size_changed += write_cf.mergeFrom(ori_region_data.write_cf);
    size_changed += lock_cf.mergeFrom(ori_region_data.lock_cf);
    cf_data_size += size_changed;
}

size_t RegionData::dataSize() const { return cf_data_size; }

void RegionData::assignRegionData(RegionData && new_region_data)
{
    default_cf = std::move(new_region_data.default_cf);
    write_cf = std::move(new_region_data.write_cf);
    lock_cf = std::move(new_region_data.lock_cf);

    cf_data_size = new_region_data.cf_data_size.load();
}

size_t RegionData::serialize(WriteBuffer & buf) const
{
    size_t total_size = 0;

    total_size += default_cf.serialize(buf);
    total_size += write_cf.serialize(buf);
    total_size += lock_cf.serialize(buf);

    return total_size;
}

void RegionData::deserialize(ReadBuffer & buf, RegionData & region_data)
{
    size_t total_size = 0;
    total_size += RegionDefaultCFData::deserialize(buf, region_data.default_cf);
    total_size += RegionWriteCFData::deserialize(buf, region_data.write_cf);
    total_size += RegionLockCFData::deserialize(buf, region_data.lock_cf);

    region_data.cf_data_size += total_size;
}

RegionWriteCFData & RegionData::writeCF() { return write_cf; }
RegionDefaultCFData & RegionData::defaultCF() { return default_cf; }

const RegionWriteCFData & RegionData::writeCF() const { return write_cf; }
const RegionDefaultCFData & RegionData::defaultCF() const { return default_cf; }
const RegionLockCFData & RegionData::lockCF() const { return lock_cf; }

bool RegionData::isEqual(const RegionData & r2) const
{
    return default_cf == r2.default_cf && write_cf == r2.write_cf && lock_cf == r2.lock_cf && cf_data_size == r2.cf_data_size;
}

RegionData::RegionData(RegionData && data)
    : write_cf(std::move(data.write_cf)),
      default_cf(std::move(data.default_cf)),
      lock_cf(std::move(data.lock_cf)),
      cf_data_size(data.cf_data_size.load())
{}

RegionData & RegionData::operator=(RegionData && rhs)
{
    write_cf = std::move(rhs.write_cf);
    default_cf = std::move(rhs.default_cf);
    lock_cf = std::move(rhs.lock_cf);
    cf_data_size = rhs.cf_data_size.load();
    return *this;
}


UInt8 RegionData::getWriteType(const ConstWriteCFIter & write_it) { return RegionWriteCFDataTrait::getWriteType(write_it->second); }

const RegionDefaultCFDataTrait::Map & RegionData::getDefaultCFMap(RegionWriteCFData * write)
{
    auto offset = (size_t) & (((RegionData *)0)->write_cf);
    RegionData * data_ptr = reinterpret_cast<RegionData *>((char *)write - offset);
    return data_ptr->defaultCF().getData();
}

} // namespace DB
