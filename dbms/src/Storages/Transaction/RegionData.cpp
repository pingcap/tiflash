#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/Transaction/RegionData.h>

namespace DB
{

void RegionData::insert(ColumnFamilyType cf, TiKVKey && key, const DecodedTiKVKey & raw_key, TiKVValue && value)
{
    switch (cf)
    {
        case Write:
        {
            size_t size = key.dataSize() + value.dataSize();
            auto res = write_cf.insert(std::move(key), std::move(value), raw_key);
            if (res)
                cf_data_size += size;
            return;
        }
        case Default:
        {
            size_t size = key.dataSize() + value.dataSize();
            default_cf.insert(std::move(key), std::move(value), raw_key);
            cf_data_size += size;
            return;
        }
        case Lock:
        {
            lock_cf.insert(std::move(key), std::move(value), raw_key);
            return;
        }
        default:
            throw Exception(std::string(__PRETTY_FUNCTION__) + " with undefined CF, should not happen", ErrorCodes::LOGICAL_ERROR);
    }
}

void RegionData::removeLockCF(const DecodedTiKVKey & raw_key)
{
    HandleID handle_id = RecordKVFormat::getHandle(raw_key);
    lock_cf.remove(handle_id);
}

void RegionData::removeDefaultCF(const TiKVKey & key, const DecodedTiKVKey & raw_key)
{
    HandleID handle_id = RecordKVFormat::getHandle(raw_key);
    Timestamp ts = RecordKVFormat::getTs(key);
    cf_data_size -= default_cf.remove(RegionDefaultCFData::Key{handle_id, ts}, true);
}

void RegionData::removeWriteCF(const TiKVKey & key, const DecodedTiKVKey & raw_key)
{
    HandleID handle_id = RecordKVFormat::getHandle(raw_key);
    Timestamp ts = RecordKVFormat::getTs(key);

    cf_data_size -= write_cf.remove(RegionWriteCFData::Key{handle_id, ts}, true);
}

RegionData::WriteCFIter RegionData::removeDataByWriteIt(const WriteCFIter & write_it)
{
    const auto & [key, value, decoded_val] = write_it->second;
    const auto & [handle, ts] = write_it->first;
    const auto & [write_type, prewrite_ts, short_str] = decoded_val;

    std::ignore = ts;
    std::ignore = value;

    if (write_type == PutFlag && !short_str)
    {
        auto & map = default_cf.getDataMut();

        if (auto data_it = map.find({handle, prewrite_ts}); data_it != map.end())
        {
            cf_data_size -= RegionDefaultCFData::calcTiKVKeyValueSize(data_it->second);
            map.erase(data_it);
        }
        else
            throw Exception(" key [" + key->toString() + "] not found in data cf when removing", ErrorCodes::LOGICAL_ERROR);
    }

    cf_data_size -= RegionWriteCFData::calcTiKVKeyValueSize(write_it->second);

    return write_cf.getDataMut().erase(write_it);
}

RegionDataReadInfo RegionData::readDataByWriteIt(const ConstWriteCFIter & write_it, bool need_value) const
{
    const auto & [key, value, decoded_val] = write_it->second;
    const auto & [handle, ts] = write_it->first;

    std::ignore = value;

    const auto & [write_type, prewrite_ts, short_value] = decoded_val;

    if (!need_value)
        return std::make_tuple(handle, write_type, ts, nullptr);

    if (write_type != PutFlag)
        return std::make_tuple(handle, write_type, ts, nullptr);

    if (short_value)
        return std::make_tuple(handle, write_type, ts, short_value);


    const auto & map = default_cf.getData();
    if (auto data_it = map.find({handle, prewrite_ts}); data_it != map.end())
        return std::make_tuple(handle, write_type, ts, std::get<1>(data_it->second));
    else
        throw Exception(" key [" + key->toString() + "] not found in data cf", ErrorCodes::LOGICAL_ERROR);
}

// https://github.com/tikv/tikv/blob/master/components/txn_types/src/lock.rs#L179-L203
LockInfoPtr RegionData::getLockInfo(Timestamp start_ts) const
{
    enum LockType : UInt8
    {
        Put = 'P',
        Delete = 'D',
        Lock = 'L',
        Pessimistic = 'S',
    };

    for (const auto & [handle, value] : lock_cf.getData())
    {
        std::ignore = handle;

        const auto & [tikv_key, tikv_val, decoded_val] = value;
        const auto & [lock_type, primary, ts, ttl, data] = decoded_val;
        std::ignore = tikv_val;
        std::ignore = data;

        if (ts > start_ts || lock_type == Lock || lock_type == Pessimistic)
            continue;

        return std::make_unique<LockInfo>(LockInfo{primary, ts, RecordKVFormat::decodeTiKVKey(*tikv_key), ttl});
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
    : write_cf(std::move(data.write_cf)), default_cf(std::move(data.default_cf)), lock_cf(std::move(data.lock_cf))
{}

UInt8 RegionData::getWriteType(const ConstWriteCFIter & write_it) { return RegionWriteCFDataTrait::getWriteType(write_it->second); }

void RegionData::deleteRange(const ColumnFamilyType cf, const RegionRange & range)
{
    switch (cf)
    {
        case Write:
            cf_data_size -= write_cf.deleteRange(range);
            break;
        case Default:
            cf_data_size -= default_cf.deleteRange(range);
            break;
        case Lock:
            cf_data_size -= lock_cf.deleteRange(range);
            break;
        default:
            throw Exception(std::string(__PRETTY_FUNCTION__) + ": undefined CF, should not happen", ErrorCodes::LOGICAL_ERROR);
    }
}

} // namespace DB
