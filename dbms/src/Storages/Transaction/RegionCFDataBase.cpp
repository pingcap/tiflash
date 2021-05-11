#include <Storages/Transaction/RegionCFDataBase.h>
#include <Storages/Transaction/RegionCFDataTrait.h>
#include <Storages/Transaction/RegionData.h>
#include <Storages/Transaction/RegionRangeKeys.h>

namespace DB
{

using CFModifyFlag = RecordKVFormat::CFModifyFlag;

template <typename Trait>
const TiKVKey & RegionCFDataBase<Trait>::getTiKVKey(const Value & val)
{
    return *std::get<0>(val);
}

template <typename Value>
const std::shared_ptr<const TiKVValue> & getTiKVValuePtr(const Value & val)
{
    return std::get<1>(val);
}

template <typename Trait>
const TiKVValue & RegionCFDataBase<Trait>::getTiKVValue(const Value & val)
{
    return *getTiKVValuePtr<Value>(val);
}

template <typename Trait>
RegionDataRes RegionCFDataBase<Trait>::insert(TiKVKey && key, TiKVValue && value)
{
    const auto & raw_key = RecordKVFormat::decodeTiKVKey(key);
    auto kv_pair = Trait::genKVPair(std::move(key), raw_key, std::move(value));
    if (!kv_pair)
        return 0;

    return insert(std::move(*kv_pair));
}

template <>
RegionDataRes RegionCFDataBase<RegionLockCFDataTrait>::insert(TiKVKey && key, TiKVValue && value)
{
    Pair kv_pair = RegionLockCFDataTrait::genKVPair(std::move(key), std::move(value));
    // according to the process of pessimistic lock, just overwrite.
    data.insert_or_assign(std::move(kv_pair.first), std::move(kv_pair.second));
    return 0;
}

template <typename Trait>
RegionDataRes RegionCFDataBase<Trait>::insert(std::pair<Key, Value> && kv_pair)
{
    auto & map = data;
    auto [it, ok] = map.emplace(std::move(kv_pair));
    if (!ok)
        throw Exception("Found existing key in hex: " + getTiKVKey(it->second).toDebugString(), ErrorCodes::LOGICAL_ERROR);

    return calcTiKVKeyValueSize(it->second);
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::calcTiKVKeyValueSize(const Value & value)
{
    return calcTiKVKeyValueSize(getTiKVKey(value), getTiKVValue(value));
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::calcTiKVKeyValueSize(const TiKVKey & key, const TiKVValue & value)
{
    if constexpr (std::is_same<Trait, RegionLockCFDataTrait>::value)
    {
        std::ignore = key;
        std::ignore = value;
        return 0;
    }
    else
        return key.dataSize() + value.dataSize();
}


template <typename Trait>
bool RegionCFDataBase<Trait>::shouldIgnoreRemove(const RegionCFDataBase::Value &)
{
    return false;
}

template <>
bool RegionCFDataBase<RegionWriteCFDataTrait>::shouldIgnoreRemove(const RegionCFDataBase::Value & value)
{
    return RegionWriteCFDataTrait::getWriteType(value) == CFModifyFlag::DelFlag;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::remove(const Key & key, bool quiet)
{
    auto & map = data;

    if (auto it = map.find(key); it != map.end())
    {
        const Value & value = it->second;

        if (shouldIgnoreRemove(value))
            return 0;

        size_t size = calcTiKVKeyValueSize(value);
        map.erase(it);
        return size;
    }
    else if (!quiet)
        throw Exception("Key not found", ErrorCodes::LOGICAL_ERROR);

    return 0;
}

template <typename Trait>
bool RegionCFDataBase<Trait>::cmp(const Map & a, const Map & b)
{
    if (a.size() != b.size())
        return false;
    for (const auto & [key, value] : a)
    {
        if (auto it = b.find(key); it != b.end())
        {
            if (getTiKVKey(value) != getTiKVKey(it->second) || getTiKVValue(value) != getTiKVValue(it->second))
                return false;
        }
        else
            return false;
    }
    return true;
}

template <typename Trait>
bool RegionCFDataBase<Trait>::operator==(const RegionCFDataBase & cf) const
{
    return cmp(cf.data, data);
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::getSize() const
{
    return data.size();
}

template <typename Trait>
RegionCFDataBase<Trait>::RegionCFDataBase(RegionCFDataBase && region) : data(std::move(region.data))
{}

template <typename Trait>
RegionCFDataBase<Trait> & RegionCFDataBase<Trait>::operator=(RegionCFDataBase && region)
{
    data = std::move(region.data);
    return *this;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::mergeFrom(const RegionCFDataBase & ori_region_data)
{
    size_t size_changed = 0;

    const auto & ori_map = ori_region_data.data;
    auto & tar_map = data;

    for (auto it = ori_map.begin(); it != ori_map.end(); it++)
    {
        size_changed += calcTiKVKeyValueSize(it->second);
        auto ok = tar_map.emplace(*it).second;
        if (!ok)
            throw Exception(std::string(__PRETTY_FUNCTION__) + ": got duplicate key", ErrorCodes::LOGICAL_ERROR);
    }

    return size_changed;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::splitInto(const RegionRange & range, RegionCFDataBase & new_region_data)
{
    const auto & [start_key, end_key] = range;
    size_t size_changed = 0;

    {
        auto & ori_map = data;
        auto & tar_map = new_region_data.data;

        for (auto it = ori_map.begin(); it != ori_map.end();)
        {
            const auto & key = getTiKVKey(it->second);

            if (start_key.compare(key) <= 0 && end_key.compare(key) > 0)
            {
                size_changed += calcTiKVKeyValueSize(it->second);
                tar_map.insert(std::move(*it));
                it = ori_map.erase(it);
            }
            else
                ++it;
        }
    }
    return size_changed;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::serialize(WriteBuffer & buf) const
{
    size_t total_size = 0;

    size_t size = getSize();

    total_size += writeBinary2(size, buf);

    for (const auto & ele : data)
    {
        const auto & key = getTiKVKey(ele.second);
        const auto & value = getTiKVValue(ele.second);
        total_size += key.serialize(buf);
        total_size += value.serialize(buf);
    }

    return total_size;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::deserialize(ReadBuffer & buf, RegionCFDataBase & new_region_data)
{
    size_t size = readBinary2<size_t>(buf);
    size_t cf_data_size = 0;
    for (size_t i = 0; i < size; ++i)
    {
        auto key = TiKVKey::deserialize(buf);
        auto value = TiKVValue::deserialize(buf);
        cf_data_size += new_region_data.insert(std::move(key), std::move(value));
    }
    return cf_data_size;
}

template <typename Trait>
const typename RegionCFDataBase<Trait>::Data & RegionCFDataBase<Trait>::getData() const
{
    return data;
}

template <typename Trait>
typename RegionCFDataBase<Trait>::Data & RegionCFDataBase<Trait>::getDataMut()
{
    return data;
}

template struct RegionCFDataBase<RegionWriteCFDataTrait>;
template struct RegionCFDataBase<RegionDefaultCFDataTrait>;
template struct RegionCFDataBase<RegionLockCFDataTrait>;


namespace RecordKVFormat
{

// https://github.com/tikv/tikv/blob/master/components/txn_types/src/lock.rs
inline void decodeLockCfValue(DecodedLockCFValue & res)
{
    const TiKVValue & value = *res.val;
    const char * data = value.data();
    size_t len = value.dataSize();

    kvrpcpb::Op lock_type = kvrpcpb::Op_MIN;
    switch (readUInt8(data, len))
    {
        case LockType::Put:
            lock_type = kvrpcpb::Op::Put;
            break;
        case LockType::Delete:
            lock_type = kvrpcpb::Op::Del;
            break;
        case LockType::Lock:
            lock_type = kvrpcpb::Op::Lock;
            break;
        case LockType::Pessimistic:
            lock_type = kvrpcpb::Op::PessimisticLock;
            break;
    }
    res.lock_type = lock_type;
    res.primary_lock = readVarString<std::string_view>(data, len);
    res.lock_version = readVarUInt(data, len);

    if (len > 0)
    {
        res.lock_ttl = readVarUInt(data, len);
        while (len > 0)
        {
            char flag = readUInt8(data, len);
            switch (flag)
            {
                case SHORT_VALUE_PREFIX:
                {
                    size_t str_len = readUInt8(data, len);
                    if (len < str_len)
                        throw Exception("content len shorter than short value len", ErrorCodes::LOGICAL_ERROR);
                    // no need short value
                    readRawString<nullptr_t>(data, len, str_len);
                    break;
                };
                case MIN_COMMIT_TS_PREFIX:
                {
                    res.min_commit_ts = readUInt64(data, len);
                    break;
                }
                case FOR_UPDATE_TS_PREFIX:
                {
                    res.lock_for_update_ts = readUInt64(data, len);
                    break;
                }
                case TXN_SIZE_PREFIX:
                {
                    res.txn_size = readUInt64(data, len);
                    break;
                }
                case ASYNC_COMMIT_PREFIX:
                {
                    res.use_async_commit = true;
                    auto start = data;
                    UInt64 cnt = readVarUInt(data, len);
                    for (UInt64 i = 0; i < cnt; ++i)
                    {
                        readVarString<nullptr_t>(data, len);
                    }
                    auto end = data;
                    res.secondaries = {start, static_cast<size_t>(end - start)};
                    break;
                }
                case ROLLBACK_TS_PREFIX:
                {
                    UInt64 cnt = readVarUInt(data, len);
                    for (UInt64 i = 0; i < cnt; ++i)
                    {
                        readUInt64(data, len);
                    }
                    break;
                }
                default:
                {
                    std::string msg = std::string("invalid flag ") + flag + " in lock value " + value.toDebugString();
                    throw Exception(msg, ErrorCodes::LOGICAL_ERROR);
                }
            }
        }
    }
    if (len != 0)
        throw Exception("invalid lock value " + value.toDebugString(), ErrorCodes::LOGICAL_ERROR);
}

DecodedLockCFValue::DecodedLockCFValue(std::shared_ptr<const TiKVKey> key_, std::shared_ptr<const TiKVValue> val_)
    : key(std::move(key_)), val(std::move(val_))
{
    decodeLockCfValue(*this);
}

void DecodedLockCFValue::intoLockInfo(kvrpcpb::LockInfo & res) const
{
    res.set_lock_type(lock_type);
    res.set_primary_lock(primary_lock.data(), primary_lock.size());
    res.set_lock_version(lock_version);
    res.set_lock_ttl(lock_ttl);
    res.set_min_commit_ts(min_commit_ts);
    res.set_lock_for_update_ts(lock_for_update_ts);
    res.set_txn_size(txn_size);
    res.set_use_async_commit(use_async_commit);
    res.set_key(decodeTiKVKey(*key));

    if (use_async_commit)
    {
        auto data = secondaries.data();
        auto len = secondaries.size();
        UInt64 cnt = readVarUInt(data, len);
        for (UInt64 i = 0; i < cnt; ++i)
        {
            res.add_secondaries(readVarString<std::string>(data, len));
        }
    }
}

std::unique_ptr<kvrpcpb::LockInfo> DecodedLockCFValue::intoLockInfo() const
{
    auto res = std::make_unique<kvrpcpb::LockInfo>();
    intoLockInfo(*res);
    return res;
}

} // namespace RecordKVFormat
} // namespace DB
