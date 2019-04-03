#pragma once

#include <iostream>
#include <map>

#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/RegionMeta.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

enum ColumnFamilyType
{
    Write,
    Default,
    Lock,
};

struct RegionWriteCFDataTrait
{
    using DecodedWriteCFValue = RecordKVFormat::DecodedWriteCFValue;
    using Key = std::tuple<HandleID, Timestamp>;
    using Value = std::tuple<TiKVKey, TiKVValue, DecodedWriteCFValue>;
    std::pair<Key, Value> genKVPair(const TiKVKey & key, const String & raw_key, const TiKVValue & value)
    {
        HandleID handle_id = RecordKVFormat::getHandle(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);
        return {Key{handle_id, ts}, Value{key, value, RecordKVFormat::decodeWriteCfValue(value)}};
    }
};

struct RegionDefaultCFDataTrait
{
    using Key = std::tuple<HandleID, Timestamp>;
    using Value = std::tuple<TiKVKey, TiKVValue>;
    std::pair<Key, Value> genKVPair(const TiKVKey & key, const String & raw_key, const TiKVValue & value)
    {
        HandleID handle_id = RecordKVFormat::getHandle(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);
        return {Key{handle_id, ts}, Value{key, value}};
    }
};

struct RegionLockCFDataTrait
{
    using DecodedLockCFValue = RecordKVFormat::DecodedLockCFValue;
    using Key = std::tuple<HandleID, Timestamp>;
    using Value = std::tuple<TiKVKey, TiKVValue, DecodedLockCFValue>;
    std::pair<Key, Value> genKVPair(const TiKVKey & key, const String & raw_key, const TiKVValue & value)
    {
        HandleID handle_id = RecordKVFormat::getHandle(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);
        return {Key{handle_id, ts}, Value{key, value, RecordKVFormat::decodeLockCfValue(value)}};
    }
};

template <typename Trait>
struct RegionCFDataBase
{
    using Key = typename Trait::Key;
    using Value = typename Trait::Value;
    using Map = typename std::map<Key, Value>;

    TableID insert(const TiKVKey & key, const TiKVValue & value)
    {
        const String & raw_key = RecordKVFormat::decodeTiKVKey(key);
        return insert(key, value, raw_key);
    }

    TableID insert(const TiKVKey & key, const TiKVValue & value, const String & raw_key)
    {
        TableID table_id = RecordKVFormat::getTableId(raw_key);
        auto & map = data[table_id];
        auto [it, ok] = map.insert(Trait::genKVPair(key, raw_key, value));
        std::ignore = it;
        if (!ok)
            throw Exception(" found existing key [" + key.toString() + "]", ErrorCodes::LOGICAL_ERROR);
        return table_id;
    }

    static size_t calcTiKVKeyValueSize(const Value & value)
    {
        const auto & tikv_key = std::get<0>(value);
        const auto & tikv_val = std::get<1>(value);
        return tikv_key.dataSize() + tikv_val.dataSize();
    }

    size_t remove(TableID table_id, const Key & key)
    {
        auto & map = data[table_id];

        if (auto it = map.find(key); it != map.end())
        {
            const Value & value = it->second;
            size_t size = calcTiKVKeyValueSize(value);
            map.erase(it);
            return size;
        }
        else
        {
            const auto & [handle_id, ts] = key;
            auto tikv_key = RecordKVFormat::appendTs(RecordKVFormat::genKey(table_id, handle_id), ts);
            throw Exception(" key not found [" + tikv_key.toString() + "]", ErrorCodes::LOGICAL_ERROR);

            return 0;
        }
    }

    RegionCFDataBase(){}
    RegionCFDataBase(RegionCFDataBase && region): data(std::move(region.data)) {}
    RegionCFDataBase & operator = (RegionCFDataBase && region)
    {
        data = std::move(region.data);
        return *this;
    }

    std::unordered_map<TableID, Map> data;
};

template <typename Map>
bool CFDataCmp(const Map & a, const Map & b)
{
    if (a.size() != b.size())
        return false;
    for (const auto & [key, value] : a)
    {
        if (auto it = b.find(key); it != b.end())
        {
            if (std::get<0>(value) != std::get<0>(it->second) || std::get<1>(value) != std::get<1>(it->second))
                return false;
        }
        else
            return false;
    }
    return true;
}

struct RegionWriteCFData
{
    using DecodedWriteCFValue = RecordKVFormat::DecodedWriteCFValue;
    using Key = std::tuple<TableID, HandleID, Timestamp>;
    using Value = std::tuple<TiKVKey, TiKVValue, DecodedWriteCFValue>;
    using WriteCFDataMap = std::map<Key, Value>;

    TableID insert(TableID table_id, HandleID handle_id, Timestamp ts, const TiKVKey & key, const TiKVValue & value)
    {
        auto [it, ok] = map.try_emplace({table_id, handle_id, ts}, Value{key, value, RecordKVFormat::decodeWriteCfValue(value)});
        std::ignore = it;
        if (!ok)
            throw Exception(" found existing key [" + key.toString() + "]", ErrorCodes::LOGICAL_ERROR);

        return table_id;
    }

    TableID insert(const TiKVKey & key, const TiKVValue & value)
    {
        const String & raw_key = RecordKVFormat::decodeTiKVKey(key);
        return insert(key, value, raw_key);
    }

    TableID insert(const TiKVKey & key, const TiKVValue & value, const String & raw_key)
    {
        TableID table_id = RecordKVFormat::getTableId(raw_key);
        HandleID handle_id = RecordKVFormat::getHandle(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);
        return insert(table_id, handle_id, ts, key, value);
    }

    size_t remove(TableID table_id, HandleID handle_id, Timestamp ts)
    {
        if (auto it = map.find({table_id, handle_id, ts}); it != map.end())
        {
            const auto & [key, value, _] = it->second;
            std::ignore = _;
            size_t size = key.dataSize() + value.dataSize();
            map.erase(it);
            return size;
        }
        else
        {
            auto key = RecordKVFormat::appendTs(RecordKVFormat::genKey(table_id, handle_id), ts);
            throw Exception(" key not found [" + key.toString() + "]", ErrorCodes::LOGICAL_ERROR);

            return 0;
        }
    }

    friend bool operator==(const RegionWriteCFData & cf1, const RegionWriteCFData & cf2)
    {
        return CFDataCmp(cf1.map, cf2.map);
    }

    RegionWriteCFData(){}
    RegionWriteCFData(RegionWriteCFData && data): map(std::move(data.map)) {}
    RegionWriteCFData & operator = (RegionWriteCFData && data)
    {
        map = std::move(data.map);
        return *this;
    }

    WriteCFDataMap map;
};

struct RegionDefaultCFData
{
    using Key = std::tuple<TableID, HandleID, Timestamp>;
    using Value = std::tuple<TiKVKey, TiKVValue>;
    using DefaultCFDataMap = std::map<Key, Value>;

    TableID insert(TableID table_id, HandleID handle_id, Timestamp ts, const TiKVKey & key, const TiKVValue & value)
    {
        auto [it, ok] = map.try_emplace({table_id, handle_id, ts}, Value{key, value});
        std::ignore = it;
        if (!ok)
            throw Exception(" found existing key [" + key.toString() + "]", ErrorCodes::LOGICAL_ERROR);

        return table_id;
    }

    TableID insert(const TiKVKey & key, const TiKVValue & value)
    {
        const String & raw_key = RecordKVFormat::decodeTiKVKey(key);
        return insert(key, value, raw_key);
    }

    TableID insert(const TiKVKey & key, const TiKVValue & value, const String & raw_key)
    {
        TableID table_id = RecordKVFormat::getTableId(raw_key);
        HandleID handle_id = RecordKVFormat::getHandle(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);
        return insert(table_id, handle_id, ts, key, value);
    }

    size_t remove(TableID table_id, HandleID handle_id, Timestamp ts)
    {
        if (auto it = map.find({table_id, handle_id, ts}); it != map.end())
        {
            const auto & [key, value] = it->second;
            size_t size = key.dataSize() + value.dataSize();
            map.erase(it);
            return size;
        }
        else
        {
            auto key = RecordKVFormat::appendTs(RecordKVFormat::genKey(table_id, handle_id), ts);
            throw Exception(" key not found [" + key.toString() + "]", ErrorCodes::LOGICAL_ERROR);

            return 0;
        }
    }

    friend bool operator==(const RegionDefaultCFData & cf1, const RegionDefaultCFData & cf2)
    {
        return CFDataCmp(cf1.map, cf2.map);
    }

    RegionDefaultCFData(){}
    RegionDefaultCFData(RegionDefaultCFData && data): map(std::move(data.map)) {}
    RegionDefaultCFData & operator = (RegionDefaultCFData && data)
    {
        map = std::move(data.map);
        return *this;
    }

    DefaultCFDataMap map;
};

struct RegionLockCFData
{
    using DecodedLockCFValue = std::tuple<UInt8, String, UInt64, UInt64, std::unique_ptr<String>>;
    using Key = std::tuple<TableID, HandleID>;
    using Value = std::tuple<TiKVKey, TiKVValue, DecodedLockCFValue>;
    using LockCFDataMap = std::map<Key, Value>;

    TableID insert(TableID table_id, HandleID handle_id, const TiKVKey & key, const TiKVValue & value)
    {
        auto [it, ok] = map.try_emplace({table_id, handle_id}, Value{key, value, RecordKVFormat::decodeLockCfValue(value)});
        std::ignore = it;

        if (!ok)
            throw Exception(" found existing key [" + key.toString() + "]", ErrorCodes::LOGICAL_ERROR);

        return table_id;
    }

    TableID insert(const TiKVKey & key, const TiKVValue & value)
    {
        const String & raw_key = RecordKVFormat::decodeTiKVKey(key);
        return insert(key, value, raw_key);
    }

    TableID insert(const TiKVKey & key, const TiKVValue & value, const String & raw_key)
    {
        TableID table_id = RecordKVFormat::getTableId(raw_key);
        HandleID handle_id = RecordKVFormat::getHandle(raw_key);
        return insert(table_id, handle_id, key, value);
    }

    void remove(TableID table_id, HandleID handle_id)
    {
        if (auto it = map.find({table_id, handle_id}); it != map.end())
        {
            map.erase(it);
        }
        else
        {
            auto key = RecordKVFormat::genKey(table_id, handle_id);
            throw Exception(" key not found [" + key.toString() + "]", ErrorCodes::LOGICAL_ERROR);
        }
    }

    friend bool operator==(const RegionLockCFData & cf1, const RegionLockCFData & cf2)
    {
        return CFDataCmp(cf1.map, cf2.map);
    }

    RegionLockCFData(){}
    RegionLockCFData(RegionLockCFData && data): map(std::move(data.map)) {}
    RegionLockCFData & operator = (RegionLockCFData && data)
    {
        map = std::move(data.map);
        return *this;
    }

    LockCFDataMap map;
};

class RegionData
{
public:
    // In both lock_cf and write_cf.
    enum CFModifyFlag : UInt8
    {
        PutFlag = 'P',
        DelFlag = 'D',
        // useless for TiFLASH
        /*
        LockFlag = 'L',
        // In write_cf, only raft leader will use RollbackFlag in txn mode. Learner should ignore it.
        RollbackFlag = 'R',
        */
    };

    /// A quick-and-dirty copy of LockInfo structure in kvproto.
    /// Used to transmit to client using non-ProtoBuf protocol.
    struct LockInfo
    {
        std::string primary_lock;
        UInt64 lock_version;
        std::string key;
        UInt64 lock_ttl;
    };

    using ReadInfo = std::tuple<UInt64, UInt8, UInt64, TiKVValue>;
    using WriteCFIter = RegionWriteCFData::WriteCFDataMap::iterator;
    using ConstWriteCFIter = RegionWriteCFData::WriteCFDataMap::const_iterator;

    using LockInfoPtr = std::unique_ptr<LockInfo>;

    TableID insert(ColumnFamilyType cf, const TiKVKey & key, const String & raw_key, const TiKVValue & value)
    {
        switch(cf)
        {
            case Write:
            {
                cf_data_size += key.dataSize() + value.dataSize();
                return write_cf.insert(key, value, raw_key);
            }
            case Default:
            {
                cf_data_size += key.dataSize() + value.dataSize();
                return default_cf.insert(key, value, raw_key);
            }
            case Lock:
            {
                return lock_cf.insert(key, value, raw_key);
            }
            default:
                throw Exception(" should not happen", ErrorCodes::LOGICAL_ERROR);
        }
    }

    TableID remove(ColumnFamilyType cf, const TiKVKey & key, const String & raw_key)
    {
        switch(cf)
        {
            case Write:
            {
                TableID table_id = RecordKVFormat::getTableId(raw_key);
                HandleID handle_id = RecordKVFormat::getHandle(raw_key);
                Timestamp ts = RecordKVFormat::getTs(key);
                cf_data_size -= write_cf.remove(table_id, handle_id, ts);
                return table_id;
            }
            case Default:
            {
                TableID table_id = RecordKVFormat::getTableId(raw_key);
                HandleID handle_id = RecordKVFormat::getHandle(raw_key);
                Timestamp ts = RecordKVFormat::getTs(key);
                cf_data_size -= default_cf.remove(table_id, handle_id, ts);
                return table_id;
            }
            case Lock:
            {
                TableID table_id = RecordKVFormat::getTableId(raw_key);
                HandleID handle_id = RecordKVFormat::getHandle(raw_key);
                lock_cf.remove(table_id, handle_id);
                return table_id;
            }
            default:
                throw Exception(" should not happen", ErrorCodes::LOGICAL_ERROR);
        }
    }

    WriteCFIter removeDataByWriteIt(const WriteCFIter & write_it)
    {
        const auto & [key, value, decoded_val] = write_it->second;
        const auto & [table, handle, ts] = write_it->first;
        std::ignore = ts;
        const auto & [write_type, prewrite_ts, short_str] = decoded_val;

        auto data_it = default_cf.map.find({table, handle, prewrite_ts});

        if (write_type == PutFlag && !short_str)
        {
            if (unlikely(data_it == default_cf.map.end()))
                throw Exception(" key [" + key.toString() + "] not found in data cf when removing", ErrorCodes::LOGICAL_ERROR);

            const auto & [key, value] = data_it->second;

            cf_data_size -= key.dataSize() + value.dataSize();
            default_cf.map.erase(data_it);
        }

        cf_data_size -= key.dataSize() + value.dataSize();

        return write_cf.map.erase(write_it);
    }

    ReadInfo readDataByWriteIt(const ConstWriteCFIter & write_it, std::vector<RegionWriteCFData::Key> * keys)
    {
        const auto & [key, value, decoded_val] = write_it->second;
        const auto & [table, handle, ts] = write_it->first;

        std::ignore = value;

        if (keys)
            keys->push_back(write_it->first);

        const auto & [write_type, prewrite_ts, short_value] = decoded_val;

        if (write_type != PutFlag)
            return std::make_tuple(handle, write_type, ts, TiKVValue());

        if (short_value)
            return std::make_tuple(handle, write_type, ts, TiKVValue(*short_value));

        auto data_it = default_cf.map.find({table, handle, prewrite_ts});
        if (unlikely(data_it == default_cf.map.end()))
            throw Exception("key [" + key.toString() + "] not found in data cf", ErrorCodes::LOGICAL_ERROR);

        return std::make_tuple(handle, write_type, ts, std::get<1>(data_it->second));
    }

    LockInfoPtr getLockInfo(TableID expected_table_id, Timestamp start_ts)
    {
        for (const auto & [key, value] : lock_cf.map)
        {
            const auto & [table_id, handle] = key;
            if (expected_table_id != table_id)
                continue;
            std::ignore = handle;

            const auto & [tikv_key, tikv_val, decoded_val] = value;
            const auto & [lock_type, primary, ts, ttl, data] = decoded_val;
            std::ignore = tikv_val;
            std::ignore = data;

            if (lock_type == DelFlag || ts > start_ts)
                continue;

            return std::make_unique<LockInfo>(LockInfo{primary, ts, RecordKVFormat::decodeTiKVKey(tikv_key), ttl});
        }

        return nullptr;
    }

    void splitInto(const RegionRange & range, RegionData & new_region_data)
    {
        const auto & [start_key, end_key] = range;

        for (auto it = default_cf.map.begin(); it != default_cf.map.end();)
        {
            const auto & [key, val] = it->second;

            bool ok = start_key ? key >= start_key : true;
            ok = ok && (end_key ? key < end_key : true);
            if (ok)
            {
                cf_data_size -= key.dataSize() + val.dataSize();
                new_region_data.cf_data_size += key.dataSize() + val.dataSize();

                new_region_data.default_cf.map.insert(std::move(*it));
                it = default_cf.map.erase(it);
            }
            else
                ++it;
        }

        for (auto it = write_cf.map.begin(); it != write_cf.map.end();)
        {
            const auto & [key, val, _] = it->second;
            std::ignore = _;

            bool ok = start_key ? key >= start_key : true;
            ok = ok && (end_key ? key < end_key : true);
            if (ok)
            {
                cf_data_size -= key.dataSize() + val.dataSize();
                new_region_data.cf_data_size += key.dataSize() + val.dataSize();

                new_region_data.write_cf.map.insert(std::move(*it));
                it = write_cf.map.erase(it);
            }
            else
                ++it;
        }

        for (auto it = lock_cf.map.begin(); it != lock_cf.map.end();)
        {
            const auto & [key, val, _] = it->second;
            std::ignore = val;
            std::ignore = _;

            bool ok = start_key ? key >= start_key : true;
            ok = ok && (end_key ? key < end_key : true);
            if (ok)
            {
                new_region_data.lock_cf.map.insert(std::move(*it));
                it = lock_cf.map.erase(it);
            }
            else
                ++it;
        }
    }

    size_t dataSize() const { return cf_data_size; }

    void reset(RegionData && new_region_data)
    {
        default_cf = std::move(new_region_data.default_cf);
        write_cf = std::move(new_region_data.write_cf);
        lock_cf = std::move(new_region_data.lock_cf);

        cf_data_size = new_region_data.cf_data_size.load();
    }

    size_t serialize(WriteBuffer & buf)
    {
        size_t total_size = 0;

        total_size += writeBinary2(default_cf.map.size(), buf);
        for (const auto & ele : default_cf.map)
        {
            const auto & [key, value] = ele.second;
            total_size += key.serialize(buf);
            total_size += value.serialize(buf);
        }

        total_size += writeBinary2(write_cf.map.size(), buf);
        for (const auto & ele : write_cf.map)
        {
            const auto & [key, value, _] = ele.second;
            std::ignore = _;

            total_size += key.serialize(buf);
            total_size += value.serialize(buf);
        }

        total_size += writeBinary2(lock_cf.map.size(), buf);
        for (const auto & ele : lock_cf.map)
        {
            const auto & [key, value, _] = ele.second;
            std::ignore = _;

            total_size += key.serialize(buf);
            total_size += value.serialize(buf);
        }

        return total_size;
    }

    static void deserialize(ReadBuffer & buf, RegionData & region_data)
    {
        auto size = readBinary2<size_t >(buf);
        for (size_t i = 0; i < size; ++i)
        {
            auto key = TiKVKey::deserialize(buf);
            auto value = TiKVValue::deserialize(buf);

            region_data.default_cf.insert(key, value);
            region_data.cf_data_size += key.dataSize() + value.dataSize();
        }

        size = readBinary2<size_t>(buf);
        for (size_t i = 0; i < size; ++i)
        {
            auto key = TiKVKey::deserialize(buf);
            auto value = TiKVValue::deserialize(buf);

            region_data.write_cf.insert(key, value);
            region_data.cf_data_size += key.dataSize() + value.dataSize();
        }

        size = readBinary2<size_t>(buf);
        for (size_t i = 0; i < size; ++i)
        {
            auto key = TiKVKey::deserialize(buf);
            auto value = TiKVValue::deserialize(buf);

            region_data.lock_cf.insert(key, value);
        }
    }

    friend bool operator==(const RegionData & r1, const RegionData & r2)
    {
        return r1.default_cf == r2.default_cf && r1.write_cf == r2.write_cf
            && r1.lock_cf == r2.lock_cf && r1.cf_data_size == r2.cf_data_size;
    }

    RegionData() {}

    RegionData(RegionData && data):write_cf(std::move(data.write_cf)),default_cf(std::move(data.default_cf)),lock_cf(std::move(data.lock_cf)) {}

public:
    RegionWriteCFData write_cf;
    RegionDefaultCFData default_cf;
    RegionLockCFData lock_cf;

    // Size of data cf & write cf, without lock cf.
    std::atomic<size_t> cf_data_size = 0;
};

} // namespace DB
