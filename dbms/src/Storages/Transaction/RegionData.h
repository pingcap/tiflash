#pragma once

#include <iostream>
#include <map>
#include <list>

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
    using Map = std::map<Key, Value>;
    using Keys = std::list<Key>;

    static std::pair<Key, Value> genKVPair(const TiKVKey & key, const String & raw_key, const TiKVValue & value)
    {
        HandleID handle_id = RecordKVFormat::getHandle(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);
        return {Key{handle_id, ts}, Value{key, value, RecordKVFormat::decodeWriteCfValue(value)}};
    }
    static TiKVKey genTiKVKey(const TableID & table_id, const Key & key)
    {
        const auto & [handle_id, ts] = key;
        auto tikv_key = RecordKVFormat::appendTs(RecordKVFormat::genKey(table_id, handle_id), ts);
        return tikv_key;
    }
};

struct RegionDefaultCFDataTrait
{
    using Key = std::tuple<HandleID, Timestamp>;
    using Value = std::tuple<TiKVKey, TiKVValue>;
    using Map = std::map<Key, Value>;

    static std::pair<Key, Value> genKVPair(const TiKVKey & key, const String & raw_key, const TiKVValue & value)
    {
        HandleID handle_id = RecordKVFormat::getHandle(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);
        return {Key{handle_id, ts}, Value{key, value}};
    }
    static TiKVKey genTiKVKey(const TableID & table_id, const Key & key)
    {
        const auto & [handle_id, ts] = key;
        auto tikv_key = RecordKVFormat::appendTs(RecordKVFormat::genKey(table_id, handle_id), ts);
        return tikv_key;
    }
};

struct RegionLockCFDataTrait
{
    using DecodedLockCFValue = RecordKVFormat::DecodedLockCFValue;
    using Key = HandleID;
    using Value = std::tuple<TiKVKey, TiKVValue, DecodedLockCFValue>;
    using Map = std::map<Key, Value>;

    static std::pair<Key, Value> genKVPair(const TiKVKey & key, const String & raw_key, const TiKVValue & value)
    {
        HandleID handle_id = RecordKVFormat::getHandle(raw_key);
        return {handle_id, Value{key, value, RecordKVFormat::decodeLockCfValue(value)}};
    }
    static TiKVKey genTiKVKey(const TableID & table_id, const Key & key)
    {
        auto tikv_key = RecordKVFormat::genKey(table_id, key);
        return tikv_key;
    }
};

template <typename Trait>
struct RegionCFDataBase
{
    using Key = typename Trait::Key;
    using Value = typename Trait::Value;
    using Map = typename Trait::Map;
    using Data = std::unordered_map<TableID, Map>;

    static const TiKVKey & getTiKVKey(const Value & val)
    {
        return std::get<0>(val);
    }

    static const TiKVValue & getTiKVValue(const Value & val)
    {
        return std::get<1>(val);
    }

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
        return calcTiKVKeyValueSize(getTiKVKey(value), getTiKVValue(value));
    }

    static size_t calcTiKVKeyValueSize(const TiKVKey & key, const TiKVValue & value)
    {
        if constexpr (std::is_same<Trait, RegionLockCFDataTrait>::value)
            return 0;
        else
            return key.dataSize() + value.dataSize();
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
            auto tikv_key = Trait::genTiKVKey(table_id, key);
            throw Exception(" key not found [" + tikv_key.toString() + "]", ErrorCodes::LOGICAL_ERROR);

            return 0;
        }
    }

    static bool cmp(const Map & a, const Map & b)
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

    bool operator == (const RegionCFDataBase & cf) const
    {
        if (getSize() != cf.getSize())
            return false;

        const auto & cf_data = cf.data;
        for (const auto & [table_id, map] : data)
        {
            if (map.empty())
                continue;

            if (auto it = cf_data.find(table_id); it != cf_data.end())
            {
                if (!cmp(map, it->second))
                    return false;
            }
            else
                return false;
        }
        return true;
    }

    size_t getSize() const
    {
        size_t size = 0;
        for (auto data_it = data.begin(); data_it != data.end(); ++data_it)
            size += data_it->second.size();
        return size;
    }

    RegionCFDataBase(){}
    RegionCFDataBase(RegionCFDataBase && region): data(std::move(region.data)) {}
    RegionCFDataBase & operator = (RegionCFDataBase && region)
    {
        data = std::move(region.data);
        return *this;
    }

    size_t splitInto(const RegionRange & range, RegionCFDataBase & new_region_data)
    {
        const auto & [start_key, end_key] = range;
        size_t size_changed = 0;

        for (auto data_it = data.begin(); data_it != data.end(); )
        {
            const auto & table_id = data_it->first;
            auto & ori_map = data_it->second;
            if (ori_map.empty())
            {
                data_it = data.erase(data_it);
                continue;
            }

            auto & tar_map = new_region_data.data[table_id];

            for (auto it = ori_map.begin(); it != ori_map.end();)
            {
                const auto & key = getTiKVKey(it->second);

                bool ok = start_key ? key >= start_key : true;
                ok = ok && (end_key ? key < end_key : true);
                if (ok)
                {
                    size_changed += calcTiKVKeyValueSize(it->second);
                    tar_map.insert(std::move(*it));
                    it = ori_map.erase(it);
                }
                else
                    ++it;
            }

            ++data_it;
        }
        return size_changed;
    }

    size_t serialize(WriteBuffer & buf) const
    {
        size_t total_size = 0;

        size_t size = getSize();

        total_size += writeBinary2(size, buf);

        for (const auto & [table_id, map] : data)
        {
            std::ignore = table_id;
            for (const auto & ele : map)
            {
                const auto & key = getTiKVKey(ele.second);
                const auto & value = getTiKVValue(ele.second);
                total_size += key.serialize(buf);
                total_size += value.serialize(buf);
            }
        }

        return total_size;
    }

    static size_t deserialize(ReadBuffer & buf, RegionCFDataBase & new_region_data)
    {
        size_t size = readBinary2<size_t>(buf);
        size_t cf_data_size = 0;
        for (size_t i = 0; i < size; ++i)
        {
            auto key = TiKVKey::deserialize(buf);
            auto value = TiKVValue::deserialize(buf);

            new_region_data.insert(key, value);
            cf_data_size += calcTiKVKeyValueSize(key, value);
        }
        return cf_data_size;
    }

    const auto & getData() const
    {
        return data;
    }

    auto & getDataMut()
    {
        return data;
    }

    TableIDSet getAllRecordTableID() const
    {
        TableIDSet tables;
        for (const auto & [table_id, map] : data)
        {
            if (map.empty())
                continue;
            tables.insert(table_id);
        }
        return tables;
    }

private:
    Data data;
};

using RegionWriteCFData = RegionCFDataBase<RegionWriteCFDataTrait>;
using RegionDefaultCFData = RegionCFDataBase<RegionDefaultCFDataTrait>;
using RegionLockCFData = RegionCFDataBase<RegionLockCFDataTrait>;

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
    using WriteCFIter = RegionWriteCFData::Map::iterator;
    using ConstWriteCFIter = RegionWriteCFData::Map::const_iterator;

    using LockInfoPtr = std::unique_ptr<LockInfo>;

    TableID insert(ColumnFamilyType cf, const TiKVKey & key, const String & raw_key, const TiKVValue & value)
    {
        switch(cf)
        {
            case Write:
            {
                auto table_id = write_cf.insert(key, value, raw_key);
                cf_data_size += key.dataSize() + value.dataSize();
                return table_id;
            }
            case Default:
            {
                auto table_id = default_cf.insert(key, value, raw_key);
                cf_data_size += key.dataSize() + value.dataSize();
                return table_id;
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
                cf_data_size -= write_cf.remove(table_id, RegionWriteCFData::Key{handle_id, ts});
                return table_id;
            }
            case Default:
            {
                TableID table_id = RecordKVFormat::getTableId(raw_key);
                HandleID handle_id = RecordKVFormat::getHandle(raw_key);
                Timestamp ts = RecordKVFormat::getTs(key);
                cf_data_size -= default_cf.remove(table_id, RegionDefaultCFData::Key{handle_id, ts});
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

    WriteCFIter removeDataByWriteIt(const TableID & table_id, const WriteCFIter & write_it)
    {
        const auto & [key, value, decoded_val] = write_it->second;
        const auto & [handle, ts] = write_it->first;
        const auto & [write_type, prewrite_ts, short_str] = decoded_val;

        std::ignore = ts;
        std::ignore = value;

        if (write_type == PutFlag && !short_str)
        {
            auto & map = default_cf.getDataMut()[table_id];

            if (auto data_it = map.find({handle, prewrite_ts}); data_it != map.end())
            {
                cf_data_size -= RegionDefaultCFData::calcTiKVKeyValueSize(data_it->second);
                map.erase(data_it);
            }
            else
                throw Exception(" key [" + key.toString() + "] not found in data cf when removing", ErrorCodes::LOGICAL_ERROR);
        }

        cf_data_size -= RegionWriteCFData::calcTiKVKeyValueSize(write_it->second);

        return write_cf.getDataMut()[table_id].erase(write_it);
    }

    ReadInfo readDataByWriteIt(const TableID & table_id, const ConstWriteCFIter & write_it, RegionWriteCFDataTrait::Keys * keys) const
    {
        const auto & [key, value, decoded_val] = write_it->second;
        const auto & [handle, ts] = write_it->first;

        std::ignore = value;

        if (keys)
            keys->push_back(write_it->first);

        const auto & [write_type, prewrite_ts, short_value] = decoded_val;

        if (write_type != PutFlag)
            return std::make_tuple(handle, write_type, ts, TiKVValue());

        if (short_value)
            return std::make_tuple(handle, write_type, ts, TiKVValue(*short_value));

        if (auto map_it = default_cf.getData().find(table_id); map_it != default_cf.getData().end())
        {
            const auto & map = map_it->second;
            if (auto data_it = map.find({handle, prewrite_ts}); data_it != map.end())
                return std::make_tuple(handle, write_type, ts, RegionDefaultCFData::getTiKVValue(data_it->second));
            else
                throw Exception(" key [" + key.toString() + "] not found in data cf", ErrorCodes::LOGICAL_ERROR);
        }
        else
            throw Exception(" table [" + toString(table_id) + "] not found in data cf", ErrorCodes::LOGICAL_ERROR);
    }

    LockInfoPtr getLockInfo(TableID expected_table_id, Timestamp start_ts) const
    {
        if (auto it = lock_cf.getData().find(expected_table_id); it != lock_cf.getData().end())
        {
            for (const auto & [handle, value] : it->second)
            {
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
        else
            return nullptr;
    }

    void splitInto(const RegionRange & range, RegionData & new_region_data)
    {
        size_t size_changed = 0;
        size_changed += default_cf.splitInto(range, new_region_data.default_cf);
        size_changed += write_cf.splitInto(range, new_region_data.write_cf);
        size_changed += lock_cf.splitInto(range, new_region_data.lock_cf);
        cf_data_size -= size_changed;
        new_region_data.cf_data_size += size_changed;
    }

    size_t dataSize() const { return cf_data_size; }

    void reset(RegionData && new_region_data)
    {
        default_cf = std::move(new_region_data.default_cf);
        write_cf = std::move(new_region_data.write_cf);
        lock_cf = std::move(new_region_data.lock_cf);

        cf_data_size = new_region_data.cf_data_size.load();
    }

    size_t serialize(WriteBuffer & buf) const
    {
        size_t total_size = 0;

        total_size += default_cf.serialize(buf);
        total_size += write_cf.serialize(buf);
        total_size += lock_cf.serialize(buf);

        return total_size;
    }

    static void deserialize(ReadBuffer & buf, RegionData & region_data)
    {
        size_t total_size = 0;
        total_size += RegionDefaultCFData::deserialize(buf, region_data.default_cf);
        total_size += RegionWriteCFData::deserialize(buf, region_data.write_cf);
        total_size += RegionLockCFData::deserialize(buf, region_data.lock_cf);

        region_data.cf_data_size += total_size;
    }

    friend bool operator==(const RegionData & r1, const RegionData & r2)
    {
        return r1.default_cf == r2.default_cf && r1.write_cf == r2.write_cf
            && r1.lock_cf == r2.lock_cf && r1.cf_data_size == r2.cf_data_size;
    }

    RegionWriteCFData & writeCFMute()
    {
        return write_cf;
    }

    const RegionWriteCFData & writeCF() const
    {
        return write_cf;
    }

    TableIDSet getCommittedRecordTableID() const
    {
        return writeCF().getAllRecordTableID();
    }

    RegionData() {}

    RegionData(RegionData && data):write_cf(std::move(data.write_cf)),default_cf(std::move(data.default_cf)),lock_cf(std::move(data.lock_cf)) {}

private:
    RegionWriteCFData write_cf;
    RegionDefaultCFData default_cf;
    RegionLockCFData lock_cf;

    // Size of data cf & write cf, without lock cf.
    std::atomic<size_t> cf_data_size = 0;
};

} // namespace DB
