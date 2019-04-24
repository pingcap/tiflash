#pragma once

#include <map>

#include <Storages/Transaction/TiKVRecordFormat.h>
#include <Storages/Transaction/RegionLockInfo.h>
#include <Storages/Transaction/RegionDataRead.h>

namespace DB
{

using RegionRange = std::pair<TiKVKey, TiKVKey>;

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

    using WriteCFIter = RegionWriteCFData::Map::iterator;
    using ConstWriteCFIter = RegionWriteCFData::Map::const_iterator;

    TableID insert(ColumnFamilyType cf, const TiKVKey & key, const String & raw_key, const TiKVValue & value);

    TableID removeLockCF(const TableID & table_id, const String & raw_key);

    WriteCFIter removeDataByWriteIt(const TableID & table_id, const WriteCFIter & write_it);

    RegionDataReadInfo readDataByWriteIt(const TableID & table_id, const ConstWriteCFIter & write_it) const;

    LockInfoPtr getLockInfo(TableID expected_table_id, Timestamp start_ts) const;

    void splitInto(const RegionRange & range, RegionData & new_region_data);

    size_t dataSize() const;

    void reset(RegionData && new_region_data);

    size_t serialize(WriteBuffer & buf) const;

    static void deserialize(ReadBuffer & buf, RegionData & region_data);

    friend bool operator==(const RegionData & r1, const RegionData & r2)
    {
        return r1.isEqual(r2);
    }

    bool isEqual(const RegionData & r2) const;

    RegionWriteCFData & writeCFMute();

    const RegionWriteCFData & writeCF() const;

    TableIDSet getCommittedRecordTableID() const;

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
