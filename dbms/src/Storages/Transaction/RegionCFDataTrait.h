#pragma once

#include <map>

#include <Storages/Transaction/TiKVRecordFormat.h>

namespace DB
{

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

} // namespace DB
