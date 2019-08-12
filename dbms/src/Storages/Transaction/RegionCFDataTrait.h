#pragma once

#include <map>

#include <Storages/Transaction/TiKVRecordFormat.h>

namespace DB
{

struct RegionWriteCFDataTrait
{
    using DecodedWriteCFValue = RecordKVFormat::DecodedWriteCFValue;
    using Key = std::tuple<HandleID, Timestamp>;
    using Value = std::tuple<std::shared_ptr<const TiKVKey>, std::shared_ptr<const TiKVValue>, DecodedWriteCFValue>;
    using Map = std::map<Key, Value>;

    static std::pair<Key, Value> genKVPair(TiKVKey && key, const String & raw_key, TiKVValue && value)
    {
        HandleID handle_id = RecordKVFormat::getHandle(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);
        auto decoded_val = RecordKVFormat::decodeWriteCfValue(value);
        return {Key{handle_id, ts},
            Value{std::make_shared<const TiKVKey>(std::move(key)), std::make_shared<const TiKVValue>(std::move(value)),
                std::move(decoded_val)}};
    }

    static UInt8 getWriteType(const Value & value) { return std::get<0>(std::get<2>(value)); }
};


struct RegionDefaultCFDataTrait
{
    using Key = std::tuple<HandleID, Timestamp>;
    using Value = std::tuple<std::shared_ptr<const TiKVKey>, std::shared_ptr<const TiKVValue>>;
    using Map = std::map<Key, Value>;

    static std::pair<Key, Value> genKVPair(TiKVKey && key, const String & raw_key, TiKVValue && value)
    {
        HandleID handle_id = RecordKVFormat::getHandle(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);
        return {Key{handle_id, ts},
            Value{std::make_shared<const TiKVKey>(std::move(key)), std::make_shared<const TiKVValue>(std::move(value))}};
    }
};

struct RegionLockCFDataTrait
{
    using DecodedLockCFValue = RecordKVFormat::DecodedLockCFValue;
    using Key = HandleID;
    using Value = std::tuple<std::shared_ptr<const TiKVKey>, std::shared_ptr<const TiKVValue>, DecodedLockCFValue>;
    using Map = std::map<Key, Value>;

    static std::pair<Key, Value> genKVPair(TiKVKey && key, const String & raw_key, TiKVValue && value)
    {
        HandleID handle_id = RecordKVFormat::getHandle(raw_key);
        auto decoded_val = RecordKVFormat::decodeLockCfValue(value);
        return {handle_id,
            Value{std::make_shared<const TiKVKey>(std::move(key)), std::make_shared<const TiKVValue>(std::move(value)),
                std::move(decoded_val)}};
    }
};

} // namespace DB
