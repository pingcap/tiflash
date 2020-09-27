#pragma once

#include <Storages/Transaction/TiKVRecordFormat.h>

#include <map>

namespace DB
{

struct CFKeyHasher
{
    size_t operator()(const std::pair<HandleID, Timestamp> & k) const noexcept
    {
        const static Timestamp mask = std::numeric_limits<Timestamp>::max() << 40 >> 40;
        size_t res = k.first << 24 | (k.second & mask);
        return res;
    }
};

struct RegionWriteCFDataTrait
{
    using DecodedWriteCFValue = RecordKVFormat::DecodedWriteCFValue;
    using Key = std::pair<RawTiDBPK, Timestamp>;
    using Value = std::tuple<std::shared_ptr<const TiKVKey>, std::shared_ptr<const TiKVValue>, DecodedWriteCFValue>;
    using Map = std::map<Key, Value>;

    static Map::value_type genKVPair(TiKVKey && key, const DecodedTiKVKey & raw_key, TiKVValue && value)
    {
        RawTiDBPK tidb_pk = RecordKVFormat::getRawTiDBPK(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);
        auto decoded_val = RecordKVFormat::decodeWriteCfValue(value);
        return {Key{std::move(tidb_pk), ts},
            Value{std::make_shared<const TiKVKey>(std::move(key)), std::make_shared<const TiKVValue>(std::move(value)),
                std::move(decoded_val)}};
    }

    static const std::shared_ptr<const TiKVValue> & getRecordRawValuePtr(const Value & val) { return std::get<2>(std::get<2>(val)); }

    static UInt8 getWriteType(const Value & value) { return std::get<0>(std::get<2>(value)); }
};


struct RegionDefaultCFDataTrait
{
    using Key = std::pair<RawTiDBPK, Timestamp>;
    using Value = std::tuple<std::shared_ptr<const TiKVKey>, std::shared_ptr<const TiKVValue>>;
    using Map = std::map<Key, Value>;

    static Map::value_type genKVPair(TiKVKey && key, const DecodedTiKVKey & raw_key, TiKVValue && value)
    {
        RawTiDBPK tidb_pk = RecordKVFormat::getRawTiDBPK(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);
        return {Key{std::move(tidb_pk), ts},
            Value{std::make_shared<const TiKVKey>(std::move(key)), std::make_shared<const TiKVValue>(std::move(value))}};
    }

    static std::shared_ptr<const TiKVValue> getTiKVValue(const Map::const_iterator & it) { return std::get<1>(it->second); }
};

struct RegionLockCFDataTrait
{
    struct Key
    {
        std::shared_ptr<const TiKVKey> key;
        std::string_view view;
        struct Hash
        {
            size_t operator()(const Key & x) const { return std::hash<std::string_view>()(x.view); }
        };
        bool operator==(const Key & tar) const { return view == tar.view; }
    };
    using DecodedLockCFValue = RecordKVFormat::DecodedLockCFValue;
    using Value = std::tuple<std::shared_ptr<const TiKVKey>, std::shared_ptr<const TiKVValue>, std::shared_ptr<const DecodedLockCFValue>>;
    using Map = std::unordered_map<Key, Value, Key::Hash>;

    static Map::value_type genKVPair(TiKVKey && key_, const DecodedTiKVKey &, TiKVValue && value_)
    {
        auto key = std::make_shared<const TiKVKey>(std::move(key_));
        auto value = std::make_shared<const TiKVValue>(std::move(value_));
        return {{key, std::string_view(key->data(), key->dataSize())},
            Value{key, value, std::make_shared<const DecodedLockCFValue>(key, value)}};
    }
};

} // namespace DB
