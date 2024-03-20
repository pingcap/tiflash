// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Storages/KVStore/TiKVHelpers/DecodedLockCFValue.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>

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
    using DecodedWriteCFValue = RecordKVFormat::InnerDecodedWriteCFValue;
    using Key = std::pair<RawTiDBPK, Timestamp>;
    using Value = std::tuple<std::shared_ptr<const TiKVKey>, std::shared_ptr<const TiKVValue>, DecodedWriteCFValue>;
    using Map = std::map<Key, Value>;

    static Key genKey(const TiKVKey & key) {
        DecodedTiKVKey raw_key = RecordKVFormat::decodeTiKVKey(key);
        RawTiDBPK tidb_pk = RecordKVFormat::getRawTiDBPK(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);
        return Key(std::move(tidb_pk), ts);
    }

    static std::optional<Map::value_type> genKVPair(TiKVKey && key, const DecodedTiKVKey & raw_key, TiKVValue && value)
    {
        auto decoded_val = RecordKVFormat::decodeWriteCfValue(value);
        if (!decoded_val)
            return std::nullopt;

        RawTiDBPK tidb_pk = RecordKVFormat::getRawTiDBPK(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);
        return Map::value_type(
            Key(std::move(tidb_pk), ts),
            Value(
                std::make_shared<const TiKVKey>(std::move(key)),
                std::make_shared<const TiKVValue>(std::move(value)),
                std::move(*decoded_val)));
    }

    static const std::shared_ptr<const TiKVValue> & getRecordRawValuePtr(const Value & value)
    {
        return std::get<2>(value).short_value;
    }

    static UInt8 getWriteType(const Value & value) { return std::get<2>(value).write_type; }
};


struct RegionDefaultCFDataTrait
{
    using Key = std::pair<RawTiDBPK, Timestamp>;
    using Value = std::tuple<std::shared_ptr<const TiKVKey>, std::shared_ptr<const TiKVValue>>;
    using Map = std::map<Key, Value>;

    static Key genKey(const TiKVKey & key) {
        DecodedTiKVKey raw_key = RecordKVFormat::decodeTiKVKey(key);
        RawTiDBPK tidb_pk = RecordKVFormat::getRawTiDBPK(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);
        return Key(std::move(tidb_pk), ts);
    }

    static std::optional<Map::value_type> genKVPair(TiKVKey && key, const DecodedTiKVKey & raw_key, TiKVValue && value)
    {
        RawTiDBPK tidb_pk = RecordKVFormat::getRawTiDBPK(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);
        return Map::value_type(
            Key(std::move(tidb_pk), ts),
            Value(
                std::make_shared<const TiKVKey>(std::move(key)),
                std::make_shared<const TiKVValue>(std::move(value))));
    }

    static std::shared_ptr<const TiKVValue> getTiKVValue(const Map::const_iterator & it)
    {
        return std::get<1>(it->second);
    }
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
    using Value = std::tuple<
        std::shared_ptr<const TiKVKey>,
        std::shared_ptr<const TiKVValue>,
        std::shared_ptr<const DecodedLockCFValue>>;
    using Map = std::unordered_map<Key, Value, Key::Hash>;

    static Key genKey(const TiKVKey & key_) {
        auto key = std::make_shared<const TiKVKey>(TiKVKey::copyFrom(key_));
        return {key, std::string_view(key->data(), key->dataSize())};
    }

    static Map::value_type genKVPair(TiKVKey && key_, TiKVValue && value_)
    {
        auto key = std::make_shared<const TiKVKey>(std::move(key_));
        auto value = std::make_shared<const TiKVValue>(std::move(value_));
        return {
            {key, std::string_view(key->data(), key->dataSize())},
            Value{key, value, std::make_shared<const DecodedLockCFValue>(key, value)}};
    }
};

} // namespace DB
