// Copyright 2024 PingCAP, Inc.
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

#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>
#include <Storages/KVStore/MultiRaft/RegionCFDataBase.h>
#include <Storages/KVStore/MultiRaft/RegionCFDataTrait.h>
#include <Storages/KVStore/MultiRaft/RegionRangeKeys.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>

namespace DB
{

class RegionData;

struct LargeDefaultCFDataTrait
{
    using Key = RawTiDBPK;
    using Value = std::tuple<std::shared_ptr<const TiKVKey>, std::shared_ptr<const TiKVValue>>;
    using Map = std::map<Key, Value>;

    static std::optional<Map::value_type> genKVPair(TiKVKey && key, const DecodedTiKVKey & raw_key, TiKVValue && value)
    {
        RawTiDBPK tidb_pk = RecordKVFormat::getRawTiDBPK(raw_key);
        return Map::value_type(
            std::move(tidb_pk),
            Value(
                std::make_shared<const TiKVKey>(std::move(key)),
                std::make_shared<const TiKVValue>(std::move(value))));
    }

    static std::shared_ptr<const TiKVValue> getTiKVValue(const Map::const_iterator & it)
    {
        return std::get<1>(it->second);
    }
};

struct LargeTxnDefaultCf
{
    using Trait = LargeDefaultCFDataTrait;
    using Inner = RegionCFDataBase<LargeDefaultCFDataTrait>;
    using Level1Key = Timestamp;
    using Level2Key = typename Trait::Key; // Actually RawTiDBPK
    using Value = typename Trait::Value;
    using Map = typename std::unordered_map<Level1Key, std::shared_ptr<Inner>>;
    using InnerMap = Inner::Map;
    using Data = Map;
    using Pair = std::pair<Level2Key, Value>;
    using Status = bool;

    using ConstTwoLevelIt = std::optional<std::tuple<Map::const_iterator, InnerMap::const_iterator>>;
    using TwoLevelIt = std::optional<std::tuple<Map::iterator, InnerMap::iterator>>;

    ConstTwoLevelIt find(const Level2Key & key, const Level1Key & ts) const;

    static std::shared_ptr<Inner> & mustGet(LargeTxnDefaultCf & cf, const Level1Key & key);

    RegionDataRes insertWithTs(TiKVKey && key, TiKVValue && value, Timestamp ts, DupCheck mode = DupCheck::Deny);
    RegionDataRes insert(TiKVKey && key, TiKVValue && value, DupCheck mode = DupCheck::Deny);

    static size_t calcTiKVKeyValueSize(const Inner::Value & value);
    static size_t calcTiKVKeyValueSize(const TiKVKey & key, const TiKVValue & value);

    // Serves "Del" raft command.
    size_t remove(const Level2Key & key, const Level1Key & ts, bool quiet = false);
    // Just remove the given key, if any.
    void erase(const Level2Key & key, const Level1Key & ts);
    void erase(const ConstTwoLevelIt & it);
    static bool cmp(const Map & a, const Map & b);

    bool operator==(const LargeTxnDefaultCf & cf) const;

    size_t getSize() const;

    size_t getTxnCount() const;

    LargeTxnDefaultCf() = default;
    LargeTxnDefaultCf(LargeTxnDefaultCf && region)
        : txns(std::move(region.txns))
    {}

    LargeTxnDefaultCf & operator=(LargeTxnDefaultCf && region)
    {
        txns = std::move(region.txns);
        return *this;
    }

    size_t splitInto(const RegionRange & range, LargeTxnDefaultCf & new_region_data);
    size_t mergeFrom(const LargeTxnDefaultCf & ori_region_data);

    size_t serializeMeta(WriteBuffer & buf) const;
    size_t serialize(WriteBuffer & buf) const;
    static size_t deserializeMeta(ReadBuffer & buf);
    static size_t deserialize(ReadBuffer & buf, size_t txn_count, LargeTxnDefaultCf & new_region_data);

    const Data & getData() const { return txns; }
    Data & getDataMut() { return txns; }
    const Inner & getTxn(const Level1Key & ts) const;
    Inner & getTxnMut(const Level1Key & ts);
    bool hasTxn(const Level1Key & ts) const;
    size_t getTxnKeyCount(const Level1Key & ts) const;

    static size_t getTiKVKeyValueSize(const ConstTwoLevelIt & it);

private:
    friend class RegionData;

private:
    Data txns;
};

} // namespace DB
