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

#include <Common/TiFlashMetrics.h>
#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>
#include <Storages/KVStore/MultiRaft/RegionRangeKeys.h>

#include <map>

namespace DB
{

struct TiKVRangeKey;
using RegionRange = RegionRangeKeys::RegionRange;
struct RegionDataMemDiff
{
    using Type = Int64;
    Type payload;
    Type decoded;

    template <typename T1, typename T2>
    RegionDataMemDiff(T1 payload_, T2 decoded_)
        : payload(payload_)
        , decoded(decoded_)
    {}
    RegionDataMemDiff()
        : payload(0)
        , decoded(0)
    {}

    RegionDataMemDiff operator-() const { return {-payload, -decoded}; }

    void add(const RegionDataMemDiff & other)
    {
        payload += other.payload;
        decoded += other.decoded;
    }

    void sub(const RegionDataMemDiff & other)
    {
        payload -= other.payload;
        decoded -= other.decoded;
    }
};

enum class DupCheck
{
    Deny,
    AllowSame,
};

template <typename Trait>
struct RegionCFDataBase
{
    using Key = typename Trait::Key;
    using Value = typename Trait::Value;
    using Map = typename Trait::Map;
    using Data = Map;
    using Pair = std::pair<Key, Value>;
    using Status = bool;

    static const TiKVKey & getTiKVKey(const Value & val);
    static const TiKVValue & getTiKVValue(const Value & val);

    RegionDataMemDiff insert(TiKVKey && key, TiKVValue && value, DupCheck mode = DupCheck::Deny);

    static size_t calcTiKVKeyValueSize(const Value & value);
    static size_t calcTiKVKeyValueSize(const TiKVKey & key, const TiKVValue & value);
    static RegionDataMemDiff calcTotalKVSize(const Value & value);

    size_t calcDecodedKeyValueSize(const Value & value);
    size_t calcDecodedKeyValueSize(const TiKVKey & key, const TiKVValue & value);

    RegionDataMemDiff remove(const Key & key, bool quiet = false);

    static bool cmp(const Map & a, const Map & b);

    bool operator==(const RegionCFDataBase & cf) const;

    size_t getSize() const;

    RegionCFDataBase() = default;
    RegionCFDataBase(RegionCFDataBase && region);
    RegionCFDataBase & operator=(RegionCFDataBase && region);

    RegionDataMemDiff splitInto(const RegionRange & range, RegionCFDataBase & new_region_data);
    RegionDataMemDiff mergeFrom(const RegionCFDataBase & ori_region_data);

    size_t serialize(WriteBuffer & buf) const;
    static RegionDataMemDiff deserialize(ReadBuffer & buf, RegionCFDataBase & new_region_data);

    const Data & getData() const;

    Data & getDataMut();

private:
    static bool shouldIgnoreRemove(const Value & value);

private:
    Data data;
};

} // namespace DB
