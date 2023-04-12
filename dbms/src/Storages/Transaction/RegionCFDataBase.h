// Copyright 2022 PingCAP, Ltd.
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

#include <Storages/Transaction/TiKVKeyValue.h>

#include <map>

namespace DB
{

struct TiKVRangeKey;
using RegionRange = std::pair<TiKVRangeKey, TiKVRangeKey>;
using RegionDataRes = size_t;

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

    RegionDataRes insert(TiKVKey && key, TiKVValue && value, DupCheck mode = DupCheck::Deny);

    static size_t calcTiKVKeyValueSize(const Value & value);

    static size_t calcTiKVKeyValueSize(const TiKVKey & key, const TiKVValue & value);

    size_t remove(const Key & key, bool quiet = false);

    static bool cmp(const Map & a, const Map & b);

    bool operator==(const RegionCFDataBase & cf) const;

    size_t getSize() const;

    RegionCFDataBase() {}
    RegionCFDataBase(RegionCFDataBase && region);
    RegionCFDataBase & operator=(RegionCFDataBase && region);

    size_t splitInto(const RegionRange & range, RegionCFDataBase & new_region_data);
    size_t mergeFrom(const RegionCFDataBase & ori_region_data);

    size_t serialize(WriteBuffer & buf) const;

    static size_t deserialize(ReadBuffer & buf, RegionCFDataBase & new_region_data);

    const Data & getData() const;

    Data & getDataMut();

private:
    static bool shouldIgnoreRemove(const Value & value);
    RegionDataRes insert(std::pair<Key, Value> && kv_pair, DupCheck mode = DupCheck::Deny);

private:
    Data data;
};

} // namespace DB
