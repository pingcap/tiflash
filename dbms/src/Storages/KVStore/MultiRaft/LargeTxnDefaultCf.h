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

#include <Storages/KVStore/MultiRaft/RegionCFDataBase.h>
#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>
#include <Storages/KVStore/MultiRaft/RegionRangeKeys.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>
#include <Storages/KVStore/MultiRaft/RegionCFDataTrait.h>

namespace DB {
struct LargeTxnDefaultCf {
    using Trait = RegionDefaultCFDataTrait;
    using Inner = RegionCFDataBase<RegionDefaultCFDataTrait>;
    using Level1Key = Timestamp;
    using Key = typename Trait::Key;
    using Value = typename Trait::Value;
    using Map = typename std::unordered_map<Level1Key, std::shared_ptr<Inner>>;
    using Data = Map;
    using Pair = std::pair<Key, Value>;
    using Status = bool;

    static std::shared_ptr<Inner> & mustGet(LargeTxnDefaultCf & cf, const Level1Key & key);

    RegionDataRes insert(TiKVKey && key, TiKVValue && value, DupCheck mode = DupCheck::Deny);

    static size_t calcTiKVKeyValueSize(const Value & value);
    static size_t calcTiKVKeyValueSize(const TiKVKey & key, const TiKVValue & value);

    size_t remove(const Key & key, bool quiet = false);
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

    size_t serialize(WriteBuffer & buf) const;
    static size_t deserialize(ReadBuffer & buf, LargeTxnDefaultCf & new_region_data);

    const Data & getData() const {
        return txns;
    }
    Data & getDataMut() {
        return txns;
    }
private:
    // TODO(Split) We can neglect Timestamp in RegionDefaultCFDataTrait to save memory.
    Data txns;
};

} // namespace DB

