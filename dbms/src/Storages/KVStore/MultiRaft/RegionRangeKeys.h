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

#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>
#include <Storages/KVStore/Decode/TiKVHandle.h>
#include <Storages/KVStore/TiKVHelpers/TiKVKeyValue.h>

namespace DB
{

struct TiKVRangeKey : boost::noncopyable
{
    enum State : Int8
    {
        MIN = 1,
        NORMAL = 2,
        MAX = 4,
    };

    int compare(const TiKVKey & tar) const;
    int compare(const TiKVRangeKey & tar) const;

    template <bool is_start>
    static TiKVRangeKey makeTiKVRangeKey(TiKVKey &&);

    TiKVRangeKey(State state_, TiKVKey && key_);
    TiKVRangeKey(TiKVRangeKey &&);

    TiKVRangeKey copy() const;
    TiKVRangeKey & operator=(TiKVRangeKey &&);
    std::string toDebugString() const;
    std::string toString() const { return key.toString(); }

    State state;
    TiKVKey key;
};

class RegionRangeKeys : boost::noncopyable
{
public:
    using RegionRange = std::pair<TiKVRangeKey, TiKVRangeKey>;

    static RegionRange cloneRange(const RegionRange & from);
    static RegionRange makeComparableKeys(TiKVKey && start_key, TiKVKey && end_key);


    explicit RegionRangeKeys(TiKVKey && start_key, TiKVKey && end_key);
    explicit RegionRangeKeys(RegionRange && range)
        : RegionRangeKeys(std::move(range.first.key), std::move(range.second.key))
    {}

    const std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & rawKeys() const { return raw; }
    const RegionRange & comparableKeys() const { return ori; }

    KeyspaceTableID getKeyspaceTableID() const { return KeyspaceTableID(keyspace_id, mapped_table_id); }
    TableID getMappedTableID() const { return mapped_table_id; }
    KeyspaceID getKeyspaceID() const { return keyspace_id; }
    std::string toDebugString() const;

    static bool isRangeOverlapped(const RegionRange & a, const RegionRange & b)
    {
        auto start = a.first.compare(b.first);
        if (start == 0)
        {
            return true;
        }
        else if (start < 0)
        {
            return a.second.compare(b.first) > 0;
        }
        else
        {
            return b.second.compare(a.first) > 0;
        }
    }

private:
    RegionRange ori;
    std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> raw;
    TableID mapped_table_id = InvalidTableID;
    KeyspaceID keyspace_id = NullspaceID;
};

bool computeMappedTableID(const DecodedTiKVKey & key, TableID & table_id);

} // namespace DB
