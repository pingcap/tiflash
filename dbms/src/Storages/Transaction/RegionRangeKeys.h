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

#include <Storages/Transaction/TiKVHandle.h>
#include <Storages/Transaction/TiKVKeyValue.h>

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

    State state;
    TiKVKey key;
};

using DecodedTiKVKeyPtr = std::shared_ptr<DecodedTiKVKey>;
class RegionRangeKeys : boost::noncopyable
{
public:
    using RegionRange = std::pair<TiKVRangeKey, TiKVRangeKey>;

    const RegionRange & comparableKeys() const;
    static RegionRange makeComparableKeys(TiKVKey && start_key, TiKVKey && end_key);
    const std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & rawKeys() const;
    explicit RegionRangeKeys(TiKVKey && start_key, TiKVKey && end_key);
    TableID getMappedTableID() const;

private:
    RegionRange ori;
    std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> raw;
    TableID mapped_table_id;
};

} // namespace DB
