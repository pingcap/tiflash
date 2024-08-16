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

#include <Storages/KVStore/MultiRaft/RegionRangeKeys.h>
#include <Storages/KVStore/Types.h>

#include <map>
#include <variant>

namespace DB
{
namespace tests
{
class KVStoreTestBase;
class RegionKVStoreOldTest;
} // namespace tests

class Region;
using RegionPtr = std::shared_ptr<Region>;
using RegionMap = std::unordered_map<RegionID, RegionPtr>;

struct TiKVRangeKey;
using RegionRange = RegionRangeKeys::RegionRange;

struct TiKVRangeKeyCmp
{
    bool operator()(const TiKVRangeKey & x, const TiKVRangeKey & y) const;
};

struct IndexNode
{
    RegionMap region_map;
};

class RegionsRangeIndex : private boost::noncopyable
{
public:
    using RootMap = std::map<TiKVRangeKey, IndexNode, TiKVRangeKeyCmp>;
    using OverlapInfo = std::tuple<TiKVRangeKey, std::vector<RegionID>>;

    void add(const RegionPtr & new_region);

    void remove(const RegionRange & range, RegionID region_id);

    RegionMap findByRangeOverlap(const RegionRange & range) const;

    // Returns a region map of all regions of range, or the id of the first region that is checked overlapped with another region.
    std::variant<RegionMap, OverlapInfo> findByRangeChecked(const RegionRange & range) const;

    RegionsRangeIndex();

    const RootMap & getRoot() const;

    void clear();

    void tryMergeEmpty();

    // TODO this friend class decl is not working.
    RootMap::iterator split(const TiKVRangeKey & new_start);

    static bool isRangeOverlapped(const RegionRangeKeys::RegionRange &, const RegionRangeKeys::RegionRange &);

private:
    friend class ::DB::tests::KVStoreTestBase;
    friend class ::DB::tests::RegionKVStoreOldTest;
    void tryMergeEmpty(RootMap::iterator remove_it);

private:
    RootMap root;
    RootMap::const_iterator min_it;
    RootMap::const_iterator max_it;
};

} // namespace DB
