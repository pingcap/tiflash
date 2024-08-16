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

#include <Storages/KVStore/MultiRaft/RegionsRangeIndex.h>
#include <Storages/KVStore/Region.h>

namespace DB
{
bool TiKVRangeKeyCmp::operator()(const TiKVRangeKey & x, const TiKVRangeKey & y) const
{
    return x.compare(y) < 0;
}

void RegionsRangeIndex::add(const RegionPtr & new_region)
{
    auto new_region_range = new_region->getRange();
    const auto & new_range_keys = new_region_range->comparableKeys();
    auto begin_it = split(new_range_keys.first);
    auto end_it = split(new_range_keys.second);
    if (begin_it == end_it)
        throw Exception(
            std::string(__PRETTY_FUNCTION__) + ": range of region_id=" + toString(new_region->id()) + " is empty",
            ErrorCodes::LOGICAL_ERROR);

    for (auto it = begin_it; it != end_it; ++it)
        it->second.region_map.emplace(new_region->id(), new_region);
}

void RegionsRangeIndex::remove(const RegionRange & range, RegionID region_id)
{
    auto begin_it = root.find(range.first);
    if (begin_it == root.end())
        throw Exception(std::string(__PRETTY_FUNCTION__) + ": not found start key", ErrorCodes::LOGICAL_ERROR);

    auto end_it = root.find(range.second);
    if (end_it == root.end())
        throw Exception(std::string(__PRETTY_FUNCTION__) + ": not found end key", ErrorCodes::LOGICAL_ERROR);

    if (begin_it == end_it)
        throw Exception(
            std::string(__PRETTY_FUNCTION__) + ": range of region_id=" + toString(region_id) + " is empty",
            ErrorCodes::LOGICAL_ERROR);

    for (auto it = begin_it; it != end_it; ++it)
    {
        if (it->second.region_map.erase(region_id) == 0)
            throw Exception(
                std::string(__PRETTY_FUNCTION__) + ": not found region_id=" + toString(region_id),
                ErrorCodes::LOGICAL_ERROR);
    }
    tryMergeEmpty(begin_it);
}

RegionMap RegionsRangeIndex::findByRangeOverlap(const RegionRange & range) const
{
    auto begin_it = root.lower_bound(range.first);
    auto end_it = root.lower_bound(range.second);
    if (begin_it->first.compare(range.first) != 0)
        --begin_it;

    RegionMap res;
    for (auto it = begin_it; it != end_it; ++it)
        res.insert(it->second.region_map.begin(), it->second.region_map.end());
    return res;
}

std::variant<RegionMap, RegionsRangeIndex::OverlapInfo> RegionsRangeIndex::findByRangeChecked(
    const RegionRange & range) const
{
    auto begin_it = root.lower_bound(range.first);
    auto end_it = root.lower_bound(range.second);
    if (begin_it->first.compare(range.first) != 0)
        --begin_it;

    RegionMap res;
    for (auto it = begin_it; it != end_it; ++it)
    {
        if (it->second.region_map.size() < 2)
            res.insert(it->second.region_map.begin(), it->second.region_map.end());
        else
        {
            std::vector<RegionID> v;
            for (const auto & iter : it->second.region_map)
            {
                v.push_back(iter.first);
            }
            return std::make_tuple(it->first.copy(), std::move(v));
        }
    }
    return res;
}

RegionsRangeIndex::RegionsRangeIndex()
{
    clear();
}

const RegionsRangeIndex::RootMap & RegionsRangeIndex::getRoot() const
{
    return root;
}

void RegionsRangeIndex::clear()
{
    root.clear();
    min_it = root.emplace(TiKVRangeKey::makeTiKVRangeKey<true>(TiKVKey()), IndexNode{}).first;
    max_it = root.emplace(TiKVRangeKey::makeTiKVRangeKey<false>(TiKVKey()), IndexNode{}).first;
}

void RegionsRangeIndex::tryMergeEmpty()
{
    tryMergeEmpty(root.begin());
}

void RegionsRangeIndex::tryMergeEmpty(RootMap::iterator remove_it)
{
    if (!remove_it->second.region_map.empty())
        return;

    auto left_it = remove_it, right_it = remove_it;

    if (left_it != min_it)
    {
        auto it = remove_it;
        do
        {
            --it;
            if (it->second.region_map.empty())
                left_it = it;
            else
                break;
        } while (it != min_it);
    }

    for (; right_it != max_it; ++right_it)
    {
        if (!right_it->second.region_map.empty())
            break;
    }
    left_it++;
    root.erase(left_it, right_it);
}

RegionsRangeIndex::RootMap::iterator RegionsRangeIndex::split(const TiKVRangeKey & new_start)
{
    const auto do_split = [this](RootMap::iterator begin_it, const TiKVRangeKey & new_start) {
        begin_it--;
        auto & ori = begin_it->second;
        auto tar_it = root.emplace(new_start.copy(), IndexNode{}).first;
        tar_it->second.region_map = ori.region_map;
        return tar_it;
    };

    auto begin_it = root.lower_bound(new_start);
    assert(begin_it != root.end());

    if (begin_it->first.compare(new_start) == 0)
        return begin_it;
    else
        return do_split(begin_it, new_start);
}


bool RegionsRangeIndex::isRangeOverlapped(const RegionRangeKeys::RegionRange & a, const RegionRangeKeys::RegionRange & b) {
    auto start = a.first.compare(b.first);
    if(start == 0) {
        return true;
    } else if (start < 0) {
        return a.second.compare(b.second) >= 0;
    } else {
        return b.second.compare(a.second) >= 0;
    }
}


} // namespace DB
