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

#include <Storages/DeltaMerge/FilterExpressionCache.h>

#include <optional>


namespace DB::DM
{

std::optional<BitmapFilterPtr> FilterExpressionCache::get(const std::string & filter_expression) const
{
    std::shared_lock lock(rw_mutex);

    auto it = map.find(filter_expression);
    if (it == map.end())
        return {};

    list.splice(list.begin(), list, it->second);
    return it->second->second;
}

void FilterExpressionCache::set(const std::string & filter_expression, const BitmapFilterPtr & result)
{
    if (result->size() == 0)
        return;
    std::unique_lock lock(rw_mutex);
    auto it = map.find(filter_expression);
    if (it != map.end())
    {
        list.splice(list.begin(), list, it->second);
        it->second->second = result;
        return;
    }

    if (list.size() == capacity)
    {
        map.erase(list.back().first);
        list.pop_back();
    }

    list.emplace_front(filter_expression, result);
    map[filter_expression] = list.begin();
}

void FilterExpressionCache::clear()
{
    std::unique_lock lock(rw_mutex);

    list.clear();
    map.clear();
}

size_t FilterExpressionCache::size() const
{
    std::shared_lock lock(rw_mutex);
    return list.size();
}

} // namespace DB::DM