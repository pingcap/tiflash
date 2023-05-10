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

#include <Storages/DeltaMerge/BitmapFilter/BitmapFilter.h>

#include <list>
#include <shared_mutex>
#include <string>

// TODO: make it configurable.
#define DefaultFilterExpressionCacheCapacity 100


namespace DB::DM
{

// FilterExpressionCache is used to cache the result of filter expression in stable layer.
// LRU is used to evict the least recently used item when the cache is full.
// The cache is thread-safe.
class FilterExpressionCache
{
public:
    explicit FilterExpressionCache(size_t capacity = DefaultFilterExpressionCacheCapacity)
        : capacity(capacity)
    {}

    ~FilterExpressionCache() = default;

    // Get the result of filter expression from cache.
    std::optional<BitmapFilterPtr> get(const std::string & filter_expression);

    // Set the result of filter expression to cache.
    void set(const std::string & filter_expression, const BitmapFilterPtr & result);

    // Clear the cache.
    void clear();

    size_t size() const;

private:
    // The capacity of cache.
    size_t capacity;

    // The mutex to protect the cache.
    mutable std::shared_mutex rw_mutex;

    // The list to store the filter expression.
    // The most recently used item is at the front of the list.
    std::list<std::pair<std::string, BitmapFilterPtr>> list;

    // The map to store the filter expression and its result.
    std::unordered_map<std::string, std::list<std::pair<std::string, BitmapFilterPtr>>::iterator> map;
};

} // namespace DB::DM