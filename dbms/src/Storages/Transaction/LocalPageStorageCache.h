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

#include <map>
#include <mutex>
#include <optional>

#include "Poco/Logger.h"
#include "common/logger_useful.h"

template <typename T>
struct LocalPageStorageCache
{
    using Key = std::pair<uint64_t, uint64_t>;

    struct Comparator
    {
        bool operator()(const Key & a, const Key & b) const
        {
            if (a.first == b.first)
            {
                // reverse
                return b.second < a.second;
            }
            return a.first < b.first;
        }
    };

    LocalPageStorageCache(size_t size_for_every_store)
        : evict_size(size_for_every_store)
    {
    }

    std::optional<T> maybe_get(uint64_t store_id, uint64_t version) const
    {
        std::scoped_lock lock(this->mtx);

        auto key = std::make_pair(store_id, version);
        if (cache.count(key))
        {
            return cache.at(key);
        }
        return std::nullopt;
    }

    void insert(uint64_t store_id, uint64_t version, T ptr)
    {
        std::scoped_lock lock(this->mtx);
        cache[std::make_pair(store_id, version)] = ptr;
        guardedMaybeEvict(store_id);
    }

    void guardedMaybeEvict(uint64_t store_id)
    {
        // The first greater than (store_id, 0), or end.
        auto upper = cache.upper_bound(std::make_pair(store_id, 0));
        // The first greater than (store_id - 1, 0), or end.
        // TODO store_id should not be 0.
        auto lower = cache.upper_bound(std::make_pair(store_id - 1, 0));
        size_t cnt = 0;
        auto iter = lower;
        while (iter != upper && iter != cache.end())
        {
            if (iter->first.first == store_id)
                cnt++;
            if (cnt > evict_size)
            {
                LOG_DEBUG(&Poco::Logger::get("!!!! AAAA"), "Remove {} {}", iter->first.first, iter->first.second);
                iter = cache.erase(iter);
            }
            else
            {
                LOG_DEBUG(&Poco::Logger::get("!!!! AAAA"), "Skip {} {}", iter->first.first, iter->first.second);
                iter++;
            }
        }
    }

protected:
    std::map<Key, T, Comparator> cache;
    mutable std::mutex mtx;
    size_t evict_size;
};
