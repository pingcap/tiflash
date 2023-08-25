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

#include <Common/CurrentMetrics.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/DeltaIndex.h>
#include <Storages/DeltaMerge/Remote/RNDeltaIndexCache.h>
namespace CurrentMetrics
{
extern const Metric DT_DeltaIndexCacheSize;
} // namespace CurrentMetrics

namespace DB::DM::Remote
{

DeltaIndexPtr RNDeltaIndexCache::getDeltaIndex(const CacheKey & key)
{
    auto [value, miss]
        = cache.getOrSet(key, [&key] { return std::make_shared<CacheValue>(std::make_shared<DeltaIndex>(key), 0); });
    if (miss)
    {
        GET_METRIC(tiflash_storage_delta_index_cache, type_miss).Increment();
    }
    else
    {
        GET_METRIC(tiflash_storage_delta_index_cache, type_hit).Increment();
    }
    return value->delta_index;
}

void RNDeltaIndexCache::setDeltaIndex(const DeltaIndexPtr & delta_index)
{
    RUNTIME_CHECK(delta_index != nullptr);
    if (const auto & key = delta_index->getRNCacheKey(); key)
    {
        std::lock_guard lock(mtx);
        if (auto value = cache.get(*key); value)
        {
            cache.set(*key, std::make_shared<CacheValue>(delta_index, delta_index->getBytes()));
            CurrentMetrics::set(CurrentMetrics::DT_DeltaIndexCacheSize, cache.weight());
        }
    }
}

} // namespace DB::DM::Remote
