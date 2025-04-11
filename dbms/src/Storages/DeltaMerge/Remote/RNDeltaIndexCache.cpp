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
#include <Storages/DeltaMerge/DeltaIndex/DeltaIndex.h>
#include <Storages/DeltaMerge/Remote/RNDeltaIndexCache.h>
#include <Storages/DeltaMerge/VersionChain/VersionChain.h>
namespace CurrentMetrics
{
extern const Metric DT_DeltaIndexCacheSize;
} // namespace CurrentMetrics

namespace DB::DM::Remote
{

namespace
{
void reportCacheHitStats(bool miss)
{
    if (miss)
        GET_METRIC(tiflash_storage_delta_index_cache, type_miss).Increment();
    else
        GET_METRIC(tiflash_storage_delta_index_cache, type_hit).Increment();
}
} // namespace

DeltaIndexPtr RNDeltaIndexCache::getDeltaIndex(const CacheKey & key)
{
    RUNTIME_CHECK(!key.is_version_chain);
    auto [value, miss] = cache.getOrSet(key, [&key] {
        return std::make_shared<CacheValue>(CacheDeltaIndex(std::make_shared<DeltaIndex>(key), 0));
    });
    reportCacheHitStats(miss);
    return value->getDeltaIndex();
}

void RNDeltaIndexCache::setDeltaIndex(const CacheKey & key, const DeltaIndexPtr & delta_index)
{
    RUNTIME_CHECK(delta_index != nullptr);
    std::lock_guard lock(mtx);
    if (auto value = cache.get(key); value)
    {
        cache.set(key, std::make_shared<CacheValue>(CacheDeltaIndex(delta_index, delta_index->getBytes())));
        CurrentMetrics::set(CurrentMetrics::DT_DeltaIndexCacheSize, cache.weight());
    }
}

GenericVersionChainPtr RNDeltaIndexCache::getVersionChain(const CacheKey & key, bool is_common_handle)
{
    RUNTIME_CHECK(key.is_version_chain);
    auto [value, miss] = cache.getOrSet(key, [is_common_handle] {
        return std::make_shared<CacheValue>(CacheVersionChain(createVersionChain(is_common_handle), 0));
    });
    reportCacheHitStats(miss);
    return value->getVersionChain();
}

void RNDeltaIndexCache::setVersionChain(const CacheKey & key, const GenericVersionChainPtr & version_chain)
{
    RUNTIME_CHECK(version_chain != nullptr);
    std::lock_guard lock(mtx);
    if (auto value = cache.get(key); value)
    {
        cache.set(
            key,
            std::make_shared<CacheValue>(CacheVersionChain(version_chain, getVersionChainBytes(*version_chain))));
        CurrentMetrics::set(CurrentMetrics::DT_DeltaIndexCacheSize, cache.weight());
    }
}

} // namespace DB::DM::Remote
