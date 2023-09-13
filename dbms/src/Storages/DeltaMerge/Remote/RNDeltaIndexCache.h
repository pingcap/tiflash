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

#include <Common/LRUCache.h>
#include <Storages/DeltaMerge/Remote/RNDeltaIndexCache_fwd.h>
#include <Storages/KVStore/Types.h>
#include <common/types.h>

#include <boost/noncopyable.hpp>

namespace DB::DM
{
class DeltaIndex;
using DeltaIndexPtr = std::shared_ptr<DeltaIndex>;
} // namespace DB::DM

namespace DB::DM::Remote
{
/**
 * A LRU cache that holds delta-tree indexes from different remote write nodes.
 * Delta-tree indexes are used as much as possible when same segments are accessed multiple times.
 */
class RNDeltaIndexCache : private boost::noncopyable
{
public:
    explicit RNDeltaIndexCache(size_t max_cache_size)
        : cache(max_cache_size)
    {}

    struct CacheKey
    {
        UInt64 store_id;
        KeyspaceID keyspace_id;
        Int64 table_id;
        UInt64 segment_id;
        UInt64 segment_epoch;
        UInt64 delta_index_epoch;

        bool operator==(const CacheKey & other) const
        {
            return store_id == other.store_id && keyspace_id == other.keyspace_id && table_id == other.table_id
                && segment_id == other.segment_id && segment_epoch == other.segment_epoch
                && delta_index_epoch == other.delta_index_epoch;
        }
    };

    struct CacheValue
    {
        CacheValue(const DeltaIndexPtr & delta_index_, size_t bytes_)
            : delta_index(delta_index_)
            , bytes(bytes_)
        {}

        DeltaIndexPtr delta_index;
        size_t bytes;
    };

    struct CacheKeyHasher
    {
        size_t operator()(const CacheKey & k) const
        {
            using std::hash;

            return hash<UInt64>()(k.store_id) ^ //
                hash<UInt64>()(k.keyspace_id) ^ //
                hash<Int64>()(k.table_id) ^ //
                hash<UInt64>()(k.segment_id) ^ //
                hash<UInt64>()(k.segment_epoch) ^ //
                hash<UInt64>()(k.delta_index_epoch);
        }
    };

    struct CacheValueWeight
    {
        size_t operator()(const CacheKey & key, const CacheValue & v) const { return sizeof(key) + v.bytes; }
    };

    /**
     * Returns a cached or newly created delta index, which is assigned to the specified segment(at)epoch.
     */
    DeltaIndexPtr getDeltaIndex(const CacheKey & key);

    // `setDeltaIndex` will updated cache size and remove overflows if necessary.
    void setDeltaIndex(const DeltaIndexPtr & delta_index);

    size_t getCacheWeight() const { return cache.weight(); }
    size_t getCacheCount() const { return cache.count(); }


private:
    std::mutex mtx;
    LRUCache<CacheKey, CacheValue, CacheKeyHasher, CacheValueWeight> cache;
};

} // namespace DB::DM::Remote
