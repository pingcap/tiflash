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

#include <Common/LRUCache.h>
#include <Storages/DeltaMerge/DeltaIndex.h>
#include <Storages/DeltaMerge/Remote/RNDeltaIndexCache_fwd.h>
#include <common/types.h>

#include <boost/noncopyable.hpp>

namespace DB::DM::Remote
{
/**
 * A LRU cache that holds delta-tree indexes from different remote write nodes.
 * Delta-tree indexes are used as much as possible when same segments are accessed multiple times.
 */
class RNDeltaIndexCache : private boost::noncopyable
{
public:
    // TODO: Currently we use a quantity based cache size. We could change to memory-size based.
    //       However, as the delta index's size could be changing, we need to implement our own LRU instead.
    explicit RNDeltaIndexCache(size_t max_cache_keys)
        : cache(max_cache_keys)
    {
    }

    struct CacheKey
    {
        UInt64 store_id;
        Int64 table_id;
        UInt64 segment_id;
        UInt64 segment_epoch;
        UInt64 delta_index_epoch;

        bool operator==(const CacheKey & other) const
        {
            return store_id == other.store_id
                && table_id == other.table_id
                && segment_id == other.segment_id
                && segment_epoch == other.segment_epoch
                && delta_index_epoch == other.delta_index_epoch;
        }
    };

    struct CacheKeyHasher
    {
        size_t operator()(const CacheKey & k) const
        {
            using std::hash;

            return hash<UInt64>()(k.store_id) ^ //
                hash<Int64>()(k.table_id) ^ //
                hash<UInt64>()(k.segment_id) ^ //
                hash<UInt64>()(k.segment_epoch) ^ //
                hash<UInt64>()(k.delta_index_epoch);
        }
    };

    /**
     * Returns a cached or newly created delta index, which is assigned to the specified segment(at)epoch.
     */
    DeltaIndexPtr getDeltaIndex(const CacheKey & key);

private:
    LRUCache<CacheKey, DeltaIndex, CacheKeyHasher> cache;
};

} // namespace DB::DM::Remote
