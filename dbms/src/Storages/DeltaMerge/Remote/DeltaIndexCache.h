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
#include <common/types.h>

#include <boost/noncopyable.hpp>

namespace DB::DM::Remote
{
/**
 * A LRU cache that holds delta-tree indexes from different remote write nodes.
 * Delta-tree indexes are used as much as possible when same segments are accessed multiple times.
 */
class DeltaIndexCache : private boost::noncopyable
{
public:
    explicit DeltaIndexCache()
        : cache(1000) // TODO
    {
    }

    struct CacheKey
    {
        UInt64 write_node_id;
        UInt64 segment_id;
        UInt64 segment_epoch;

        bool operator==(const CacheKey & other) const
        {
            return write_node_id == other.write_node_id
                && segment_id == other.segment_id
                && segment_epoch == other.segment_epoch;
        }
    };

    struct CacheKeyHasher
    {
        size_t operator()(const CacheKey & k) const
        {
            using std::hash;

            return hash<UInt64>()(k.write_node_id) ^ hash<UInt64>()(k.segment_id) ^ hash<UInt64>()(k.segment_epoch);
        }
    };

    /**
     * Returns a cached or newly created delta index, which is assigned to the specified segment(at)epoch.
     */
    DeltaIndexPtr getDeltaIndex(const CacheKey & key)
    {
        auto [index_ptr, _] = cache.getOrSet(key, [] {
            auto index = std::make_shared<DeltaIndex>();
            return index;
        });

        return index_ptr;
    }

private:
    struct WeakIndex
    {
        std::weak_ptr<DeltaIndex> index;
    };

    LRUCache<CacheKey, DeltaIndex, CacheKeyHasher> cache;
};

using DeltaIndexCachePtr = std::shared_ptr<DeltaIndexCache>;

} // namespace DB::DM::Remote
