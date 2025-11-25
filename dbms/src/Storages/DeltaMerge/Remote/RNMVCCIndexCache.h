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
#include <Storages/DeltaMerge/Remote/RNMVCCIndexCache_fwd.h>
#include <Storages/DeltaMerge/VersionChain/VersionChain_fwd.h>
#include <Storages/KVStore/Types.h>
#include <common/types.h>
#include <fmt/format.h>

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
class RNMVCCIndexCache : private boost::noncopyable
{
public:
    explicit RNMVCCIndexCache(size_t max_cache_size)
        : cache(max_cache_size)
    {}

    struct CacheKey
    {
        UInt64 store_id;
        Int64 table_id;
        UInt64 segment_id;
        UInt64 segment_epoch;
        UInt64 delta_index_epoch;
        KeyspaceID keyspace_id;
        bool is_version_chain;

        bool operator==(const CacheKey & other) const
        {
            return store_id == other.store_id && keyspace_id == other.keyspace_id && table_id == other.table_id
                && segment_id == other.segment_id && segment_epoch == other.segment_epoch
                && delta_index_epoch == other.delta_index_epoch && is_version_chain == other.is_version_chain;
        }

        String toString() const
        {
            return fmt::format(
                "<store_id={}, table_id={}, segment_id={}, segment_epoch={}, "
                "delta_index_epoch={}, keyspace={}, is_version_chain={}>",
                store_id,
                table_id,
                segment_id,
                segment_epoch,
                delta_index_epoch,
                keyspace_id,
                is_version_chain);
        }
    };

    // Returns a cached or newly created delta index, which is assigned to the specified segment(at)epoch.
    DeltaIndexPtr getDeltaIndex(const CacheKey & key);
    // `setDeltaIndex` will updated cache size and remove overflows if necessary.
    void setDeltaIndex(const CacheKey & key, const DeltaIndexPtr & delta_index);

    // Similar to `getDeltaIndex`, but this method is used to get version chain.
    GenericVersionChainPtr getVersionChain(const CacheKey & key, bool is_common_handle);
    // Similar to `setDeltaIndex`, but this method is used to set version chain.
    void setVersionChain(const CacheKey & key, const GenericVersionChainPtr & version_chain);

    size_t getCacheWeight() const { return cache.weight(); }
    size_t getCacheCount() const { return cache.count(); }

private:
    struct CacheDeltaIndex
    {
        CacheDeltaIndex(const DeltaIndexPtr & delta_index_, size_t bytes_)
            : delta_index(delta_index_)
            , bytes(bytes_)
        {}

        DeltaIndexPtr delta_index;
        const size_t bytes;
    };

    struct CacheVersionChain
    {
        CacheVersionChain(const GenericVersionChainPtr & version_chain_, size_t bytes_)
            : version_chain(version_chain_)
            , bytes(bytes_)
        {}

        GenericVersionChainPtr version_chain;
        const size_t bytes;
    };
    struct CacheValue
    {
        std::variant<CacheDeltaIndex, CacheVersionChain> value;

        size_t bytes() const
        {
            return std::visit([](const auto & v) { return v.bytes; }, value);
        }

        DeltaIndexPtr getDeltaIndex() const { return std::get<CacheDeltaIndex>(value).delta_index; }

        GenericVersionChainPtr getVersionChain() const { return std::get<CacheVersionChain>(value).version_chain; }
    };

    struct CacheKeyHasher
    {
        size_t operator()(const CacheKey & k) const
        {
            size_t seed = 0;
            boost::hash_combine(seed, k.store_id);
            boost::hash_combine(seed, k.table_id);
            boost::hash_combine(seed, k.segment_id);
            boost::hash_combine(seed, k.segment_epoch);
            boost::hash_combine(seed, k.delta_index_epoch);
            boost::hash_combine(seed, k.keyspace_id);
            boost::hash_combine(seed, k.is_version_chain);
            return seed;
        }
    };

    struct CacheValueWeight
    {
        size_t operator()(const CacheKey & key, const CacheValue & v) const { return sizeof(key) + v.bytes(); }
    };

private:
    std::mutex mtx;
    LRUCache<CacheKey, CacheValue, CacheKeyHasher, CacheValueWeight> cache;
};

} // namespace DB::DM::Remote
