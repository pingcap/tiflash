// Copyright 2024 PingCAP, Inc.
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

#include <Columns/IColumn.h>
#include <Common/LRUCache.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/ColumnCacheLongTerm_fwd.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/KVStore/Types.h>
#include <Storages/Page/PageDefinesBase.h>

namespace DB::DM
{

/**
 * @brief ColumnCacheLongTerm exists for the lifetime of the process to reduce.
 * repeated reading of some frequently used columns (like PK) involved in queries.
 * It is unlike ColumnCache, which only exists for the lifetime of a snapshot.
 *
 * Currently ColumnCacheLongTerm is only filled in Vector Search, which requires
 * high QPS.
 */
class ColumnCacheLongTerm
{
private:
    struct CacheKey
    {
        String dmfile_parent_path;
        PageIdU64 dmfile_id;
        ColumnID column_id;

        bool operator==(const CacheKey & other) const
        {
            return dmfile_parent_path == other.dmfile_parent_path //
                && dmfile_id == other.dmfile_id //
                && column_id == other.column_id;
        }
    };

    struct CacheKeyHasher
    {
        std::size_t operator()(const CacheKey & id) const
        {
            using boost::hash_combine;
            using boost::hash_value;

            std::size_t seed = 0;
            hash_combine(seed, hash_value(id.dmfile_parent_path));
            hash_combine(seed, hash_value(id.dmfile_id));
            hash_combine(seed, hash_value(id.column_id));
            return seed;
        }
    };

    struct CacheWeightFn
    {
        size_t operator()(const CacheKey & key, const IColumn::Ptr & col) const
        {
            return sizeof(key) + key.dmfile_parent_path.size() + col->byteSize();
        }
    };

    using LRUCache = DB::LRUCache<CacheKey, IColumn::Ptr, CacheKeyHasher, CacheWeightFn>;

public:
    explicit ColumnCacheLongTerm(size_t cache_size_bytes)
        : cache(cache_size_bytes)
    {}

    static bool isCacheableColumn(const ColumnDefine & cd) { return cd.type->isInteger(); }

    IColumn::Ptr get(
        const String & dmf_parent_path,
        PageIdU64 dmf_id,
        ColumnID column_id,
        std::function<IColumn::Ptr()> load_fn)
    {
        auto key = CacheKey{
            .dmfile_parent_path = dmf_parent_path,
            .dmfile_id = dmf_id,
            .column_id = column_id,
        };
        auto [result, _] = cache.getOrSet(key, [&load_fn] { return std::make_shared<IColumn::Ptr>(load_fn()); });
        return *result;
    }

    void clear() { cache.reset(); }

    void getStats(size_t & out_hits, size_t & out_misses) const { cache.getStats(out_hits, out_misses); }

private:
    LRUCache cache;
};

} // namespace DB::DM
