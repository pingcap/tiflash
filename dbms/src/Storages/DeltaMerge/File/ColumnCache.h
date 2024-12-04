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

#include <Common/SharedMutexProtected.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/KVStore/Types.h>

#include <cstddef>
#include <memory>

namespace DB::DM
{

using ColId = DB::ColumnID;
using PackId = size_t;
using PackRange = std::pair<PackId, PackId>;
using PackRanges = std::vector<PackRange>;

enum class ColumnCacheType
{
    ExtraColumnCache,
    DataSharingCache,
};

// ColumnCache is a thread-safe cache for columns, the entry is one pack.
class ColumnCache : private boost::noncopyable
{
public:
    enum class Strategy
    {
        Memory,
        Disk,
        Unknown
    };

    explicit ColumnCache(ColumnCacheType type_ = ColumnCacheType::ExtraColumnCache)
        : type(type_)
    {}

    using RangeWithStrategy = std::pair<PackRange, ColumnCache::Strategy>;
    using RangeWithStrategys = std::vector<RangeWithStrategy>;
    RangeWithStrategys getReadStrategy(size_t start_pack_idx, size_t pack_count, ColId column_id);
    static RangeWithStrategys getCleanReadStrategy(
        size_t start_pack_idx,
        size_t pack_count,
        const std::vector<size_t> & clean_read_pack_idx);

    void tryPutColumn(size_t pack_id, ColId column_id, const ColumnPtr & column, size_t rows_offset, size_t rows_count);

    using ColumnCacheElement = std::pair<ColumnPtr, std::pair<size_t, size_t>>;
    ColumnCacheElement getColumn(size_t pack_id, ColId column_id);

    void delColumn(ColId column_id, size_t upper_pack_id);

    void clear()
    {
        column_caches.withExclusive([](auto & column_caches) { column_caches.clear(); });
    }

private:
    static RangeWithStrategys getReadStrategyImpl(
        size_t start_pack_idx,
        size_t pack_count,
        ColId column_id,
        std::function<bool(size_t, ColId)> is_hit);
    bool isPackInCache(PackId pack_id, ColId column_id);

private:
    struct ColumnCacheEntry
    {
        std::unordered_map<ColId, ColumnPtr> columns;

        size_t rows_offset;
        size_t rows_count;
    };
    SharedMutexProtected<std::unordered_map<PackId, ColumnCacheEntry>> column_caches;
    const ColumnCacheType type;
};

using ColumnCachePtr = std::shared_ptr<ColumnCache>;
using ColumnCachePtrs = std::vector<ColumnCachePtr>;
using RangeWithStrategy = ColumnCache::RangeWithStrategy;
using RangeWithStrategys = ColumnCache::RangeWithStrategys;
using ColumnCacheElement = ColumnCache::ColumnCacheElement;

} // namespace DB::DM
