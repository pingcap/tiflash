#pragma once

#include <Core/Block.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/Transaction/Types.h>
#include <common/logger_useful.h>
#include <cstddef>
#include <memory>

namespace DB
{
namespace DM
{
using ColId      = DB::ColumnID;
using PackId     = size_t;
using PackRange  = std::pair<PackId, PackId>;
using PackRanges = std::vector<PackRange>;
class ColumnCache : public std::enable_shared_from_this<ColumnCache>, private boost::noncopyable
{
public:
    enum class Strategy
    {
        Memory,
        Disk,
        Unknown
    };

    ColumnCache() = default;

    using RangeWithStrategy  = std::pair<PackRange, ColumnCache::Strategy>;
    using RangeWithStrategys = std::vector<RangeWithStrategy>;
    RangeWithStrategys getReadStrategy(size_t pack_id, size_t pack_count, ColId column_id);

    void tryPutColumn(size_t pack_id, ColId column_id, const ColumnPtr & column, size_t rows_offset, size_t rows_count);

    using ColumnCacheElement = std::pair<ColumnPtr, std::pair<size_t, size_t>>;
    ColumnCacheElement getColumn(size_t pack_id, ColId column_id);

private:
    bool isPackInCache(PackId pack_id, ColId column_id);

private:
    struct ColumnCacheEntry
    {
        std::unordered_map<ColId, ColumnPtr> columns;

        size_t rows_offset;
        size_t rows_count;
    };
    std::unordered_map<PackId, ColumnCacheEntry> column_caches;
};

using ColumnCachePtr     = std::shared_ptr<ColumnCache>;
using ColumnCachePtrs    = std::vector<ColumnCachePtr>;
using RangeWithStrategy  = ColumnCache::RangeWithStrategy;
using RangeWithStrategys = ColumnCache::RangeWithStrategys;
using ColumnCacheElement = ColumnCache::ColumnCacheElement;
} // namespace DM
} // namespace DB
