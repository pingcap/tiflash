#include <Storages/DeltaMerge/File/ColumnCache.h>

namespace DB
{
namespace DM
{
RangeWithStrategys ColumnCache::getReadStrategy(size_t pack_id, size_t pack_count, ColId column_id)
{
    PackRange target_range{pack_id, pack_id + pack_count};

    RangeWithStrategys range_and_strategys;

    Strategy strategy = Strategy::Unknown;
    size_t range_start = 0;
    for (size_t cursor = target_range.first; cursor < target_range.second; cursor++)
    {
        if (isPackInCache(cursor, column_id))
        {
            if (strategy == Strategy::Memory)
            {
                continue;
            }
            else if (strategy == Strategy::Disk)
            {
                range_and_strategys.emplace_back(std::make_pair(PackRange{range_start, cursor}, Strategy::Disk));
            }
            range_start = cursor;
            strategy = Strategy::Memory;
        }
        else
        {
            if (strategy == Strategy::Memory)
            {
                range_and_strategys.emplace_back(std::make_pair(PackRange{range_start, cursor}, Strategy::Memory));
            }
            else if (strategy == Strategy::Disk)
            {
                continue;
            }
            range_start = cursor;
            strategy = Strategy::Disk;
        }
    }
    range_and_strategys.emplace_back(std::make_pair(PackRange{range_start, target_range.second}, strategy));

    return range_and_strategys;
}

void ColumnCache::tryPutColumn(size_t pack_id, ColId column_id, const ColumnPtr & column, size_t rows_offset, size_t rows_count)
{
    if (auto iter = column_caches.find(pack_id); iter != column_caches.end())
    {
        auto & column_cache_entry = iter->second;
        if (column_cache_entry.columns.find(column_id) != column_cache_entry.columns.end())
        {
            return;
        }
        if (column_cache_entry.rows_offset != rows_offset || column_cache_entry.rows_count != rows_count)
        {
            return;
        }

        column_cache_entry.columns.emplace(column_id, column);
    }
    else
    {
        ColumnCache::ColumnCacheEntry column_cache_entry;
        column_cache_entry.columns.emplace(column_id, column);
        column_cache_entry.rows_offset = rows_offset;
        column_cache_entry.rows_count = rows_count;

        column_caches.emplace(pack_id, column_cache_entry);
    }
}

ColumnCacheElement ColumnCache::getColumn(size_t pack_id, ColId column_id)
{
    if (auto iter = column_caches.find(pack_id); iter != column_caches.end())
    {
        auto & column_cache_entry = iter->second;
        auto & columns = column_cache_entry.columns;
        if (auto column_iter = columns.find(column_id); column_iter != columns.end())
        {
            auto & column = column_iter->second;
            return std::make_pair(column, std::make_pair(column_cache_entry.rows_offset, column_cache_entry.rows_count));
        }
    }
    throw Exception("Cannot find column in cache for pack id: " + std::to_string(pack_id) + " column id: " + std::to_string(column_id),
                    ErrorCodes::LOGICAL_ERROR);
}

bool ColumnCache::isPackInCache(PackId pack_id, ColId column_id)
{
    if (auto iter = column_caches.find(pack_id); iter != column_caches.end())
    {
        auto & columns = iter->second.columns;
        if (columns.find(column_id) != columns.end())
        {
            return true;
        }
    }
    return false;
}

} // namespace DM
} // namespace DB
