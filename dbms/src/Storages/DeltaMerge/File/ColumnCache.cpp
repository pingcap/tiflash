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

#include <Storages/DeltaMerge/File/ColumnCache.h>

namespace DB::DM
{

RangeWithStrategys ColumnCache::getReadStrategy(size_t start_pack_idx, size_t pack_count, ColId column_id)
{
    PackRange target_range{start_pack_idx, start_pack_idx + pack_count};

    RangeWithStrategys range_and_strategys;
    range_and_strategys.reserve(pack_count);
    auto strategy = Strategy::Unknown;
    size_t range_start = 0;
    for (size_t cursor = target_range.first; cursor < target_range.second; ++cursor)
    {
        if (isPackInCache(cursor, column_id))
        {
            if (strategy == Strategy::Memory)
            {
                continue;
            }
            else if (strategy == Strategy::Disk)
            {
                range_and_strategys.emplace_back(PackRange{range_start, cursor}, Strategy::Disk);
            }
            range_start = cursor;
            strategy = Strategy::Memory;
        }
        else
        {
            if (strategy == Strategy::Memory)
            {
                range_and_strategys.emplace_back(PackRange{range_start, cursor}, Strategy::Memory);
            }
            else if (strategy == Strategy::Disk)
            {
                continue;
            }
            range_start = cursor;
            strategy = Strategy::Disk;
        }
    }
    range_and_strategys.emplace_back(PackRange{range_start, target_range.second}, strategy);
    range_and_strategys.shrink_to_fit();
    return range_and_strategys;
}

RangeWithStrategys ColumnCache::getReadStrategy(
    size_t start_pack_idx,
    size_t pack_count,
    const std::vector<size_t> & clean_read_pack_idx)
{
    PackRange target_range{start_pack_idx, start_pack_idx + pack_count};

    RangeWithStrategys range_and_strategys;
    range_and_strategys.reserve(pack_count);
    auto strategy = Strategy::Unknown;
    size_t range_start = 0;
    for (size_t cursor = target_range.first; cursor < target_range.second; ++cursor)
    {
        if (std::find(clean_read_pack_idx.cbegin(), clean_read_pack_idx.cend(), cursor) != clean_read_pack_idx.cend())
        {
            if (strategy == Strategy::Memory)
            {
                continue;
            }
            else if (strategy == Strategy::Disk)
            {
                range_and_strategys.emplace_back(PackRange{range_start, cursor}, Strategy::Disk);
            }
            range_start = cursor;
            strategy = Strategy::Memory;
        }
        else
        {
            if (strategy == Strategy::Memory)
            {
                range_and_strategys.emplace_back(PackRange{range_start, cursor}, Strategy::Memory);
            }
            else if (strategy == Strategy::Disk)
            {
                continue;
            }
            range_start = cursor;
            strategy = Strategy::Disk;
        }
    }
    range_and_strategys.emplace_back(PackRange{range_start, target_range.second}, strategy);
    range_and_strategys.shrink_to_fit();
    return range_and_strategys;
}

void ColumnCache::tryPutColumn(
    size_t pack_id,
    ColId column_id,
    const ColumnPtr & column,
    size_t rows_offset,
    size_t rows_count)
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
            return std::make_pair(
                column,
                std::make_pair(column_cache_entry.rows_offset, column_cache_entry.rows_count));
        }
    }
    throw Exception(
        "Cannot find column in cache for pack id: " + std::to_string(pack_id)
            + " column id: " + std::to_string(column_id),
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

} // namespace DB::DM
