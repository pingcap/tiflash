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

#include <Columns/IColumn.h>
#include <Common/Logger.h>
#include <Common/TiFlashMetrics.h>
#include <DataTypes/IDataType.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>

#include <memory>

namespace DB::DM
{
using PackId = size_t;

enum class ColumnCacheStatus
{
    ADD_COUNT = 0,
    ADD_STALE,
    GET_MISS,
    GET_PART,
    GET_HIT,
    GET_COPY,

    _TOTAL_COUNT, // NOLINT(bugprone-reserved-identifier)
};

class ColumnSharingCache
{
public:
    struct ColumnData
    {
        size_t pack_count{0};
        ColumnPtr col_data;
    };

    void add(size_t start_pack_id, size_t pack_count, ColumnPtr & col_data)
    {
        std::lock_guard lock(mtx);
        GET_METRIC(tiflash_storage_read_thread_counter, type_add_cache_succ).Increment();
        auto & value = packs[start_pack_id];
        if (value.pack_count < pack_count)
        {
            value.pack_count = pack_count;
            value.col_data = col_data;
        }
    }

    ColumnCacheStatus get(
        size_t start_pack_id,
        size_t pack_count,
        size_t read_rows,
        ColumnPtr & col_data,
        DataTypePtr data_type)
    {
        ColumnCacheStatus status;
        std::lock_guard lock(mtx);
        auto target = packs.find(start_pack_id);
        if (target == packs.end())
        {
            GET_METRIC(tiflash_storage_read_thread_counter, type_get_cache_miss).Increment();
            status = ColumnCacheStatus::GET_MISS;
        }
        else if (target->second.pack_count < pack_count)
        {
            GET_METRIC(tiflash_storage_read_thread_counter, type_get_cache_part).Increment();
            status = ColumnCacheStatus::GET_PART;
        }
        else if (target->second.pack_count == pack_count)
        {
            GET_METRIC(tiflash_storage_read_thread_counter, type_get_cache_hit).Increment();
            status = ColumnCacheStatus::GET_HIT;
            col_data = target->second.col_data;
        }
        else
        {
            GET_METRIC(tiflash_storage_read_thread_counter, type_get_cache_copy).Increment();
            status = ColumnCacheStatus::GET_COPY;
            auto column = data_type->createColumn();
            column->insertRangeFrom(*(target->second.col_data), 0, read_rows);
            col_data = std::move(column);
        }
        return status;
    }

    void del(size_t upper_start_pack_id)
    {
        std::lock_guard lock(mtx);
        for (auto itr = packs.begin(); itr != packs.end();)
        {
            if (itr->first + itr->second.pack_count <= upper_start_pack_id)
            {
                itr = packs.erase(itr);
            }
            else
            {
                break;
            }
        }
    }

private:
    std::mutex mtx;
    // start_pack_id -> <pack_count, col_data>
    std::map<PackId, ColumnData> packs;
};

// `ColumnSharingCacheMap` is a per DMFileReader cache.
// It store a ColumnSharingCache(std::map) for each column.
// When read threads are enable, each DMFileReader will be add to DMFileReaderPool,
// so we can find DMFileReaders of the same DMFile easily.
// Each DMFileReader will add the block that they read to other DMFileReaders' cache if the start_pack_id of the block
// is greater or equal than the next_pack_id of the DMFileReader —— This means that the DMFileReader maybe read the block later.
class ColumnSharingCacheMap
{
public:
    ColumnSharingCacheMap(const String & dmfile_name_, const ColumnDefines & cds, LoggerPtr & log_)
        : dmfile_name(dmfile_name_)
        , stats(static_cast<int>(ColumnCacheStatus::_TOTAL_COUNT))
        , log(log_)
    {
        for (const auto & cd : cds)
        {
            addColumn(cd.id);
        }
    }

    ~ColumnSharingCacheMap() { LOG_DEBUG(log, "dmfile {} stat {}", dmfile_name, statString()); }

    // `addStale` just do some statistics.
    void addStale()
    {
        GET_METRIC(tiflash_storage_read_thread_counter, type_add_cache_stale).Increment();
        stats[static_cast<int>(ColumnCacheStatus::ADD_STALE)].fetch_add(1, std::memory_order_relaxed);
    }

    void add(int64_t col_id, size_t start_pack_id, size_t pack_count, ColumnPtr & col_data)
    {
        stats[static_cast<int>(ColumnCacheStatus::ADD_COUNT)].fetch_add(1, std::memory_order_relaxed);
        auto itr = cols.find(col_id);
        if (itr == cols.end())
        {
            return;
        }
        itr->second.add(start_pack_id, pack_count, col_data);
    }

    bool get(
        int64_t col_id,
        size_t start_pack_id,
        size_t pack_count,
        size_t read_rows,
        ColumnPtr & col_data,
        DataTypePtr data_type)
    {
        auto status = ColumnCacheStatus::GET_MISS;
        auto itr = cols.find(col_id);
        if (itr != cols.end())
        {
            status = itr->second.get(start_pack_id, pack_count, read_rows, col_data, data_type);
        }
        stats[static_cast<int>(status)].fetch_add(1, std::memory_order_relaxed);
        return status == ColumnCacheStatus::GET_HIT || status == ColumnCacheStatus::GET_COPY;
    }

    // Each read operator of DMFileReader will advance the next_pack_id.
    // So we can delete packs if their pack_id is less than next_pack_id to save memory.
    void del(int64_t col_id, size_t upper_start_pack_id)
    {
        auto itr = cols.find(col_id);
        if (itr != cols.end())
        {
            itr->second.del(upper_start_pack_id);
        }
    }

private:
    void addColumn(int64_t col_id) { cols[col_id]; }
    std::string statString() const
    {
        auto add_count = stats[static_cast<int>(ColumnCacheStatus::ADD_COUNT)].load(std::memory_order_relaxed);
        auto add_stale = stats[static_cast<int>(ColumnCacheStatus::ADD_STALE)].load(std::memory_order_relaxed);
        auto get_miss = stats[static_cast<int>(ColumnCacheStatus::GET_MISS)].load(std::memory_order_relaxed);
        auto get_part = stats[static_cast<int>(ColumnCacheStatus::GET_PART)].load(std::memory_order_relaxed);
        auto get_hit = stats[static_cast<int>(ColumnCacheStatus::GET_HIT)].load(std::memory_order_relaxed);
        auto get_copy = stats[static_cast<int>(ColumnCacheStatus::GET_COPY)].load(std::memory_order_relaxed);
        auto add_total = add_count + add_stale;
        auto get_cached = get_hit + get_copy;
        auto get_total = get_miss + get_part + get_hit + get_copy;
        return fmt::format(
            "add_count={} add_stale={} add_ratio={} get_miss={} get_part={} get_hit={} get_copy={} cached_ratio={}",
            add_count,
            add_stale,
            add_total > 0 ? add_count * 1.0 / add_total : 0,
            get_miss,
            get_part,
            get_hit,
            get_copy,
            get_total > 0 ? get_cached * 1.0 / get_total : 0);
    }
    std::string dmfile_name;
    std::unordered_map<int64_t, ColumnSharingCache> cols;
    std::vector<std::atomic<int64_t>> stats;
    LoggerPtr log;
};

class DMFileReader;

class DMFileReaderPoolSharding
{
public:
    void add(const String & path, DMFileReader & reader);
    void del(const String & path, DMFileReader & reader);
    void set(
        const String & path,
        DMFileReader & from_reader,
        int64_t col_id,
        size_t start,
        size_t count,
        ColumnPtr & col);
    bool hasConcurrentReader(const String & path);
    // `get` is just for test.
    DMFileReader * get(const std::string & path);

private:
    std::mutex mtx;
    std::unordered_map<std::string, std::unordered_set<DMFileReader *>> readers;
};

// DMFileReaderPool holds all the DMFileReader objects, so we can easily find DMFileReader objects with the same DMFile ID.
// When a DMFileReader object successfully reads a column's packs, it will try to put these packs into other DMFileReader objects' cache.
class DMFileReaderPool
{
public:
    static DMFileReaderPool & instance();
    ~DMFileReaderPool() = default;
    DISALLOW_COPY_AND_MOVE(DMFileReaderPool);

    void add(DMFileReader & reader);
    void del(DMFileReader & reader);
    void set(DMFileReader & from_reader, int64_t col_id, size_t start, size_t count, ColumnPtr & col);
    bool hasConcurrentReader(DMFileReader & from_reader);
    // `get` is just for test.
    DMFileReader * get(const std::string & path);

private:
    DMFileReaderPool() = default;
    DMFileReaderPoolSharding & getSharding(const String & path);

private:
    std::array<DMFileReaderPoolSharding, 16> shardings;
};

} // namespace DB::DM
