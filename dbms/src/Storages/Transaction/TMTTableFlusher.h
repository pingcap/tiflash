#pragma once

#include <chrono>
#include <common/logger_useful.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

class Context;

class TMTTableFlusher
{
public:
    using Clock = std::chrono::steady_clock;

    struct Partition
    {
        UInt64 partition_id;
        Clock::time_point last_modified_time;
        size_t cached_rows;

        Partition() = default;

        Partition(UInt64 partition_id_, size_t cached_rows_) : partition_id(partition_id_),
            last_modified_time(Clock::now()), cached_rows(cached_rows_) {}

        void onPut(size_t put_rows)
        {
            last_modified_time = Clock::now();
            cached_rows += put_rows;
        }

        void onFlush()
        {
            last_modified_time = Clock::now();
            cached_rows = 0;
        }
    };

public:
    TMTTableFlusher(Context & context, TableID table_id, size_t flush_threshold_rows);

    void setFlushThresholdRows(size_t flush_threshold_rows);

    void onPutNotification(RegionPtr region, size_t put_rows);

    void tryAsyncFlush(size_t deadline_seconds);

private:
    void asyncFlush(UInt64 partition_id);
    void flush(UInt64 partition_id);

private:
    Context & context;

    std::atomic<TableID> table_id;
    std::atomic<size_t> flush_threshold_rows;

    std::unordered_map<UInt64, Partition> partitions;
    std::mutex mutex;

    Logger * log;
};

using TMTTableFlusherPtr = std::shared_ptr<TMTTableFlusher>;


class TMTTableFlushers
{
public:
    TMTTableFlushers(Context & context, size_t deadline_seconds_, size_t flush_threshold_rows_);
    virtual ~TMTTableFlushers();

    void setFlushThresholdRows(size_t flush_threshold_rows);
    void setDeadlineSeconds(size_t deadline_seconds);

    void onPutTryFlush(RegionPtr region);

    void dropRegionsInTable(TableID table_id);

private:
    // Get or create the specified table's partition flusher, the table should be created.
    TMTTableFlusherPtr getOrCreate(TableID table_id);

private:
    Context & context;

    std::atomic<size_t> deadline_seconds;
    std::atomic<size_t> flush_threshold_rows;

    std::thread interval_thread;
    std::atomic<bool> interval_thread_stopping;

    using FlusherMap = std::unordered_map<TableID, TMTTableFlusherPtr>;
    FlusherMap flushers;
    std::mutex mutex;
};

}
