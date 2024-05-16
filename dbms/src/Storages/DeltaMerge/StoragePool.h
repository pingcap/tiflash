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

#include <Storages/BackgroundProcessingPool.h>
#include <Storages/Page/FileUsage.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/PathPool.h>

#include <atomic>
#include <chrono>

namespace DB
{
struct Settings;
class Context;
class StoragePathPool;
class StableDiskDelegator;
class AsynchronousMetrics;

namespace DM
{
class StoragePool;
using StoragePoolPtr = std::shared_ptr<StoragePool>;

static constexpr std::chrono::seconds DELTA_MERGE_GC_PERIOD(60);

class GlobalStoragePool : private boost::noncopyable
{
public:
    using Clock = std::chrono::system_clock;
    using Timepoint = Clock::time_point;
    using Seconds = std::chrono::seconds;

    GlobalStoragePool(const PathPool & path_pool, Context & global_ctx, const Settings & settings);

    ~GlobalStoragePool();

    void restore();

    void shutdown();

    friend class StoragePool;
    friend class ::DB::AsynchronousMetrics;

    // GC immediately
    // Only used on dbgFuncMisc
    bool gc();

    FileUsageStatistics getLogFileUsage() const;

private:
    bool gc(const Settings & settings, bool immediately = false, const Seconds & try_gc_period = DELTA_MERGE_GC_PERIOD);

private:
    PageStoragePtr log_storage;
    PageStoragePtr data_storage;
    PageStoragePtr meta_storage;

    std::atomic<Timepoint> last_try_gc_time = Clock::now();

    Context & global_context;
    BackgroundProcessingPool::TaskHandle gc_handle;
};
using GlobalStoragePoolPtr = std::shared_ptr<GlobalStoragePool>;

class StoragePool : private boost::noncopyable
{
public:
    using Clock = std::chrono::system_clock;
    using Timepoint = Clock::time_point;
    using Seconds = std::chrono::seconds;

<<<<<<< HEAD
    StoragePool(Context & global_ctx, NamespaceId ns_id_, StoragePathPool & storage_path_pool_, const String & name = "");
=======
    StoragePool(Context & global_ctx, KeyspaceID keyspace_id_, NamespaceID table_id_, StoragePathPool & storage_path_pool_, const String & name = "");
>>>>>>> 1b6cc860f9 (Storage: Fix page_id being mis-reuse when upgrade from cluster < 6.5 (#9041) (release-7.1) (#9048))

    PageStorageRunMode restore();

    ~StoragePool();

<<<<<<< HEAD
    NamespaceId getNamespaceId() const { return ns_id; }
=======
    KeyspaceID getKeyspaceID() const { return keyspace_id; }

    NamespaceID getNamespaceID() const { return table_id; }
>>>>>>> 1b6cc860f9 (Storage: Fix page_id being mis-reuse when upgrade from cluster < 6.5 (#9041) (release-7.1) (#9048))

    PageStorageRunMode getPageStorageRunMode() const
    {
        return run_mode;
    }

    PageReaderPtr & logReader()
    {
        assert(log_storage_reader);
        return log_storage_reader;
    }

    PageReaderPtr & dataReader()
    {
        assert(data_storage_reader);
        return data_storage_reader;
    }

    PageReaderPtr & metaReader()
    {
        assert(meta_storage_reader);
        return meta_storage_reader;
    }

    PageWriterPtr & logWriter()
    {
        assert(log_storage_writer);
        return log_storage_writer;
    }

    PageWriterPtr & dataWriter()
    {
        assert(data_storage_writer);
        return data_storage_writer;
    }

    PageWriterPtr & metaWriter()
    {
        assert(meta_storage_writer);
        return meta_storage_writer;
    }


    PageReader newLogReader(ReadLimiterPtr read_limiter, bool snapshot_read, const String & tracing_id);
    PageReader newLogReader(ReadLimiterPtr read_limiter, PageStorage::SnapshotPtr & snapshot);

    PageReader newDataReader(ReadLimiterPtr read_limiter, bool snapshot_read, const String & tracing_id);
    PageReader newDataReader(ReadLimiterPtr read_limiter, PageStorage::SnapshotPtr & snapshot);

    PageReader newMetaReader(ReadLimiterPtr read_limiter, bool snapshot_read, const String & tracing_id);
    PageReader newMetaReader(ReadLimiterPtr read_limiter, PageStorage::SnapshotPtr & snapshot);

    // Register the clean up DMFiles callbacks to PageStorage.
    // The callbacks will be unregister when `shutdown` is called.
    void startup(ExternalPageCallbacks && callbacks);

    // Shutdown the gc handle and DMFile callbacks
    void shutdown();

    bool gc(const Settings & settings, const Seconds & try_gc_period = DELTA_MERGE_GC_PERIOD);

    // Caller must cancel gc tasks before drop
    void drop();

    // For function `newLogPageId`,`newMetaPageId`,`newDataPageIdForDTFile`:
    // For PageStorageRunMode::ONLY_V2, every table have its own three PageStorage (meta/data/log).
    // So these functions return the Page id starts from 1 and is continuously incremented.
    // For PageStorageRunMode::ONLY_V3/MIX_MODE, PageStorage is global(distinguish by table_id for different table).
    // In order to avoid Page id from being reused (and cause troubles while restoring WAL from disk),
    // StoragePool will assign the max_log_page_id/max_meta_page_id/max_data_page_id by the global max id
    // regardless of table_id while being restored. This causes the ids in a table to not be continuously incremented.

    PageId newDataPageIdForDTFile(StableDiskDelegator & delegator, const char * who);
    PageId newLogPageId() { return ++max_log_page_id; }
    PageId newMetaPageId() { return ++max_meta_page_id; }

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    bool doV2Gc(const Settings & settings);

    void forceTransformMetaV2toV3();
    void forceTransformDataV2toV3();

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    LoggerPtr logger;

    PageStorageRunMode run_mode;

<<<<<<< HEAD
    // whether the three storage instance is owned by this StoragePool
    const NamespaceId ns_id;
=======
    const KeyspaceID keyspace_id;
    const NamespaceID table_id;
>>>>>>> 1b6cc860f9 (Storage: Fix page_id being mis-reuse when upgrade from cluster < 6.5 (#9041) (release-7.1) (#9048))

    StoragePathPool & storage_path_pool;

    PageStoragePtr log_storage_v2;
    PageStoragePtr data_storage_v2;
    PageStoragePtr meta_storage_v2;

    PageStoragePtr log_storage_v3;
    PageStoragePtr data_storage_v3;
    PageStoragePtr meta_storage_v3;

    PageReaderPtr log_storage_reader;
    PageReaderPtr data_storage_reader;
    PageReaderPtr meta_storage_reader;

    PageWriterPtr log_storage_writer;
    PageWriterPtr data_storage_writer;
    PageWriterPtr meta_storage_writer;

    std::atomic<Timepoint> last_try_gc_time = Clock::now();

    std::mutex mutex;

    Context & global_context;

    std::atomic<PageId> max_log_page_id = 0;
    std::atomic<PageId> max_data_page_id = 0;
    std::atomic<PageId> max_meta_page_id = 0;

    BackgroundProcessingPool::TaskHandle gc_handle = nullptr;

    CurrentMetrics::Increment storage_pool_metrics;
};

struct StorageSnapshot : private boost::noncopyable
{
    StorageSnapshot(StoragePool & storage, ReadLimiterPtr read_limiter, const String & tracing_id, bool snapshot_read)
        : log_reader(storage.newLogReader(read_limiter, snapshot_read, tracing_id))
        , data_reader(storage.newDataReader(read_limiter, snapshot_read, tracing_id))
        , meta_reader(storage.newMetaReader(read_limiter, snapshot_read, tracing_id))
    {}

    PageReader log_reader;
    PageReader data_reader;
    PageReader meta_reader;
};
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;


} // namespace DM
} // namespace DB
