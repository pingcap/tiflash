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

#include <Common/CurrentMetrics.h>
#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/Settings_fwd.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool_fwd.h>
#include <Storages/KVStore/Types.h>
#include <Storages/Page/FileUsage.h>
#include <Storages/Page/PageStorage_fwd.h>

#include <atomic>
#include <chrono>

namespace DB
{
class WriteLimiter;
using WriteLimiterPtr = std::shared_ptr<WriteLimiter>;
class ReadLimiter;
using ReadLimiterPtr = std::shared_ptr<ReadLimiter>;

class StoragePathPool;
class PathPool;
class StableDiskDelegator;
class AsynchronousMetrics;
} // namespace DB

namespace DB::DM
{

class StoragePool : private boost::noncopyable
{
public:
    using Clock = std::chrono::system_clock;
    using Timepoint = Clock::time_point;
    using Seconds = std::chrono::seconds;

    StoragePool(
        Context & global_ctx,
        KeyspaceID keyspace_id_,
        NamespaceID table_id_,
        StoragePathPool & storage_path_pool_,
        const String & name);

    // For test
    StoragePool(
        Context & global_ctx,
        KeyspaceID keyspace_id_,
        NamespaceID table_id_,
        StoragePathPool & storage_path_pool_,
        GlobalPageIdAllocatorPtr page_id_allocator_,
        const String & name);

    PageStorageRunMode restore();

    ~StoragePool();

    KeyspaceID getKeyspaceID() const { return keyspace_id; }

    NamespaceID getTableID() const { return table_id; }

    PageStorageRunMode getPageStorageRunMode() const { return run_mode; }

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


    PageReaderPtr newLogReader(ReadLimiterPtr read_limiter, bool snapshot_read, const String & tracing_id);
    PageReaderPtr newLogReader(ReadLimiterPtr read_limiter, PageStorageSnapshotPtr & snapshot);

    PageReaderPtr newDataReader(ReadLimiterPtr read_limiter, bool snapshot_read, const String & tracing_id);

    PageReaderPtr newMetaReader(ReadLimiterPtr read_limiter, bool snapshot_read, const String & tracing_id);

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
    // For PageStorageRunMode::ONLY_V3/MIX_MODE, PageStorage is global(distinguish by ns_id for different table).
    // In order to avoid Page id from being reused (and cause troubles while restoring WAL from disk),
    // StoragePool will assign the max_log_page_id/max_meta_page_id/max_data_page_id by the global max id
    // regardless of ns_id while being restored. This causes the ids in a table to not be continuously incremented.

    PageIdU64 newDataPageIdForDTFile(StableDiskDelegator & delegator, const char * who) const;
    PageIdU64 newLogPageId() const;
    PageIdU64 newMetaPageId() const;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    bool doV2Gc(const Settings & settings) const;

    void forceTransformMetaV2toV3();
    void forceTransformDataV2toV3();

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    LoggerPtr logger;

    PageStorageRunMode run_mode;
    const KeyspaceID keyspace_id;
    const NamespaceID table_id;

    StoragePathPool & storage_path_pool;

    PageStoragePtr log_storage_v2;
    PageStoragePtr data_storage_v2;
    PageStoragePtr meta_storage_v2;

    PageStoragePtr log_storage_v3;
    PageStoragePtr data_storage_v3;
    PageStoragePtr meta_storage_v3;

    UniversalPageStoragePtr uni_ps;

    PageReaderPtr log_storage_reader;
    PageReaderPtr data_storage_reader;
    PageReaderPtr meta_storage_reader;

    PageWriterPtr log_storage_writer;
    PageWriterPtr data_storage_writer;
    PageWriterPtr meta_storage_writer;

    std::atomic<Timepoint> last_try_gc_time = Clock::now();

    std::mutex mutex;

    Context & global_context;

    GlobalPageIdAllocatorPtr global_id_allocator;

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

    PageReaderPtr log_reader;
    PageReaderPtr data_reader;
    PageReaderPtr meta_reader;
};


} // namespace DB::DM
