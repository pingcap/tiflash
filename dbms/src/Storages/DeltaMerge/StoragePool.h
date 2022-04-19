// Copyright 2022 PingCAP, Ltd.
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

namespace DM
{
class StoragePool;
using StoragePoolPtr = std::shared_ptr<StoragePool>;

static const std::chrono::seconds DELTA_MERGE_GC_PERIOD(60);

enum class StoragePoolRunMode : UInt8
{
    ONLY_V2 = 1,
    ONLY_V3 = 2,
    MIX_MODE = 3,
};

class GlobalStoragePool : private boost::noncopyable
{
public:
    using Clock = std::chrono::system_clock;
    using Timepoint = Clock::time_point;
    using Seconds = std::chrono::seconds;

    // not thread safe
    static void init(const PathPool & path_pool, Context & global_ctx, const Settings & settings)
    {
        if (global_storage_pool != nullptr)
        {
            return;
        }

        try
        {
            global_storage_pool = std::make_shared<GlobalStoragePool>(path_pool, global_ctx, settings);
            global_storage_pool->restore();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            throw;
        }
    }

    static std::shared_ptr<GlobalStoragePool> getInstance()
    {
        return global_storage_pool;
    }

    GlobalStoragePool(const PathPool & path_pool, Context & global_ctx, const Settings & settings);

    ~GlobalStoragePool();

    void restore();

    PageStoragePtr log() const { return log_storage; }
    PageStoragePtr data() const { return data_storage; }
    PageStoragePtr meta() const { return meta_storage; }

private:
    // TODO: maybe more frequent gc for GlobalStoragePool?
    bool gc(const Settings & settings, const Seconds & try_gc_period = DELTA_MERGE_GC_PERIOD);

private:
    static std::shared_ptr<GlobalStoragePool> global_storage_pool;
    PageStoragePtr log_storage;
    PageStoragePtr data_storage;
    PageStoragePtr meta_storage;

    std::atomic<Timepoint> last_try_gc_time = Clock::now();

    std::mutex mutex;

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

    StoragePool(StoragePoolRunMode mode, NamespaceId ns_id_, const GlobalStoragePoolPtr & global_storage_pool, StoragePathPool & path_pool, Context & global_ctx, const String & name = "");

    void restore();

    ~StoragePool();

    NamespaceId getNamespaceId() const { return ns_id; }

    // TODO remove
    PageStoragePtr log() const { return log_storage_v2; }
    PageStoragePtr data() const { return data_storage_v2; }
    PageStoragePtr meta() const { return meta_storage_v2; }

    PageReaderPtr & logReader() { return log_storage_reader; }
    PageReaderPtr & dataReader() { return data_storage_reader; }
    PageReaderPtr & metaReader() { return meta_storage_reader; }

    // TODO change to two snapshot from both of v2 and v3
    PageReader newLogReader(ReadLimiterPtr read_limiter, bool snapshot_read, const String & tracing_id)
    {
        return PageReader(ns_id, log_storage_v2, snapshot_read ? log_storage_v2->getSnapshot(tracing_id) : nullptr, read_limiter);
    }
    PageReader newDataReader(ReadLimiterPtr read_limiter, bool snapshot_read, const String & tracing_id)
    {
        return PageReader(ns_id, data_storage_v2, snapshot_read ? data_storage_v2->getSnapshot(tracing_id) : nullptr, read_limiter);
    }
    PageReader newMetaReader(ReadLimiterPtr read_limiter, bool snapshot_read, const String & tracing_id)
    {
        return PageReader(ns_id, meta_storage_v2, snapshot_read ? meta_storage_v2->getSnapshot(tracing_id) : nullptr, read_limiter);
    }

    void enableGC();

    bool gc(const Settings & settings, const Seconds & try_gc_period = DELTA_MERGE_GC_PERIOD);

    void shutdown();

    // Caller must cancel gc tasks before drop
    void drop();

    PageId newDataPageIdForDTFile(StableDiskDelegator & delegator, const char * who);

    PageId maxMetaPageId() { return max_meta_page_id; }

    PageId newLogPageId() { return ++max_log_page_id; }
    PageId newMetaPageId() { return ++max_meta_page_id; }

private:
    StoragePoolRunMode run_mode = StoragePoolRunMode::ONLY_V2;

    // whether the three storage instance is owned by this StoragePool
    const NamespaceId ns_id;

    PageStoragePtr log_storage_v2;
    PageStoragePtr data_storage_v2;
    PageStoragePtr meta_storage_v2;

    PageStoragePtr log_storage_v3;
    PageStoragePtr data_storage_v3;
    PageStoragePtr meta_storage_v3;

    PageReaderPtr log_storage_reader;
    PageReaderPtr data_storage_reader;
    PageReaderPtr meta_storage_reader;

    std::atomic<Timepoint> last_try_gc_time = Clock::now();

    std::mutex mutex;

    Context & global_context;

    std::atomic<PageId> max_log_page_id = 0;
    std::atomic<PageId> max_data_page_id = 0;
    std::atomic<PageId> max_meta_page_id = 0;

    BackgroundProcessingPool::TaskHandle gc_handle = nullptr;
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
