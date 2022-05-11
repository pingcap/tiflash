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
class GlobalStoragePool;
using GlobalStoragePoolPtr = std::shared_ptr<GlobalStoragePool>;

static const std::chrono::seconds DELTA_MERGE_GC_PERIOD(60);

class GlobalStoragePool : private boost::noncopyable
{
public:
    using Clock = std::chrono::system_clock;
    using Timepoint = Clock::time_point;
    using Seconds = std::chrono::seconds;

    GlobalStoragePool(const PathPool & path_pool, Context & global_ctx, const Settings & settings);

    void restore();

    ~GlobalStoragePool();

    PageStoragePtr log() const { return log_storage; }
    PageStoragePtr data() const { return data_storage; }
    PageStoragePtr meta() const { return meta_storage; }

    std::map<NamespaceId, PageId> getLogMaxIds() const
    {
        return log_max_ids;
    }

    std::map<NamespaceId, PageId> getDataMaxIds() const
    {
        return data_max_ids;
    }

    std::map<NamespaceId, PageId> getMetaMaxIds() const
    {
        return meta_max_ids;
    }

private:
    // TODO: maybe more frequent gc for GlobalStoragePool?
    bool gc(const Settings & settings, const Seconds & try_gc_period = DELTA_MERGE_GC_PERIOD);

private:
    PageStoragePtr log_storage;
    PageStoragePtr data_storage;
    PageStoragePtr meta_storage;

    std::map<NamespaceId, PageId> log_max_ids;
    std::map<NamespaceId, PageId> data_max_ids;
    std::map<NamespaceId, PageId> meta_max_ids;

    std::atomic<Timepoint> last_try_gc_time = Clock::now();

    std::mutex mutex;

    Context & global_context;
    BackgroundProcessingPool::TaskHandle gc_handle;
};

class StoragePool : private boost::noncopyable
{
public:
    using Clock = std::chrono::system_clock;
    using Timepoint = Clock::time_point;
    using Seconds = std::chrono::seconds;

    StoragePool(const String & name, NamespaceId ns_id_, StoragePathPool & path_pool, Context & global_ctx, const Settings & settings);

    StoragePool(NamespaceId ns_id_, const GlobalStoragePool & global_storage_pool, Context & global_ctx);

    void restore();

    ~StoragePool();

    NamespaceId getNamespaceId() const { return ns_id; }

    PageStoragePtr log() const { return log_storage; }
    PageStoragePtr data() const { return data_storage; }
    PageStoragePtr meta() const { return meta_storage; }

    PageReader & logReader() { return log_storage_reader; }
    PageReader & dataReader() { return data_storage_reader; }
    PageReader & metaReader() { return meta_storage_reader; }

    PageReader newLogReader(ReadLimiterPtr read_limiter, bool snapshot_read, const String & tracing_id)
    {
        return PageReader(ns_id, log_storage, snapshot_read ? log_storage->getSnapshot(tracing_id) : nullptr, read_limiter);
    }
    PageReader newDataReader(ReadLimiterPtr read_limiter, bool snapshot_read, const String & tracing_id)
    {
        return PageReader(ns_id, data_storage, snapshot_read ? data_storage->getSnapshot(tracing_id) : nullptr, read_limiter);
    }
    PageReader newMetaReader(ReadLimiterPtr read_limiter, bool snapshot_read, const String & tracing_id)
    {
        return PageReader(ns_id, meta_storage, snapshot_read ? meta_storage->getSnapshot(tracing_id) : nullptr, read_limiter);
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
    // whether the three storage instance is owned by this StoragePool
    const bool owned_storage = false;
    const NamespaceId ns_id;

    const PageStoragePtr log_storage;
    const PageStoragePtr data_storage;
    const PageStoragePtr meta_storage;

    PageReader log_storage_reader;
    PageReader data_storage_reader;
    PageReader meta_storage_reader;

    std::atomic<Timepoint> last_try_gc_time = Clock::now();

    std::mutex mutex;

    Context & global_context;

    // TBD: Will be replaced GlobalPathPoolPtr after mix mode ptr ready
    std::map<NamespaceId, PageId> v3_log_max_ids;
    std::map<NamespaceId, PageId> v3_data_max_ids;
    std::map<NamespaceId, PageId> v3_meta_max_ids;

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
