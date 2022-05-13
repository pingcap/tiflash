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

#include <Poco/Logger.h>
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

class GlobalStoragePool : private boost::noncopyable
{
public:
    using Clock = std::chrono::system_clock;
    using Timepoint = Clock::time_point;
    using Seconds = std::chrono::seconds;

    GlobalStoragePool(const PathPool & path_pool, Context & global_ctx, const Settings & settings);

    ~GlobalStoragePool();

    void restore();

    friend class StoragePool;

    PageId getLogMaxId(NamespaceId ns_id) const
    {
        PageId max_log_page_id = 0;
        if (const auto & it = log_max_ids.find(ns_id); it != log_max_ids.end())
        {
            max_log_page_id = it->second;
        }

        return max_log_page_id;
    }

    PageId getDataMaxId(NamespaceId ns_id) const
    {
        PageId max_data_page_id = 0;
        if (const auto & it = data_max_ids.find(ns_id); it != data_max_ids.end())
        {
            max_data_page_id = it->second;
        }

        return max_data_page_id;
    }

    PageId getMetaMaxId(NamespaceId ns_id) const
    {
        PageId max_meta_page_id = 0;
        if (const auto & it = meta_max_ids.find(ns_id); it != meta_max_ids.end())
        {
            max_meta_page_id = it->second;
        }

        return max_meta_page_id;
    }

    // GC immediately
    // Only used on dbgFuncMisc
    bool gc();

private:
    bool gc(const Settings & settings, bool immediately = false, const Seconds & try_gc_period = DELTA_MERGE_GC_PERIOD);

private:
    PageStoragePtr log_storage;
    PageStoragePtr data_storage;
    PageStoragePtr meta_storage;

    std::map<NamespaceId, PageId> log_max_ids;
    std::map<NamespaceId, PageId> data_max_ids;
    std::map<NamespaceId, PageId> meta_max_ids;

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

    StoragePool(Context & global_ctx, NamespaceId ns_id_, StoragePathPool & path_pool, const String & name = "");

    PageStorageRunMode restore();

    ~StoragePool();

    NamespaceId getNamespaceId() const { return ns_id; }

    PageStorageRunMode getPageStorageRunMode()
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

    void enableGC();

    void dataRegisterExternalPagesCallbacks(const ExternalPageCallbacks & callbacks);

    void dataUnregisterExternalPagesCallbacks(NamespaceId ns_id);

    bool gc(const Settings & settings, const Seconds & try_gc_period = DELTA_MERGE_GC_PERIOD);

    void shutdown();

    // Caller must cancel gc tasks before drop
    void drop();

    PageId newDataPageIdForDTFile(StableDiskDelegator & delegator, const char * who);

    PageId maxMetaPageId() { return max_meta_page_id; }
    PageId newLogPageId() { return ++max_log_page_id; }
    PageId newMetaPageId() { return ++max_meta_page_id; }
#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    bool doV2Gc(const Settings & settings);

    void forceTransformMetaV2toV3();

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    LoggerPtr logger;

    PageStorageRunMode run_mode;

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
