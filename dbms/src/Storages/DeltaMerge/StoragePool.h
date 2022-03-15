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

private:
    // TODO: maybe more frequent gc for GlobalStoragePool?
    bool gc(const Settings & settings, const Seconds & try_gc_period = DELTA_MERGE_GC_PERIOD);

private:
    PageStoragePtr log_storage;
    PageStoragePtr data_storage;
    PageStoragePtr meta_storage;

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

    PageReader newLogReader(ReadLimiterPtr read_limiter, bool snapshot_read)
    {
        return PageReader(ns_id, log_storage, snapshot_read ? log_storage->getSnapshot() : nullptr, read_limiter);
    }
    PageReader newDataReader(ReadLimiterPtr read_limiter, bool snapshot_read)
    {
        return PageReader(ns_id, data_storage, snapshot_read ? data_storage->getSnapshot() : nullptr, read_limiter);
    }
    PageReader newMetaReader(ReadLimiterPtr read_limiter, bool snapshot_read)
    {
        return PageReader(ns_id, meta_storage, snapshot_read ? meta_storage->getSnapshot() : nullptr, read_limiter);
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
    NamespaceId ns_id;

    PageStoragePtr log_storage;
    PageStoragePtr data_storage;
    PageStoragePtr meta_storage;

    PageReader log_storage_reader;
    PageReader data_storage_reader;
    PageReader meta_storage_reader;

    std::atomic<Timepoint> last_try_gc_time = Clock::now();

    std::mutex mutex;

    Context & global_context;

    // whether the three storage instance is owned by this StoragePool
    bool owned_storage = false;

    std::atomic<PageId> max_log_page_id = 0;
    std::atomic<PageId> max_data_page_id = 0;
    std::atomic<PageId> max_meta_page_id = 0;

    BackgroundProcessingPool::TaskHandle gc_handle = nullptr;
};

struct StorageSnapshot : private boost::noncopyable
{
    StorageSnapshot(StoragePool & storage, ReadLimiterPtr read_limiter, bool snapshot_read = true)
        : log_reader(storage.newLogReader(read_limiter, snapshot_read))
        , data_reader(storage.newDataReader(read_limiter, snapshot_read))
        , meta_reader(storage.newMetaReader(read_limiter, snapshot_read))
    {}

    PageReader log_reader;
    PageReader data_reader;
    PageReader meta_reader;
};
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;


} // namespace DM
} // namespace DB
