#pragma once

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
static const std::chrono::seconds DELTA_MERGE_GC_PERIOD(60);

class StoragePool : private boost::noncopyable
{
public:
    using Clock = std::chrono::system_clock;
    using Timepoint = Clock::time_point;
    using Duration = Clock::duration;
    using Seconds = std::chrono::seconds;

    StoragePool(const String & name, NamespaceId ns_id_, StoragePathPool & path_pool, const Context & global_ctx, const Settings & settings);

    void restore();

    NamespaceId getNamespaceId() const { return ns_id; }

    PageStoragePtr log() { return log_storage; }
    PageStoragePtr data() { return data_storage; }
    PageStoragePtr meta() { return meta_storage; }

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

    // Caller must cancel gc tasks before drop
    void drop();

    bool gc(const Settings & settings, const Seconds & try_gc_period = DELTA_MERGE_GC_PERIOD);

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

    const Context & global_context;

    friend class PageIdGenerator;
};

class PageIdGenerator : private boost::noncopyable
{
public:
    PageIdGenerator() = default;

    void restore(const StoragePool & storage_pool);

    PageId newDataPageIdForDTFile(StableDiskDelegator & delegator, const char * who);

    PageId maxMetaPageId() { return max_meta_page_id; }

    PageId newLogPageId() { return ++max_log_page_id; }
    PageId newMetaPageId() { return ++max_meta_page_id; }

private:
    std::atomic<PageId> max_log_page_id = 0;
    std::atomic<PageId> max_data_page_id = 0;
    std::atomic<PageId> max_meta_page_id = 0;
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
