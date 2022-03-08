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
class StoragePool;
using StoragePoolPtr = std::shared_ptr<StoragePool>;
static const std::chrono::seconds DELTA_MERGE_GC_PERIOD(60);

class StoragePool : private boost::noncopyable
{
public:
    using Clock = std::chrono::system_clock;
    using Timepoint = Clock::time_point;
    using Duration = Clock::duration;
    using Seconds = std::chrono::seconds;

    StoragePool(const String & name, StoragePathPool & path_pool, const Context & global_ctx, const Settings & settings);

    StoragePool(const String & name, const PathPool & path_pool, const Context & global_ctx, const Settings & settings);

    StoragePool(PageStoragePtr log_storage_, PageStoragePtr data_storage_, PageStoragePtr meta_storage_, const Context & global_ctx);

    void restore();

    PageStoragePtr log() { return log_storage; }
    PageStoragePtr data() { return data_storage; }
    PageStoragePtr meta() { return meta_storage; }

    // Caller must cancel gc tasks before drop
    // FIXME: the drop logic may not be appropriate for global StoragePool
    void drop();

    bool gc(const Settings & settings, const Seconds & try_gc_period = DELTA_MERGE_GC_PERIOD);

    PageId newDataPageIdForDTFile(StableDiskDelegator & delegator, const char * who);

    PageId maxMetaPageId() { return max_meta_page_id; }

    PageId newLogPageId() { return ++max_log_page_id; }
    PageId newMetaPageId() { return ++max_meta_page_id; }

private:
    PageStoragePtr log_storage;
    PageStoragePtr data_storage;
    PageStoragePtr meta_storage;

    std::atomic<Timepoint> last_try_gc_time = Clock::now();

    std::mutex mutex;

    const Context & global_context;

    bool initialized = false;

    std::atomic<PageId> max_log_page_id = 0;
    std::atomic<PageId> max_data_page_id = 0;
    std::atomic<PageId> max_meta_page_id = 0;
};

struct StorageSnapshot : private boost::noncopyable
{
    StorageSnapshot(StoragePool & storage, ReadLimiterPtr read_limiter, bool snapshot_read = true)
        : log_reader(storage.log(), snapshot_read ? storage.log()->getSnapshot() : nullptr, read_limiter)
        , data_reader(storage.data(), snapshot_read ? storage.data()->getSnapshot() : nullptr, read_limiter)
        , meta_reader(storage.meta(), snapshot_read ? storage.meta()->getSnapshot() : nullptr, read_limiter)
    {}

    PageReader log_reader;
    PageReader data_reader;
    PageReader meta_reader;
};
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;


} // namespace DM
} // namespace DB
