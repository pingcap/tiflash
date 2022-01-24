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
    using Clock     = std::chrono::system_clock;
    using Timepoint = Clock::time_point;
    using Duration  = Clock::duration;
    using Seconds   = std::chrono::seconds;

    StoragePool(const String & name, StoragePathPool & path_pool, const Context & global_ctx, const Settings & settings);

    void restore();

    PageId maxLogPageId() { return max_log_page_id; }
    PageId maxDataPageId() { return max_data_page_id; }
    PageId maxMetaPageId() { return max_meta_page_id; }

    PageId newLogPageId() { return ++max_log_page_id; }
    PageId newMetaPageId() { return ++max_meta_page_id; }

    PageId newDataPageIdForDTFile(StableDiskDelegator & delegator, const char * who);

    PageStorage & log() { return log_storage; }
    PageStorage & data() { return data_storage; }
    PageStorage & meta() { return meta_storage; }

    // Caller must cancel gc tasks before drop
    void drop();

    bool gc(const Settings & settings, const Seconds & try_gc_period = DELTA_MERGE_GC_PERIOD);

private:
    PageStorage log_storage;
    PageStorage data_storage;
    PageStorage meta_storage;

    std::atomic<PageId> max_log_page_id;
    std::atomic<PageId> max_data_page_id;
    std::atomic<PageId> max_meta_page_id;

    std::atomic<Timepoint> last_try_gc_time = Clock::now();

    std::mutex mutex;

    const Context & global_context;
};

struct StorageSnapshot : private boost::noncopyable
{
    StorageSnapshot(StoragePool & storage, bool snapshot_read = true)
        : log_reader(storage.log(), snapshot_read ? storage.log().getSnapshot() : nullptr),
          data_reader(storage.data(), snapshot_read ? storage.data().getSnapshot() : nullptr),
          meta_reader(storage.meta(), snapshot_read ? storage.meta().getSnapshot() : nullptr)
    {
    }

    PageReader log_reader;
    PageReader data_reader;
    PageReader meta_reader;
};
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

} // namespace DM
} // namespace DB
