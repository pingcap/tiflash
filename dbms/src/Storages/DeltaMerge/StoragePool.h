#pragma once

#include <atomic>
#include <chrono>

#include <Storages/Page/PageStorage.h>

namespace DB
{

static const std::chrono::seconds DELTA_MERGE_GC_PERIOD(60);

class StoragePool
{
public:
    using Clock     = std::chrono::system_clock;
    using Timepoint = Clock::time_point;
    using Duration  = Clock::duration;
    using Seconds   = std::chrono::seconds;

    StoragePool(const String & path);

    PageId maxLogPageId() { return max_log_page_id; }
    PageId maxDataPageId() { return max_data_page_id; }
    PageId maxMetaPageId() { return max_meta_page_id; }

    PageId newLogPageId() { return ++max_log_page_id; }
    PageId newDataPageId() { return ++max_data_page_id; }
    PageId newMetaPageId() { return ++max_meta_page_id; }

    PageStorage & log() { return log_storage; }
    PageStorage & data() { return data_storage; }
    PageStorage & meta() { return meta_storage; }

    bool gc(const Seconds try_gc_period = DELTA_MERGE_GC_PERIOD);

private:
    std::atomic<PageId> max_log_page_id;
    std::atomic<PageId> max_data_page_id;
    std::atomic<PageId> max_meta_page_id;

    PageStorage log_storage;
    PageStorage data_storage;
    PageStorage meta_storage;

    std::atomic<Timepoint> last_try_gc_time = Clock::now();

    std::mutex mutex;
};
} // namespace DB