#include <Common/Allocator.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>
#include <Databases/IDatabase.h>
#include <IO/UncompressedCache.h>
#include <Interpreters/AsynchronousMetrics.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/MarkCache.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageMergeTree.h>
#include <common/config_common.h>

#include <chrono>

#if USE_TCMALLOC
#include <gperftools/malloc_extension.h>

/// Initializing malloc extension in global constructor as required.
struct MallocExtensionInitializer
{
    MallocExtensionInitializer() { MallocExtension::Initialize(); }
} malloc_extension_initializer;
#endif

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

#if USE_MIMALLOC
#include <mimalloc.h>
#endif

namespace DB
{

AsynchronousMetrics::~AsynchronousMetrics()
{
    try
    {
        {
            std::lock_guard<std::mutex> lock{wait_mutex};
            quit = true;
        }

        wait_cond.notify_one();
        thread.join();
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


AsynchronousMetrics::Container AsynchronousMetrics::getValues() const
{
    std::lock_guard<std::mutex> lock{container_mutex};
    return container;
}


void AsynchronousMetrics::set(const std::string & name, Value value)
{
    std::lock_guard<std::mutex> lock{container_mutex};
    container[name] = value;
}


void AsynchronousMetrics::run()
{
    setThreadName("AsyncMetrics");

    std::unique_lock<std::mutex> lock{wait_mutex};

    /// Next minute + 30 seconds. To be distant with moment of transmission of metrics, see MetricsTransmitter.
    const auto get_next_minute = [] {
        return std::chrono::time_point_cast<std::chrono::minutes, std::chrono::system_clock>(
                   std::chrono::system_clock::now() + std::chrono::minutes(1))
            + std::chrono::seconds(30);
    };

    while (true)
    {
        if (wait_cond.wait_until(lock, get_next_minute(), [this] { return quit; }))
            break;

        try
        {
            update();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}


template <typename Max, typename T>
static void calculateMax(Max & max, T x)
{
    if (Max(x) > max)
        max = x;
}

template <typename Max, typename Sum, typename T>
static void calculateMaxAndSum(Max & max, Sum & sum, T x)
{
    sum += x;
    if (Max(x) > max)
        max = x;
}


void AsynchronousMetrics::update()
{
    {
        if (auto mark_cache = context.getMarkCache())
        {
            set("MarkCacheBytes", mark_cache->weight());
            set("MarkCacheFiles", mark_cache->count());
        }
    }

    {
        if (auto uncompressed_cache = context.getUncompressedCache())
        {
            set("UncompressedCacheBytes", uncompressed_cache->weight());
            set("UncompressedCacheCells", uncompressed_cache->count());
        }
    }

    set("Uptime", context.getUptimeSeconds());

    {
        auto databases = context.getDatabases();

        double max_dt_stable_oldest_snapshot_lifetime = 0.0;
        double max_dt_delta_oldest_snapshot_lifetime = 0.0;
        double max_dt_meta_oldest_snapshot_lifetime = 0.0;
        size_t max_dt_background_tasks_length = 0;

        size_t max_part_count_for_partition = 0;

        for (const auto & db : databases)
        {
            for (auto iterator = db.second->getIterator(context); iterator->isValid(); iterator->next())
            {
                auto & table = iterator->table();

                if (auto dt_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(table); dt_storage)
                {
                    auto stat = dt_storage->getStore()->getStat();
                    calculateMax(max_dt_stable_oldest_snapshot_lifetime, stat.storage_stable_oldest_snapshot_lifetime);
                    calculateMax(max_dt_delta_oldest_snapshot_lifetime, stat.storage_delta_oldest_snapshot_lifetime);
                    calculateMax(max_dt_meta_oldest_snapshot_lifetime, stat.storage_meta_oldest_snapshot_lifetime);
                    calculateMax(max_dt_background_tasks_length, stat.background_tasks_length);
                }
                else if (StorageMergeTree * table_merge_tree = dynamic_cast<StorageMergeTree *>(table.get()); table_merge_tree)
                {
                    calculateMax(max_part_count_for_partition, table_merge_tree->getData().getMaxPartsCountForPartition());
                }
            }
        }

        set("MaxDTStableOldestSnapshotLifetime", max_dt_stable_oldest_snapshot_lifetime);
        set("MaxDTDeltaOldestSnapshotLifetime", max_dt_delta_oldest_snapshot_lifetime);
        set("MaxDTMetaOldestSnapshotLifetime", max_dt_meta_oldest_snapshot_lifetime);
        set("MaxDTBackgroundTasksLength", max_dt_background_tasks_length);
        set("MaxPartCountForPartition", max_part_count_for_partition);
    }

#if USE_TCMALLOC
    {
        /// tcmalloc related metrics. Remove if you switch to different allocator.

        MallocExtension & malloc_extension = *MallocExtension::instance();

        auto malloc_metrics = {
            "generic.current_allocated_bytes",
            "generic.heap_size",
            "tcmalloc.current_total_thread_cache_bytes",
            "tcmalloc.central_cache_free_bytes",
            "tcmalloc.transfer_cache_free_bytes",
            "tcmalloc.thread_cache_free_bytes",
            "tcmalloc.pageheap_free_bytes",
            "tcmalloc.pageheap_unmapped_bytes",
        };

        for (auto malloc_metric : malloc_metrics)
        {
            size_t value = 0;
            if (malloc_extension.GetNumericProperty(malloc_metric, &value))
                set(malloc_metric, value);
        }
    }
#endif

#if USE_MIMALLOC
#define MI_STATS_SET(X) set("mimalloc." #X, X)

    {
        size_t elapsed_msecs;
        size_t user_msecs;
        size_t system_msecs;
        size_t current_rss;
        size_t peak_rss;
        size_t current_commit;
        size_t peak_commit;
        size_t page_faults;
        mi_process_info(&elapsed_msecs, &user_msecs, &system_msecs, &current_rss, &peak_rss, &current_commit, &peak_commit, &page_faults);
        MI_STATS_SET(elapsed_msecs);
        MI_STATS_SET(user_msecs);
        MI_STATS_SET(system_msecs);
        MI_STATS_SET(current_rss);
        MI_STATS_SET(peak_rss);
        MI_STATS_SET(current_commit);
        MI_STATS_SET(peak_commit);
        MI_STATS_SET(page_faults);
    };
#undef MI_STATS_SET
#endif

#if USE_JEMALLOC
    {
#define FOR_EACH_METRIC(M)                     \
    M("allocated", size_t)                     \
    M("active", size_t)                        \
    M("metadata", size_t)                      \
    M("metadata_thp", size_t)                  \
    M("resident", size_t)                      \
    M("mapped", size_t)                        \
    M("retained", size_t)                      \
    M("background_thread.num_threads", size_t) \
    M("background_thread.num_runs", uint64_t)  \
    M("background_thread.run_interval", uint64_t)

#define GET_METRIC(NAME, TYPE)                             \
    do                                                     \
    {                                                      \
        TYPE value{};                                      \
        size_t size = sizeof(value);                       \
        mallctl("stats." NAME, &value, &size, nullptr, 0); \
        set("jemalloc." NAME, value);                      \
    } while (0);

        FOR_EACH_METRIC(GET_METRIC);

#undef GET_METRIC
#undef FOR_EACH_METRIC
    }
#endif


    /// Add more metrics as you wish.
    set("mmap.alive", DB::allocator_mmap_counter.load(std::memory_order_relaxed));
}


} // namespace DB
