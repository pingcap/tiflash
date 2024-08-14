// Copyright 2023 PingCAP, Inc.
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

#include <Common/Allocator.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>
#include <Core/TiFlashDisaggregatedMode.h>
#include <Databases/IDatabase.h>
#include <Interpreters/AsynchronousMetrics.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/StoragePool/GlobalStoragePool.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/MarkCache.h>
#include <Storages/Page/FileUsage.h>
#include <Storages/Page/PageConstants.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/PageStorageMemorySummary.h>
#include <Storages/Page/V3/Universal/UniversalPageStorageService.h>
#include <Storages/StorageDeltaMerge.h>
#include <common/config_common.h>

#include <chrono>

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
            std::lock_guard lock{wait_mutex};
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
    std::lock_guard lock{container_mutex};
    return container;
}


void AsynchronousMetrics::set(const std::string & name, Value value)
{
    std::lock_guard lock{container_mutex};
    container[name] = value;
}


void AsynchronousMetrics::run()
{
    setThreadName("AsyncMetrics");

    std::unique_lock lock{wait_mutex};

    /// Next minute + 30 seconds. To be distant with moment of transmission of metrics, see MetricsTransmitter.
    const auto get_next_minute = [] {
        return std::chrono::time_point_cast<std::chrono::minutes, std::chrono::steady_clock>(
                   std::chrono::steady_clock::now() + std::chrono::minutes(1))
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
    if (static_cast<Max>(x) > max)
        max = x;
}

template <typename Max, typename Sum, typename T>
static void calculateMaxAndSum(Max & max, Sum & sum, T x)
{
    sum += x;
    if (static_cast<Max>(x) > max)
        max = x;
}

FileUsageStatistics AsynchronousMetrics::getPageStorageFileUsage()
{
    FileUsageStatistics usage;
    switch (context.getSharedContextDisagg()->disaggregated_mode)
    {
    case DisaggregatedMode::None:
    {
        if (auto uni_ps = context.tryGetWriteNodePageStorage(); uni_ps != nullptr)
        {
            /// When format_version=5 is enabled, then all data are stored in the `uni_ps`
            usage.merge(uni_ps->getFileUsageStatistics());
        }
        else
        {
            /// When format_version < 5, then there are multiple PageStorage instances

            // Get from RegionPersister
            auto & tmt = context.getTMTContext();
            auto & kvstore = tmt.getKVStore();
            usage = kvstore->getFileUsageStatistics();

            // Get the blob file status from all PS V3 instances
            if (auto global_storage_pool = context.getGlobalStoragePool(); global_storage_pool != nullptr)
            {
                const auto log_usage = global_storage_pool->log_storage->getFileUsageStatistics();
                const auto meta_usage = global_storage_pool->meta_storage->getFileUsageStatistics();
                const auto data_usage = global_storage_pool->data_storage->getFileUsageStatistics();

                usage.merge(log_usage).merge(meta_usage).merge(data_usage);
            }
        }
        break;
    }
    case DisaggregatedMode::Storage:
    {
        // disagg write node, all data are stored in the `uni_ps`
        if (auto uni_ps = context.getWriteNodePageStorage(); uni_ps != nullptr)
        {
            usage.merge(uni_ps->getFileUsageStatistics());
        }
        break;
    }
    case DisaggregatedMode::Compute:
    {
        // disagg compute node without auto-scaler, the proxy data are stored in the `uni_ps`
        if (auto uni_ps = context.tryGetWriteNodePageStorage(); uni_ps != nullptr)
        {
            usage.merge(uni_ps->getFileUsageStatistics());
        }
        // disagg compute node, all cache page data are stored in the `ps_cache`
        if (auto ps_cache = context.getSharedContextDisagg()->rn_page_cache_storage; ps_cache != nullptr)
        {
            usage.merge(ps_cache->getUniversalPageStorage()->getFileUsageStatistics());
        }
        break;
    }
    }

    return usage;
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
        if (auto min_max_cache = context.getMinMaxIndexCache())
        {
            set("MinMaxIndexCacheBytes", min_max_cache->weight());
            set("MinMaxIndexFiles", min_max_cache->count());
        }
    }

    {
        if (auto rn_delta_index_cache = context.getSharedContextDisagg()->rn_delta_index_cache)
        {
            set("RNDeltaIndexCacheBytes", rn_delta_index_cache->getCacheWeight());
            set("RNDeltaIndexFiles", rn_delta_index_cache->getCacheCount());
        }
    }


    set("Uptime", context.getUptimeSeconds());

    {
        // Get the snapshot status from all delta tree tables
        auto databases = context.getDatabases();

        double max_dt_stable_oldest_snapshot_lifetime = 0.0;
        double max_dt_delta_oldest_snapshot_lifetime = 0.0;
        double max_dt_meta_oldest_snapshot_lifetime = 0.0;
        size_t max_dt_background_tasks_length = 0;

        for (const auto & db : databases)
        {
            for (auto iterator = db.second->getIterator(context); iterator->isValid(); iterator->next())
            {
                auto & table = iterator->table();

                if (auto dt_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(table); dt_storage)
                {
                    if (auto store = dt_storage->getStoreIfInited(); store)
                    {
                        const auto stat = store->getStoreStats();
                        if (context.getPageStorageRunMode() == PageStorageRunMode::ONLY_V2)
                        {
                            calculateMax(
                                max_dt_stable_oldest_snapshot_lifetime,
                                stat.storage_stable_oldest_snapshot_lifetime);
                            calculateMax(
                                max_dt_delta_oldest_snapshot_lifetime,
                                stat.storage_delta_oldest_snapshot_lifetime);
                            calculateMax(
                                max_dt_meta_oldest_snapshot_lifetime,
                                stat.storage_meta_oldest_snapshot_lifetime);
                        }
                        calculateMax(max_dt_background_tasks_length, stat.background_tasks_length);
                    }
                }
            }
        }

        switch (context.getPageStorageRunMode())
        {
        case PageStorageRunMode::ONLY_V2:
        {
            set("MaxDTStableOldestSnapshotLifetime", max_dt_stable_oldest_snapshot_lifetime);
            set("MaxDTDeltaOldestSnapshotLifetime", max_dt_delta_oldest_snapshot_lifetime);
            set("MaxDTMetaOldestSnapshotLifetime", max_dt_meta_oldest_snapshot_lifetime);
            break;
        }
        case PageStorageRunMode::ONLY_V3:
        case PageStorageRunMode::MIX_MODE:
        {
            if (auto global_storage_pool = context.getGlobalStoragePool(); global_storage_pool)
            {
                const auto log_snap_stat = global_storage_pool->log_storage->getSnapshotsStat();
                const auto meta_snap_stat = global_storage_pool->meta_storage->getSnapshotsStat();
                const auto data_snap_stat = global_storage_pool->data_storage->getSnapshotsStat();
                set("MaxDTDeltaOldestSnapshotLifetime", log_snap_stat.longest_living_seconds);
                set("MaxDTMetaOldestSnapshotLifetime", meta_snap_stat.longest_living_seconds);
                set("MaxDTStableOldestSnapshotLifetime", data_snap_stat.longest_living_seconds);
            }
            break;
        }
        case PageStorageRunMode::UNI_PS:
        {
            if (auto uni_ps = context.tryGetWriteNodePageStorage(); uni_ps != nullptr)
            {
                // Only set delta snapshot lifetime when UniPS is enabled
                const auto snap_stat = uni_ps->getSnapshotsStat();
                set("MaxDTDeltaOldestSnapshotLifetime", snap_stat.longest_living_seconds);
            }
            break;
        }
        }

        set("MaxDTBackgroundTasksLength", max_dt_background_tasks_length);
    }

    {
        const FileUsageStatistics usage = getPageStorageFileUsage();
        set("BlobFileNums", usage.total_file_num);
        set("BlobDiskBytes", usage.total_disk_size);
        set("BlobValidBytes", usage.total_valid_size);
        set("LogNums", usage.total_log_file_num);
        set("LogDiskBytes", usage.total_log_disk_size);
        set("PagesInMem", usage.num_pages);
        set("VersionedEntries", DB::PS::PageStorageMemorySummary::versioned_entry_or_delete_count.load());
        set("UniversalWrite", DB::PS::PageStorageMemorySummary::universal_write_count.load());
    }

    if (context.getSharedContextDisagg()->isDisaggregatedStorageMode())
    {
        auto & tmt = context.getTMTContext();
        if (auto s3_gc_owner = tmt.getS3GCOwnerManager(); s3_gc_owner->isOwner())
        {
            GET_METRIC(tiflash_storage_s3_gc_status, type_owner).Set(1.0);
        }
    }

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
        mi_process_info(
            &elapsed_msecs,
            &user_msecs,
            &system_msecs,
            &current_rss,
            &peak_rss,
            &current_commit,
            &peak_commit,
            &page_faults);
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

#define GET_JEMALLOC_METRIC(NAME, TYPE)                       \
    do                                                        \
    {                                                         \
        TYPE value{};                                         \
        size_t size = sizeof(value);                          \
        je_mallctl("stats." NAME, &value, &size, nullptr, 0); \
        set("jemalloc." NAME, value);                         \
    } while (0);

        FOR_EACH_METRIC(GET_JEMALLOC_METRIC);

#undef GET_JEMALLOC_METRIC
#undef FOR_EACH_METRIC
    }
#endif


    /// Add more metrics as you wish.
    set("mmap.alive", DB::allocator_mmap_counter.load(std::memory_order_relaxed));
}


} // namespace DB
