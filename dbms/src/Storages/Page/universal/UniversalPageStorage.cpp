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

#include <Common/TiFlashMetrics.h>
#include <Storages/Page/V3/Blob/BlobConfig.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/Page/V3/WAL/WALConfig.h>
#include <Storages/Page/universal/UniversalPageStorage.h>

namespace DB
{

UniversalPageStoragePtr UniversalPageStorage::create(
    String name,
    PSDiskDelegatorPtr delegator,
    const PageStorageConfig & config,
    const FileProviderPtr & file_provider)
{
    UniversalPageStoragePtr storage = std::make_shared<UniversalPageStorage>(name, delegator, config, file_provider);
    storage->blob_store = std::make_shared<PS::V3::BlobStore<PS::V3::universal::BlobStoreTrait>>(
        name,
        file_provider,
        delegator,
        PS::V3::BlobConfig::from(config));
    return storage;
}

void UniversalPageStorage::restore()
{
    blob_store->registerPaths();

    PS::V3::universal::PageDirectoryFactory factory;
    page_directory = factory
                         .setBlobStore(*blob_store)
                         .create(storage_name, file_provider, delegator, PS::V3::WALConfig::from(config));
}

void UniversalPageStorage::write(UniversalWriteBatch && write_batch, const WriteLimiterPtr & write_limiter) const
{
    if (unlikely(write_batch.empty()))
        return;

    auto edit = blob_store->write(write_batch, write_limiter);
    page_directory->apply(std::move(edit), write_limiter);
}

String UniversalPageStorage::GCTimeStatistics::toLogging() const
{
    const std::string_view stage_suffix = [this]() {
        switch (stage)
        {
        case GCStageType::Unknown:
            return " <unknown>";
        case GCStageType::OnlyInMem:
            return " without full gc";
        case GCStageType::FullGCNothingMoved:
            return " without moving any entry";
        case GCStageType::FullGC:
            return "";
        }
    }();
    const auto get_external_msg = [this]() -> String {
        if (clean_external_page_ms == 0)
            return String("");
        static constexpr double SCALE_NS_TO_MS = 1'000'000.0;
        return fmt::format(" [external_callbacks={}] [external_gc={}ms] [scanner={:.2f}ms] [get_alive={:.2f}ms] [remover={:.2f}ms]",
                           num_external_callbacks,
                           clean_external_page_ms,
                           external_page_scan_ns / SCALE_NS_TO_MS,
                           external_page_get_alive_ns / SCALE_NS_TO_MS,
                           external_page_remove_ns / SCALE_NS_TO_MS);
    };
    return fmt::format("GC finished{}."
                       " [total time={}ms]"
                       " [dump snapshots={}ms] [gc in mem entries={}ms]"
                       " [blobstore remove entries={}ms] [blobstore get status={}ms]"
                       " [get gc entries={}ms] [blobstore full gc={}ms]"
                       " [gc apply={}ms]"
                       "{}", // a placeholder for external page gc at last
                       stage_suffix,
                       total_cost_ms,
                       dump_snapshots_ms,
                       gc_in_mem_entries_ms,
                       blobstore_remove_entries_ms,
                       blobstore_get_gc_stats_ms,
                       full_gc_get_entries_ms,
                       full_gc_blobstore_copy_ms,
                       full_gc_apply_ms,
                       get_external_msg());
}

UniversalPageStorage::GCTimeStatistics UniversalPageStorage::doGC(const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter)
{
    Stopwatch gc_watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_storage_page_gc_count, type_v3).Increment();
        GET_METRIC(tiflash_storage_page_gc_duration_seconds, type_v3).Observe(gc_watch.elapsedSeconds());
        bool is_running = true;
        gc_is_running.compare_exchange_strong(is_running, false);
    });

    GCTimeStatistics statistics;

    // 1. Do the MVCC gc, clean up expired snapshot.
    // And get the expired entries.
    if (page_directory->tryDumpSnapshot(read_limiter, write_limiter))
    {
        GET_METRIC(tiflash_storage_page_gc_count, type_v3_mvcc_dumped).Increment();
    }
    statistics.dump_snapshots_ms = gc_watch.elapsedMillisecondsFromLastTime();

    const auto & del_entries = page_directory->gcInMemEntries();
    statistics.gc_in_mem_entries_ms = gc_watch.elapsedMillisecondsFromLastTime();

    // 2. Remove the expired entries in BlobStore.
    // It won't delete the data on the disk.
    // It will only update the SpaceMap which in memory.
    blob_store->remove(del_entries);
    statistics.blobstore_remove_entries_ms = gc_watch.elapsedMillisecondsFromLastTime();

    // 3. Analyze the status of each Blob in order to obtain the Blobs that need to do `full GC`.
    // Blobs that do not need to do full GC will also do ftruncate to reduce space amplification.
    const auto & blob_ids_need_gc = blob_store->getGCStats();
    statistics.blobstore_get_gc_stats_ms = gc_watch.elapsedMillisecondsFromLastTime();
    if (blob_ids_need_gc.empty())
    {
        statistics.stage = GCStageType::OnlyInMem;
        statistics.total_cost_ms = gc_watch.elapsedMilliseconds();
        return statistics;
    }

    // Execute full gc
    GET_METRIC(tiflash_storage_page_gc_count, type_v3_bs_full_gc).Increment(blob_ids_need_gc.size());
    // 4. Filter out entries in MVCC by BlobId.
    // We also need to filter the version of the entry.
    // So that the `gc_apply` can proceed smoothly.
    auto [blob_gc_info, total_page_size] = page_directory->getEntriesByBlobIds(blob_ids_need_gc);
    statistics.full_gc_get_entries_ms = gc_watch.elapsedMillisecondsFromLastTime();
    if (blob_gc_info.empty())
    {
        statistics.stage = GCStageType::FullGCNothingMoved;
        statistics.total_cost_ms = gc_watch.elapsedMilliseconds();
        return statistics;
    }

    // 5. Do the BlobStore GC
    // After BlobStore GC, these entries will be migrated to a new blob.
    // Then we should notify MVCC apply the change.
    PS::V3::universal::PageEntriesEdit gc_edit = blob_store->gc(blob_gc_info, total_page_size, write_limiter, read_limiter);
    statistics.full_gc_blobstore_copy_ms = gc_watch.elapsedMillisecondsFromLastTime();
    RUNTIME_CHECK_MSG(!gc_edit.empty(), "Something wrong after BlobStore GC");

    // 6. MVCC gc apply
    // MVCC will apply the migrated entries.
    // Also it will generate a new version for these entries.
    // Note that if the process crash between step 5 and step 6, the stats in BlobStore will
    // be reset to correct state during restore. If any exception thrown, then some BlobFiles
    // will be remained as "read-only" files while entries in them are useless in actual.
    // Those BlobFiles should be cleaned during next restore.
    page_directory->gcApply(std::move(gc_edit), write_limiter);
    statistics.full_gc_apply_ms = gc_watch.elapsedMillisecondsFromLastTime();

    statistics.stage = GCStageType::FullGC;
    statistics.total_cost_ms = gc_watch.elapsedMilliseconds();
    return statistics;
}
} // namespace DB
