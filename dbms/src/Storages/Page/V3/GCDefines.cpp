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

#include <Common/FailPoint.h>
#include <Common/SyncPoint/SyncPoint.h>
#include <Common/TiFlashMetrics.h>
#include <IO/BaseFile/RateLimiter.h>
#include <Poco/Message.h>
#include <Storages/Page/V3/GCDefines.h>
#include <fmt/format.h>

#include <numeric>
#include <type_traits>

namespace DB
{
namespace FailPoints
{
extern const char force_ps_wal_compact[];
}
namespace PS::V3
{

Poco::Message::Priority GCTimeStatistics::getLoggingLevel() const
{
    switch (stage)
    {
    case GCStageType::FullGC:
    case GCStageType::FullGCNothingMoved:
        return Poco::Message::PRIO_INFORMATION;
    case GCStageType::OnlyInMem:
        if (compact_wal_happen)
            return Poco::Message::PRIO_INFORMATION;
        return Poco::Message::PRIO_DEBUG;
    case GCStageType::Unknown:
        return Poco::Message::PRIO_DEBUG;
    }
}

String GCTimeStatistics::toLogging() const
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
        return fmt::format(
            " [external_callbacks={}] [external_gc={}ms] [scanner={:.2f}ms] [get_alive={:.2f}ms] [remover={:.2f}ms]",
            num_external_callbacks,
            clean_external_page_ms,
            external_page_scan_ns / SCALE_NS_TO_MS,
            external_page_get_alive_ns / SCALE_NS_TO_MS,
            external_page_remove_ns / SCALE_NS_TO_MS);
    };
    return fmt::format(
        "GC finished{}."
        " [total time={}ms]"
        " [compact wal={}ms] [compact directory={}ms] [compact spacemap={}ms]"
        " [gc status={}ms] [gc entries={}ms] [gc data={}ms]"
        " [gc apply={}ms]"
        "{}", // a placeholder for external page gc at last
        stage_suffix,
        total_cost_ms,
        compact_wal_ms,
        compact_directory_ms,
        compact_spacemap_ms,
        full_gc_prepare_ms,
        full_gc_get_entries_ms,
        full_gc_blobstore_copy_ms,
        full_gc_apply_ms,
        get_external_msg());
}

void GCTimeStatistics::finishCleanExternalPage(UInt64 clean_cost_ms)
{
    clean_external_page_ms = clean_cost_ms;
    GET_METRIC(tiflash_storage_page_gc_duration_seconds, type_clean_external).Observe(clean_external_page_ms / 1000.0);
}

template <typename Trait>
void ExternalPageCallbacksManager<Trait>::registerExternalPagesCallbacks(const ExternalPageCallbacks & callbacks)
{
    std::scoped_lock lock{callbacks_mutex};
    assert(callbacks.scanner != nullptr);
    assert(callbacks.remover != nullptr);
    if constexpr (std::is_same_v<Trait, u128::ExternalPageCallbacksManagerTrait>)
    {
        assert(callbacks.prefix != 0);
    }
    else if constexpr (std::is_same_v<Trait, universal::ExternalPageCallbacksManagerTrait>)
    {
        assert(callbacks.prefix != "");
    }
    // NamespaceID(TableID) should not be reuse
    RUNTIME_CHECK_MSG(
        callbacks_container.count(callbacks.prefix) == 0,
        "Try to create callbacks for duplicated prefix {}",
        callbacks.prefix);
    // `emplace` won't invalid other iterator
    callbacks_container.emplace(callbacks.prefix, std::make_shared<ExternalPageCallbacks>(callbacks));
}

template <typename Trait>
void ExternalPageCallbacksManager<Trait>::unregisterExternalPagesCallbacks(const Prefix & prefix)
{
    std::scoped_lock lock{callbacks_mutex};
    callbacks_container.erase(prefix);
}

template <typename Trait>
bool ExternalPageCallbacksManager<Trait>::gc(
    typename Trait::BlobStore & blob_store,
    typename Trait::PageDirectory & page_directory,
    const WriteLimiterPtr & write_limiter,
    const ReadLimiterPtr & read_limiter,
    RemoteFileValidSizes * remote_valid_sizes,
    LoggerPtr log)
{
    // If another thread is running gc, just return;
    bool v = false;
    if (!gc_is_running.compare_exchange_strong(v, true))
        return false;

    const GCTimeStatistics statistics = doGC( //
        blob_store,
        page_directory,
        write_limiter,
        read_limiter,
        remote_valid_sizes);
    assert(statistics.stage != GCStageType::Unknown); // `doGC` must set the stage
    LOG_IMPL(log, statistics.getLoggingLevel(), statistics.toLogging());

    return statistics.executeNextImmediately();
}

// Remove external pages for all tables
// TODO: `clean_external_page` for all tables may slow down the whole gc process when there are lots of table.
template <typename Trait>
void ExternalPageCallbacksManager<Trait>::cleanExternalPage(
    PageDirectory & page_directory,
    Stopwatch & gc_watch,
    GCTimeStatistics & statistics)
{
    // Fine grained lock on `callbacks_mutex`.
    // So that adding/removing a storage will not be blocked for the whole
    // processing time of `cleanExternalPage`.
    std::shared_ptr<ExternalPageCallbacks> ns_callbacks;
    {
        std::scoped_lock lock{callbacks_mutex};
        // check and get the begin iter
        statistics.num_external_callbacks = callbacks_container.size();
        auto iter = callbacks_container.begin();
        if (iter == callbacks_container.end()) // empty
        {
            statistics.finishCleanExternalPage(gc_watch.elapsedMillisecondsFromLastTime());
            return;
        }

        assert(iter != callbacks_container.end()); // early exit in the previous code
        // keep the shared_ptr so that erasing ns_id from PageStorage won't invalid the `ns_callbacks`
        ns_callbacks = iter->second;
    }

    Stopwatch external_watch;

    SYNC_FOR("before_PageStorageImpl::cleanExternalPage_execute_callbacks");

    while (true)
    {
        // 1. Note that we must call `scanner` before `getAliveExternalIds`.
        // Or some committed external ids is not included in `alive_ids`
        // but exist in `pending_external_pages`. They will be removed by
        // accident with `remover` under this situation.
        // 2. Assume calling the callbacks after erasing ns_is is safe.

        // the external pages on disks.
        auto pending_external_pages = ns_callbacks->scanner();
        statistics.external_page_scan_ns += external_watch.elapsedFromLastTime();
        auto alive_external_ids = page_directory.getAliveExternalIds(ns_callbacks->prefix);
        statistics.external_page_get_alive_ns += external_watch.elapsedFromLastTime();
        if (alive_external_ids)
        {
            // remove the external pages that is not alive now.
            ns_callbacks->remover(pending_external_pages, *alive_external_ids);
        } // else the ns_id is invalid, just skip
        statistics.external_page_remove_ns += external_watch.elapsedFromLastTime();

        // move to next namespace callbacks
        {
            std::scoped_lock lock{callbacks_mutex};
            // next ns_id that is greater than `ns_id`
            auto iter = callbacks_container.upper_bound(ns_callbacks->prefix);
            if (iter == callbacks_container.end())
                break;
            ns_callbacks = iter->second;
        }
    }

    statistics.finishCleanExternalPage(gc_watch.elapsedMillisecondsFromLastTime());
}

template <typename Trait>
GCTimeStatistics ExternalPageCallbacksManager<Trait>::doGC(
    typename Trait::BlobStore & blob_store,
    typename Trait::PageDirectory & page_directory,
    const WriteLimiterPtr & write_limiter,
    const ReadLimiterPtr & read_limiter,
    RemoteFileValidSizes * remote_valid_sizes)
{
    Stopwatch gc_watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_storage_page_gc_count, type_v3).Increment();
        GET_METRIC(tiflash_storage_page_gc_duration_seconds, type_v3).Observe(gc_watch.elapsedSeconds());
        bool is_running = true;
        gc_is_running.compare_exchange_strong(is_running, false);
    });

    GCTimeStatistics statistics;

    // TODO: rewrite the GC process and split it into smaller interface
    bool force_wal_compact = false;
    fiu_do_on(FailPoints::force_ps_wal_compact, { force_wal_compact = true; });

    // 1. Do the MVCC gc, clean up expired snapshot.
    // And get the expired entries.
    typename Trait::PageDirectory::InMemGCOption options;
    if constexpr (std::is_same_v<Trait, universal::ExternalPageCallbacksManagerTrait>)
    {
        assert(remote_valid_sizes != nullptr);
        options.remote_valid_sizes = remote_valid_sizes;
    }
    const auto & del_entries = page_directory.gcInMemEntries(options);
    statistics.compact_directory_ms = gc_watch.elapsedMillisecondsFromLastTime();
    GET_METRIC(tiflash_storage_page_gc_duration_seconds, type_compact_directory)
        .Observe(statistics.compact_directory_ms / 1000.0);

    // Compact WAL after in-memory GC in PageDirectory in order to reduce the overhead of dumping useless entries
    statistics.compact_wal_happen = page_directory.tryDumpSnapshot(write_limiter, force_wal_compact);
    if (statistics.compact_wal_happen)
    {
        GET_METRIC(tiflash_storage_page_gc_count, type_v3_mvcc_dumped).Increment();
    }
    statistics.compact_wal_ms = gc_watch.elapsedMillisecondsFromLastTime();
    GET_METRIC(tiflash_storage_page_gc_duration_seconds, type_compact_wal).Observe(statistics.compact_wal_ms / 1000.0);

    SYNC_FOR("before_PageStorageImpl::doGC_fullGC_prepare");

    // 2. Remove the expired entries in BlobStore.
    // It won't delete the data on the disk.
    // It will only update the SpaceMap which in memory.
    blob_store.removeEntries(del_entries);
    statistics.compact_spacemap_ms = gc_watch.elapsedMillisecondsFromLastTime();
    GET_METRIC(tiflash_storage_page_gc_duration_seconds, type_compact_spacemap)
        .Observe(statistics.compact_spacemap_ms / 1000.0);

    // Note that if full GC is not executed, below metrics won't be shown on grafana but it should
    // only take few ms to finish these in-memory operations. Check them out by the logs if
    // the total time cost not match.

    // 3. Check whether there are BlobFiles that need to do `full GC`.
    // This function will also try to use `ftruncate` to reduce space amplification.
    const auto & blob_ids_need_gc = blob_store.getGCStats();
    statistics.full_gc_prepare_ms = gc_watch.elapsedMillisecondsFromLastTime();
    if (blob_ids_need_gc.empty())
    {
        cleanExternalPage(page_directory, gc_watch, statistics);
        statistics.stage = GCStageType::OnlyInMem;
        statistics.total_cost_ms = gc_watch.elapsedMilliseconds();
        return statistics;
    }

    // Execute full gc
    GET_METRIC(tiflash_storage_page_gc_count, type_v3_bs_full_gc)
        .Increment(std::accumulate(
            blob_ids_need_gc.begin(),
            blob_ids_need_gc.end(),
            0,
            [&](Int64 acc, const auto & page_type_and_blob_ids) {
                return acc + page_type_and_blob_ids.second.size();
            }));
    // 4. Filter out entries in MVCC by BlobId.
    // We also need to filter the version of the entry.
    // So that the `gc_apply` can proceed smoothly.
    auto page_type_gc_infos = page_directory.getEntriesByBlobIdsForDifferentPageTypes(blob_ids_need_gc);
    statistics.full_gc_get_entries_ms = gc_watch.elapsedMillisecondsFromLastTime();
    auto entries_size_to_move = std::accumulate(
        page_type_gc_infos.begin(),
        page_type_gc_infos.end(),
        0,
        [&](UInt64 acc, const auto & page_type_and_gc_info) { return acc + std::get<2>(page_type_and_gc_info); });
    if (entries_size_to_move == 0)
    {
        cleanExternalPage(page_directory, gc_watch, statistics);
        statistics.stage = GCStageType::FullGCNothingMoved;
        statistics.total_cost_ms = gc_watch.elapsedMilliseconds();
        return statistics;
    }

    SYNC_FOR("before_PageStorageImpl::doGC_fullGC_commit");

    // 5. Do the BlobStore GC
    // After BlobStore GC, these entries will be migrated to a new blob.
    // Then we should notify MVCC apply the change.
    PageEntriesEdit gc_edit = blob_store.gc(page_type_gc_infos, write_limiter, read_limiter);
    statistics.full_gc_blobstore_copy_ms = gc_watch.elapsedMillisecondsFromLastTime();
    GET_METRIC(tiflash_storage_page_gc_duration_seconds, type_fullgc_rewrite)
        .Observe( //
            (statistics.full_gc_prepare_ms + statistics.full_gc_get_entries_ms + statistics.full_gc_blobstore_copy_ms)
            / 1000.0);
    RUNTIME_CHECK_MSG(!gc_edit.empty(), "Something wrong after BlobStore GC");

    // 6. MVCC gc apply
    // MVCC will apply the migrated entries.
    // Also it will generate a new version for these entries.
    // Note that if the process crash between step 5 and step 6, the stats in BlobStore will
    // be reset to correct state during restore. If any exception thrown, then some BlobFiles
    // will be remained as "read-only" files while entries in them are useless in actual.
    // Those BlobFiles should be cleaned during next restore.
    page_directory.gcApply(std::move(gc_edit), write_limiter);
    statistics.full_gc_apply_ms = gc_watch.elapsedMillisecondsFromLastTime();
    GET_METRIC(tiflash_storage_page_gc_duration_seconds, type_fullgc_commit)
        .Observe(statistics.full_gc_apply_ms / 1000.0);

    SYNC_FOR("after_PageStorageImpl::doGC_fullGC_commit");

    cleanExternalPage(page_directory, gc_watch, statistics);
    statistics.stage = GCStageType::FullGC;
    statistics.total_cost_ms = gc_watch.elapsedMilliseconds();
    return statistics;
}

template class ExternalPageCallbacksManager<u128::ExternalPageCallbacksManagerTrait>;
template class ExternalPageCallbacksManager<universal::ExternalPageCallbacksManagerTrait>;
} // namespace PS::V3
} // namespace DB
