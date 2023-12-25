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

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/Stopwatch.h>
#include <Common/SyncPoint/SyncPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Encryption/FileProvider.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/Page/V3/WAL/WALConfig.h>
#include <Storages/PathPool.h>
#include <common/logger_useful.h>

#include <mutex>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes
namespace FailPoints
{
extern const char force_ps_wal_compact[];
}
namespace PS::V3
{
PageStorageImpl::PageStorageImpl(
    String name,
    PSDiskDelegatorPtr delegator_,
    const PageStorageConfig & config_,
    const FileProviderPtr & file_provider_)
    : DB::PageStorage(name, delegator_, config_, file_provider_)
    , log(Logger::get(name))
    , blob_store(name, file_provider_, delegator, BlobConfig::from(config_))
{
    LOG_INFO(log, "PageStorageImpl start. Config{{ {} }}", config.toDebugStringV3());
}

PageStorageImpl::~PageStorageImpl() = default;

void PageStorageImpl::reloadConfig()
{
    blob_store.reloadConfig(BlobConfig::from(config));
}

void PageStorageImpl::restore()
{
    // TODO: clean up blobstore.
    // TODO: Speedup restoring
    blob_store.registerPaths();

    PageDirectoryFactory factory;
    page_directory = factory
                         .setBlobStore(blob_store)
                         .create(storage_name, file_provider, delegator, WALConfig::from(config));
}

PageId PageStorageImpl::getMaxId()
{
    return page_directory->getMaxId();
}

void PageStorageImpl::drop()
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

PageId PageStorageImpl::getNormalPageIdImpl(NamespaceId ns_id, PageId page_id, SnapshotPtr snapshot, bool throw_on_not_exist)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    return page_directory->getNormalPageId(buildV3Id(ns_id, page_id), snapshot, throw_on_not_exist).low;
}

DB::PageStorage::SnapshotPtr PageStorageImpl::getSnapshot(const String & tracing_id)
{
    return page_directory->createSnapshot(tracing_id);
}

FileUsageStatistics PageStorageImpl::getFileUsageStatistics() const
{
    auto u = blob_store.getFileUsageStatistics();
    u.merge(page_directory->getFileUsageStatistics());
    return u;
}

SnapshotsStatistics PageStorageImpl::getSnapshotsStat() const
{
    return page_directory->getSnapshotsStat();
}

size_t PageStorageImpl::getNumberOfPages()
{
    return page_directory->numPages();
}

// For debugging purpose
std::set<PageId> PageStorageImpl::getAliveExternalPageIds(NamespaceId ns_id)
{
    // Keep backward compatibility of this functions with v2
    if (auto ids = page_directory->getAliveExternalIds(ns_id); ids)
        return *ids;
    return {};
}

void PageStorageImpl::writeImpl(DB::WriteBatch && write_batch, const WriteLimiterPtr & write_limiter)
{
    if (unlikely(write_batch.empty()))
        return;

    Stopwatch watch;
    SCOPE_EXIT({ GET_METRIC(tiflash_storage_page_write_duration_seconds, type_total).Observe(watch.elapsedSeconds()); });

    // Persist Page data to BlobStore
    auto edit = blob_store.write(std::move(write_batch), write_limiter);
    page_directory->apply(std::move(edit), write_limiter);
}

DB::PageEntry PageStorageImpl::getEntryImpl(NamespaceId ns_id, PageId page_id, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    try
    {
        const auto & [id, entry] = page_directory->getByIDOrNull(buildV3Id(ns_id, page_id), snapshot);
        (void)id;
        // TODO : after `PageEntry` in page.h been moved to v2.
        // Then we don't copy from V3 to V2 format
        PageEntry entry_ret;
        entry_ret.file_id = entry.file_id;
        entry_ret.offset = entry.offset;
        entry_ret.tag = entry.tag;
        entry_ret.size = entry.size;
        entry_ret.field_offsets = entry.field_offsets;
        entry_ret.checksum = entry.checksum;

        return entry_ret;
    }
    catch (DB::Exception & e)
    {
        LOG_WARNING(log, "{}", e.message());
        return {.file_id = INVALID_BLOBFILE_ID}; // return invalid PageEntry
    }
}

DB::Page PageStorageImpl::readImpl(NamespaceId ns_id, PageId page_id, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot, bool throw_on_not_exist)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    auto page_entry = throw_on_not_exist ? page_directory->getByID(buildV3Id(ns_id, page_id), snapshot) : page_directory->getByIDOrNull(buildV3Id(ns_id, page_id), snapshot);
    return blob_store.read(page_entry, read_limiter);
}

PageMap PageStorageImpl::readImpl(NamespaceId ns_id, const PageIds & page_ids, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot, bool throw_on_not_exist)
{
    GET_METRIC(tiflash_storage_page_command_count, type_read).Increment();
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    PageIdV3Internals page_id_v3s;
    for (auto p_id : page_ids)
    {
        page_id_v3s.emplace_back(buildV3Id(ns_id, p_id));
    }

    if (throw_on_not_exist)
    {
        auto page_entries = page_directory->getByIDs(page_id_v3s, snapshot);
        return blob_store.read(page_entries, read_limiter);
    }
    else
    {
        auto [page_entries, page_ids_not_found] = page_directory->getByIDsOrNull(page_id_v3s, snapshot);
        PageMap page_map = blob_store.read(page_entries, read_limiter);
        for (const auto & page_id_not_found : page_ids_not_found)
        {
            Page page_not_found;
            page_not_found.page_id = INVALID_PAGE_ID;
            page_map[page_id_not_found] = page_not_found;
        }
        return page_map;
    }
}

PageMap PageStorageImpl::readImpl(NamespaceId ns_id, const std::vector<PageReadFields> & page_fields, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot, bool throw_on_not_exist)
{
    GET_METRIC(tiflash_storage_page_command_count, type_read).Increment();
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    // get the entries from directory, keep track
    // for not found page_ids
    PageIds page_ids_not_found;
    BlobStore::FieldReadInfos read_infos;
    for (const auto & [page_id, field_indices] : page_fields)
    {
        const auto & [id, entry] = throw_on_not_exist ? page_directory->getByID(buildV3Id(ns_id, page_id), snapshot) : page_directory->getByIDOrNull(buildV3Id(ns_id, page_id), snapshot);

        if (entry.isValid())
        {
            auto info = BlobStore::FieldReadInfo(buildV3Id(ns_id, page_id), entry, field_indices);
            read_infos.emplace_back(info);
        }
        else
        {
            page_ids_not_found.emplace_back(id);
        }
    }

    // read page data from blob_store
    PageMap page_map = blob_store.read(read_infos, read_limiter);
    for (const auto & page_id_not_found : page_ids_not_found)
    {
        Page page_not_found;
        page_not_found.page_id = INVALID_PAGE_ID;
        page_map[page_id_not_found] = page_not_found;
    }
    return page_map;
}

Page PageStorageImpl::readImpl(NamespaceId /*ns_id*/, const PageReadFields & /*page_field*/, const ReadLimiterPtr & /*read_limiter*/, SnapshotPtr /*snapshot*/, bool /*throw_on_not_exist*/)
{
    throw Exception("Not support read single filed on V3", ErrorCodes::NOT_IMPLEMENTED);
}

void PageStorageImpl::traverseImpl(const std::function<void(const DB::Page & page)> & acceptor, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    // TODO: This could hold the read lock of `page_directory` for a long time
    const auto & page_ids = page_directory->getAllPageIds();
    for (const auto & valid_page : page_ids)
    {
        const auto & page_id_and_entry = page_directory->getByID(valid_page, snapshot);
        acceptor(blob_store.read(page_id_and_entry));
    }
}

Poco::Message::Priority
PageStorageImpl::GCTimeStatistics::getLoggingLevel() const
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

String PageStorageImpl::GCTimeStatistics::toLogging() const
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

void PageStorageImpl::GCTimeStatistics::finishCleanExternalPage(UInt64 clean_cost_ms)
{
    clean_external_page_ms = clean_cost_ms;
    GET_METRIC(tiflash_storage_page_gc_duration_seconds, type_clean_external).Observe(clean_external_page_ms / 1000.0);
}

bool PageStorageImpl::gcImpl(bool /*not_skip*/, const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter)
{
    // If another thread is running gc, just return;
    bool v = false;
    if (!gc_is_running.compare_exchange_strong(v, true))
        return false;

    const GCTimeStatistics statistics = doGC(write_limiter, read_limiter);
    assert(statistics.stage != GCStageType::Unknown); // `doGC` must set the stage
    LOG_IMPL(log, statistics.getLoggingLevel(), statistics.toLogging());

    return statistics.executeNextImmediately();
}

// Remove external pages for all tables
// TODO: `clean_external_page` for all tables may slow down the whole gc process when there are lots of table.
void PageStorageImpl::cleanExternalPage(Stopwatch & gc_watch, GCTimeStatistics & statistics)
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
        auto alive_external_ids = page_directory->getAliveExternalIds(ns_callbacks->ns_id);
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
            auto iter = callbacks_container.upper_bound(ns_callbacks->ns_id);
            if (iter == callbacks_container.end())
                break;
            ns_callbacks = iter->second;
        }
    }

    statistics.finishCleanExternalPage(gc_watch.elapsedMillisecondsFromLastTime());
}

PageStorageImpl::GCTimeStatistics PageStorageImpl::doGC(const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter)
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
    statistics.compact_wal_happen = page_directory->tryDumpSnapshot(read_limiter, write_limiter, force_wal_compact);
    if (statistics.compact_wal_happen)
    {
        GET_METRIC(tiflash_storage_page_gc_count, type_v3_mvcc_dumped).Increment();
    }
    statistics.compact_wal_ms = gc_watch.elapsedMillisecondsFromLastTime();
    GET_METRIC(tiflash_storage_page_gc_duration_seconds, type_compact_wal).Observe(statistics.compact_wal_ms / 1000.0);

    const auto & del_entries = page_directory->gcInMemEntries();
    statistics.compact_directory_ms = gc_watch.elapsedMillisecondsFromLastTime();
    GET_METRIC(tiflash_storage_page_gc_duration_seconds, type_compact_directory).Observe(statistics.compact_directory_ms / 1000.0);

    SYNC_FOR("before_PageStorageImpl::doGC_fullGC_prepare");

    // 2. Remove the expired entries in BlobStore.
    // It won't delete the data on the disk.
    // It will only update the SpaceMap which in memory.
    blob_store.remove(del_entries);
    statistics.compact_spacemap_ms = gc_watch.elapsedMillisecondsFromLastTime();
    GET_METRIC(tiflash_storage_page_gc_duration_seconds, type_compact_spacemap).Observe(statistics.compact_spacemap_ms / 1000.0);

    // Note that if full GC is not executed, below metrics won't be shown on grafana but it should
    // only take few ms to fininsh these in-memory operations. Check them out by the logs if
    // the total time cost not match.

    // 3. Check whether there are BlobFiles that need to do `full GC`.
    // This function will also try to use `ftruncate` to reduce space amplification.
    const auto & blob_ids_need_gc = blob_store.getGCStats();
    statistics.full_gc_prepare_ms = gc_watch.elapsedMillisecondsFromLastTime();
    if (blob_ids_need_gc.empty())
    {
        cleanExternalPage(gc_watch, statistics);
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
        cleanExternalPage(gc_watch, statistics);
        statistics.stage = GCStageType::FullGCNothingMoved;
        statistics.total_cost_ms = gc_watch.elapsedMilliseconds();
        return statistics;
    }

    SYNC_FOR("before_PageStorageImpl::doGC_fullGC_commit");

    // 5. Do the BlobStore GC
    // After BlobStore GC, these entries will be migrated to a new blob.
    // Then we should notify MVCC apply the change.
    PageEntriesEdit gc_edit = blob_store.gc(blob_gc_info, total_page_size, write_limiter, read_limiter);
    statistics.full_gc_blobstore_copy_ms = gc_watch.elapsedMillisecondsFromLastTime();
    GET_METRIC(tiflash_storage_page_gc_duration_seconds, type_fullgc_rewrite).Observe( //
        (statistics.full_gc_prepare_ms + statistics.full_gc_get_entries_ms + statistics.full_gc_blobstore_copy_ms) / 1000.0);
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
    GET_METRIC(tiflash_storage_page_gc_duration_seconds, type_fullgc_commit).Observe(statistics.full_gc_apply_ms / 1000.0);

    SYNC_FOR("after_PageStorageImpl::doGC_fullGC_commit");

    cleanExternalPage(gc_watch, statistics);
    statistics.stage = GCStageType::FullGC;
    statistics.total_cost_ms = gc_watch.elapsedMilliseconds();
    return statistics;
}

void PageStorageImpl::registerExternalPagesCallbacks(const ExternalPageCallbacks & callbacks)
{
    std::scoped_lock lock{callbacks_mutex};
    assert(callbacks.scanner != nullptr);
    assert(callbacks.remover != nullptr);
    assert(callbacks.ns_id != MAX_NAMESPACE_ID);
    // NamespaceId(TableID) should not be reuse
    RUNTIME_CHECK_MSG(
        callbacks_container.count(callbacks.ns_id) == 0,
        "Try to create callbacks for duplicated namespace id {}",
        callbacks.ns_id);
    // `emplace` won't invalid other iterator
    callbacks_container.emplace(callbacks.ns_id, std::make_shared<ExternalPageCallbacks>(callbacks));
}

void PageStorageImpl::unregisterExternalPagesCallbacks(NamespaceId ns_id)
{
    {
        std::scoped_lock lock{callbacks_mutex};
        callbacks_container.erase(ns_id);
    }
    // clean all external ids ptrs
    page_directory->unregisterNamespace(ns_id);
}

const String PageStorageImpl::manifests_file_name = "manifests";

bool PageStorageImpl::isManifestsFileExists(const String & path)
{
    Poco::File file(fmt::format("{}/{}", path, manifests_file_name));
    return file.exists();
}

void PageStorageImpl::createManifestsFileIfNeed(const String & path)
{
    Poco::File dir(path);
    dir.createDirectories();
    Poco::File file(fmt::format("{}/{}", path, manifests_file_name));
    file.createFile();
}

} // namespace PS::V3
} // namespace DB
