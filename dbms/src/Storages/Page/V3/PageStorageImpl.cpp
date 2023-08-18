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

#include <Common/TiFlashMetrics.h>
#include <Encryption/FileProvider.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/PathPool.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes
namespace PS::V3
{
PageStorageImpl::PageStorageImpl(
    String name,
    PSDiskDelegatorPtr delegator_,
    const Config & config_,
    const FileProviderPtr & file_provider_)
    : DB::PageStorage(name, delegator_, config_, file_provider_)
    , log(Logger::get("PageStorage", name))
    , blob_store(name, file_provider_, delegator, parseBlobConfig(config_))
{
    LOG_FMT_INFO(log, "PageStorageImpl start. Config{{ {} }}", config.toDebugStringV3());
}

PageStorageImpl::~PageStorageImpl() = default;

void PageStorageImpl::reloadConfig()
{
    blob_store.reloadConfig(parseBlobConfig(config));
}

void PageStorageImpl::restore()
{
    // TODO: clean up blobstore.
    // TODO: Speedup restoring
    blob_store.registerPaths();

    PageDirectoryFactory factory;
    page_directory = factory
                         .setBlobStore(blob_store)
                         .create(storage_name, file_provider, delegator, parseWALConfig(config));
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
    return blob_store.getFileUsageStatistics();
}

SnapshotsStatistics PageStorageImpl::getSnapshotsStat() const
{
    return page_directory->getSnapshotsStat();
}

size_t PageStorageImpl::getNumberOfPages()
{
    return page_directory->numPages();
}

std::set<PageId> PageStorageImpl::getAliveExternalPageIds(NamespaceId ns_id)
{
    return page_directory->getAliveExternalIds(ns_id);
}

void PageStorageImpl::writeImpl(DB::WriteBatch && write_batch, const WriteLimiterPtr & write_limiter)
{
    if (unlikely(write_batch.empty()))
        return;

    // Persist Page data to BlobStore
    auto edit = blob_store.write(write_batch, write_limiter);
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
        const auto & [id, entry] = page_directory->getOrNull(buildV3Id(ns_id, page_id), snapshot);
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
        LOG_FMT_WARNING(log, "{}", e.message());
        return {.file_id = INVALID_BLOBFILE_ID}; // return invalid PageEntry
    }
}

DB::Page PageStorageImpl::readImpl(NamespaceId ns_id, PageId page_id, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot, bool throw_on_not_exist)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    auto page_entry = throw_on_not_exist ? page_directory->get(buildV3Id(ns_id, page_id), snapshot) : page_directory->getOrNull(buildV3Id(ns_id, page_id), snapshot);
    return blob_store.read(page_entry, read_limiter);
}

PageMap PageStorageImpl::readImpl(NamespaceId ns_id, const PageIds & page_ids, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot, bool throw_on_not_exist)
{
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
        auto page_entries = page_directory->get(page_id_v3s, snapshot);
        return blob_store.read(page_entries, read_limiter);
    }
    else
    {
        auto [page_entries, page_ids_not_found] = page_directory->getOrNull(page_id_v3s, snapshot);
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

PageIds PageStorageImpl::readImpl(NamespaceId ns_id, const PageIds & page_ids, const PageHandler & handler, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot, bool throw_on_not_exist)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    PageIdV3Internals page_id_v3s;
    for (auto p_id : page_ids)
        page_id_v3s.emplace_back(buildV3Id(ns_id, p_id));

    if (throw_on_not_exist)
    {
        auto page_entries = page_directory->get(page_id_v3s, snapshot);
        blob_store.read(page_entries, handler, read_limiter);
        return {};
    }
    else
    {
        auto [page_entries, page_ids_not_found] = page_directory->getOrNull(page_id_v3s, snapshot);
        blob_store.read(page_entries, handler, read_limiter);
        return page_ids_not_found;
    }
}

PageMap PageStorageImpl::readImpl(NamespaceId ns_id, const std::vector<PageReadFields> & page_fields, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot, bool throw_on_not_exist)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    BlobStore::FieldReadInfos read_infos;
    PageIds page_ids_not_found;
    for (const auto & [page_id, field_indices] : page_fields)
    {
        const auto & [id, entry] = throw_on_not_exist ? page_directory->get(buildV3Id(ns_id, page_id), snapshot) : page_directory->getOrNull(buildV3Id(ns_id, page_id), snapshot);

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
        const auto & page_id_and_entry = page_directory->get(valid_page, snapshot);
        acceptor(blob_store.read(page_id_and_entry));
    }
}

bool PageStorageImpl::gcImpl(bool /*not_skip*/, const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter)
{
    // If another thread is running gc, just return;
    bool v = false;
    if (!gc_is_running.compare_exchange_strong(v, true))
        return false;

    Stopwatch gc_watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_storage_page_gc_count, type_v3).Increment();
        GET_METRIC(tiflash_storage_page_gc_duration_seconds, type_v3).Observe(gc_watch.elapsedSeconds());
        bool is_running = true;
        gc_is_running.compare_exchange_strong(is_running, false);
    });

    auto clean_external_page = [this]() {
        std::scoped_lock lock{callbacks_mutex};
        if (!callbacks_container.empty())
        {
            for (const auto & [ns_id, callbacks] : callbacks_container)
            {
                auto pending_external_pages = callbacks.scanner();
                auto alive_external_ids = getAliveExternalPageIds(ns_id);
                callbacks.remover(pending_external_pages, alive_external_ids);
            }
        }
    };

    // 1. Do the MVCC gc, clean up expired snapshot.
    // And get the expired entries.
    if (page_directory->tryDumpSnapshot(read_limiter, write_limiter))
    {
        GET_METRIC(tiflash_storage_page_gc_count, type_v3_mvcc_dumped).Increment();
    }
    const auto dump_snapshots_ms = gc_watch.elapsedMillisecondsFromLastTime();

    const auto & del_entries = page_directory->gcInMemEntries();
    const auto gc_in_mem_entries_ms = gc_watch.elapsedMillisecondsFromLastTime();

    // 2. Remove the expired entries in BlobStore.
    // It won't delete the data on the disk.
    // It will only update the SpaceMap which in memory.
    blob_store.remove(del_entries);
    const auto blobstore_remove_entries_ms = gc_watch.elapsedMillisecondsFromLastTime();

    // 3. Analyze the status of each Blob in order to obtain the Blobs that need to do `heavy GC`.
    // Blobs that do not need to do heavy GC will also do ftruncate to reduce space enlargement.
    const auto & blob_need_gc = blob_store.getGCStats();
    const auto blobstore_get_gc_stats_ms = gc_watch.elapsedMillisecondsFromLastTime();
    if (blob_need_gc.empty())
    {
        LOG_FMT_INFO(log, "GC finished without any blob need full gc. [total time(ms)={}]"
                          " [dump snapshots(ms)={}] [gc in mem entries(ms)={}]"
                          " [blobstore remove entries(ms)={}] [blobstore get status(ms)={}]",
                     gc_watch.elapsedMilliseconds(),
                     dump_snapshots_ms,
                     gc_in_mem_entries_ms,
                     blobstore_remove_entries_ms,
                     blobstore_get_gc_stats_ms);
        clean_external_page();
        return false;
    }
    else
    {
        GET_METRIC(tiflash_storage_page_gc_count, type_v3_bs_full_gc).Increment(blob_need_gc.size());
    }

    // Execute full gc
    // 4. Filter out entries in MVCC by BlobId.
    // We also need to filter the version of the entry.
    // So that the `gc_apply` can proceed smoothly.
    auto [blob_gc_info, total_page_size] = page_directory->getEntriesByBlobIds(blob_need_gc);
    const auto gc_get_entries_ms = gc_watch.elapsedMillisecondsFromLastTime();
    if (blob_gc_info.empty())
    {
        LOG_FMT_INFO(log, "GC finished without any entry need be moved. [total time(ms)={}]"
                          " [dump snapshots(ms)={}] [in mem entries(ms)={}]"
                          " [blobstore remove entries(ms)={}] [blobstore get status(ms)={}]"
                          " [get entries(ms)={}]",
                     gc_watch.elapsedMilliseconds(),
                     dump_snapshots_ms,
                     gc_in_mem_entries_ms,
                     blobstore_remove_entries_ms,
                     blobstore_get_gc_stats_ms,
                     gc_get_entries_ms);

        clean_external_page();
        return false;
    }

    // 5. Do the BlobStore GC
    // After BlobStore GC, these entries will be migrated to a new blob.
    // Then we should notify MVCC apply the change.
    PageEntriesEdit gc_edit = blob_store.gc(blob_gc_info, total_page_size, write_limiter, read_limiter);
    const auto blobstore_full_gc_ms = gc_watch.elapsedMillisecondsFromLastTime();
    if (gc_edit.empty())
    {
        throw Exception("Something wrong after BlobStore GC.", ErrorCodes::LOGICAL_ERROR);
    }

    // 6. MVCC gc apply
    // MVCC will apply the migrated entries.
    // Also it will generate a new version for these entries.
    // Note that if the process crash between step 5 and step 6, the stats in BlobStore will
    // be reset to correct state during restore. If any exception thrown, then some BlobFiles
    // will be remained as "read-only" files while entries in them are useless in actual.
    // Those BlobFiles should be cleaned during next restore.
    page_directory->gcApply(std::move(gc_edit), write_limiter);
    const auto gc_apply_ms = gc_watch.elapsedMillisecondsFromLastTime();
    LOG_FMT_INFO(log, "GC finished. [total time(ms)={}]"
                      " [dump snapshots(ms)={}] [gc in mem entries(ms)={}]"
                      " [blobstore remove entries(ms)={}] [blobstore get status(ms)={}]"
                      " [get gc entries(ms)={}] [blobstore full gc(ms)={}]"
                      " [gc apply(ms)={}]",
                 gc_watch.elapsedMilliseconds(),
                 dump_snapshots_ms,
                 gc_in_mem_entries_ms,
                 blobstore_remove_entries_ms,
                 blobstore_get_gc_stats_ms,
                 gc_get_entries_ms,
                 blobstore_full_gc_ms,
                 gc_apply_ms);

    clean_external_page();

    return true;
}

void PageStorageImpl::registerExternalPagesCallbacks(const ExternalPageCallbacks & callbacks)
{
    std::scoped_lock lock{callbacks_mutex};
    assert(callbacks.scanner != nullptr);
    assert(callbacks.remover != nullptr);
    assert(callbacks.ns_id != MAX_NAMESPACE_ID);
    assert(callbacks_container.count(callbacks.ns_id) == 0);
    callbacks_container.emplace(callbacks.ns_id, callbacks);
}

void PageStorageImpl::unregisterExternalPagesCallbacks(NamespaceId ns_id)
{
    std::scoped_lock lock{callbacks_mutex};
    callbacks_container.erase(ns_id);
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
