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
    , blob_store(file_provider_, delegator->defaultPath(), blob_config)
{
}

PageStorageImpl::~PageStorageImpl() = default;


void PageStorageImpl::restore()
{
    // TODO: clean up blobstore.
    // TODO: Speedup restoring
    PageDirectoryFactory factory;
    page_directory = factory
                         .setBlobStore(blob_store)
                         .create(file_provider, delegator);
    // factory.max_applied_page_id // TODO: return it to outer function
}

void PageStorageImpl::drop()
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

PageId PageStorageImpl::getMaxId(NamespaceId ns_id)
{
    return page_directory->getMaxId(ns_id);
}

PageId PageStorageImpl::getNormalPageId(NamespaceId ns_id, PageId page_id, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    return page_directory->getNormalPageId(buildV3Id(ns_id, page_id), snapshot).low;
}

DB::PageStorage::SnapshotPtr PageStorageImpl::getSnapshot(const String & tracing_id)
{
    return page_directory->createSnapshot(tracing_id);
}

SnapshotsStatistics PageStorageImpl::getSnapshotsStat() const
{
    return page_directory->getSnapshotsStat();
}

void PageStorageImpl::write(DB::WriteBatch && write_batch, const WriteLimiterPtr & write_limiter)
{
    if (unlikely(write_batch.empty()))
        return;

    // Persist Page data to BlobStore
    auto edit = blob_store.write(write_batch, write_limiter);
    page_directory->apply(std::move(edit), write_limiter);
}

DB::PageEntry PageStorageImpl::getEntry(NamespaceId ns_id, PageId page_id, SnapshotPtr snapshot)
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

DB::Page PageStorageImpl::read(NamespaceId ns_id, PageId page_id, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    auto page_entry = page_directory->get(buildV3Id(ns_id, page_id), snapshot);
    return blob_store.read(page_entry, read_limiter);
}

PageMap PageStorageImpl::read(NamespaceId ns_id, const std::vector<PageId> & page_ids, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    PageIdV3Internals page_id_v3s;
    for (auto p_id : page_ids)
        page_id_v3s.emplace_back(buildV3Id(ns_id, p_id));
    auto page_entries = page_directory->get(page_id_v3s, snapshot);
    return blob_store.read(page_entries, read_limiter);
}

void PageStorageImpl::read(NamespaceId ns_id, const std::vector<PageId> & page_ids, const PageHandler & handler, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    PageIdV3Internals page_id_v3s;
    for (auto p_id : page_ids)
        page_id_v3s.emplace_back(buildV3Id(ns_id, p_id));
    auto page_entries = page_directory->get(page_id_v3s, snapshot);
    blob_store.read(page_entries, handler, read_limiter);
}

PageMap PageStorageImpl::read(NamespaceId ns_id, const std::vector<PageReadFields> & page_fields, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    BlobStore::FieldReadInfos read_infos;
    for (const auto & [page_id, field_indices] : page_fields)
    {
        const auto & [id, entry] = page_directory->get(buildV3Id(ns_id, page_id), snapshot);
        (void)id;
        auto info = BlobStore::FieldReadInfo(buildV3Id(ns_id, page_id), entry, field_indices);
        read_infos.emplace_back(info);
    }

    return blob_store.read(read_infos, read_limiter);
}

void PageStorageImpl::traverse(const std::function<void(const DB::Page & page)> & acceptor, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    // TODO: This could hold the read lock of `page_directory` for a long time
    const auto & page_ids = page_directory->getAllPageIds();
    for (const auto & valid_page : page_ids)
    {
        const auto & page_entries = page_directory->get(valid_page, snapshot);
        acceptor(blob_store.read(page_entries));
    }
}

bool PageStorageImpl::gc(bool /*not_skip*/, const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter)
{
    // If another thread is running gc, just return;
    bool v = false;
    if (!gc_is_running.compare_exchange_strong(v, true))
        return false;

    SCOPE_EXIT({
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
                auto alive_external_ids = page_directory->getAliveExternalIds(ns_id);
                callbacks.remover(pending_external_pages, alive_external_ids);
            }
        }
    };


    // 1. Do the MVCC gc, clean up expired snapshot.
    // And get the expired entries.
    [[maybe_unused]] bool is_snapshot_dumped = page_directory->tryDumpSnapshot(write_limiter);
    const auto & del_entries = page_directory->gcInMemEntries();
    LOG_FMT_DEBUG(log, "Remove entries from memory [num_entries={}]", del_entries.size());

    // 2. Remove the expired entries in BlobStore.
    // It won't delete the data on the disk.
    // It will only update the SpaceMap which in memory.
    blob_store.remove(del_entries);

    // 3. Analyze the status of each Blob in order to obtain the Blobs that need to do `heavy GC`.
    // Blobs that do not need to do heavy GC will also do ftruncate to reduce space enlargement.
    const auto & blob_need_gc = blob_store.getGCStats();
    if (blob_need_gc.empty())
    {
        clean_external_page();
        return false;
    }

    // Execute full gc
    // 4. Filter out entries in MVCC by BlobId.
    // We also need to filter the version of the entry.
    // So that the `gc_apply` can proceed smoothly.
    auto [blob_gc_info, total_page_size] = page_directory->getEntriesByBlobIds(blob_need_gc);
    if (blob_gc_info.empty())
    {
        clean_external_page();
        return false;
    }

    // 5. Do the BlobStore GC
    // After BlobStore GC, these entries will be migrated to a new blob.
    // Then we should notify MVCC apply the change.
    PageEntriesEdit gc_edit = blob_store.gc(blob_gc_info, total_page_size, write_limiter, read_limiter);
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
