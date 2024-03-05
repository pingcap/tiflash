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
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDefines.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/Page/V3/WAL/WALConfig.h>
#include <Storages/Page/WriteBatchImpl.h>
#include <Storages/PathPool.h>
#include <common/logger_useful.h>


namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
} // namespace DB::ErrorCodes

namespace DB::PS::V3
{
PageStorageImpl::PageStorageImpl(
    String name,
    PSDiskDelegatorPtr delegator_,
    const PageStorageConfig & config_,
    const FileProviderPtr & file_provider_)
    : DB::PageStorage(name, delegator_, config_, file_provider_)
    , log(Logger::get(name))
    , blob_store(
          name,
          file_provider_,
          delegator,
          BlobConfig::from(config_),
          PageTypeAndConfig{{PageType::Normal, PageTypeConfig{.heavy_gc_valid_rate = config.blob_heavy_gc_valid_rate}}})
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

    u128::PageDirectoryFactory factory;
    page_directory
        = factory.setBlobStore(blob_store).create(storage_name, file_provider, delegator, WALConfig::from(config));
}

PageIdU64 PageStorageImpl::getMaxId()
{
    return page_directory->getMaxIdAfterRestart();
}

void PageStorageImpl::drop()
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

PageIdU64 PageStorageImpl::getNormalPageIdImpl(
    NamespaceID ns_id,
    PageIdU64 page_id,
    SnapshotPtr snapshot,
    bool throw_on_not_exist)
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
std::set<PageIdU64> PageStorageImpl::getAliveExternalPageIds(NamespaceID ns_id)
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
    SCOPE_EXIT(
        { GET_METRIC(tiflash_storage_page_write_duration_seconds, type_total).Observe(watch.elapsedSeconds()); });

    // Persist Page data to BlobStore
    auto edit = blob_store.write(std::move(write_batch), PageType::Normal, write_limiter);
    page_directory->apply(std::move(edit), write_limiter);
}

void PageStorageImpl::freezeDataFiles()
{
    blob_store.freezeBlobFiles();
}

DB::PageEntry PageStorageImpl::getEntryImpl(NamespaceID ns_id, PageIdU64 page_id, SnapshotPtr snapshot)
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

DB::Page PageStorageImpl::readImpl(
    NamespaceID ns_id,
    PageIdU64 page_id,
    const ReadLimiterPtr & read_limiter,
    SnapshotPtr snapshot,
    bool throw_on_not_exist)
{
    GET_METRIC(tiflash_storage_page_command_count, type_read).Increment();
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    auto page_entry = throw_on_not_exist ? page_directory->getByID(buildV3Id(ns_id, page_id), snapshot)
                                         : page_directory->getByIDOrNull(buildV3Id(ns_id, page_id), snapshot);
    return blob_store.read(page_entry, read_limiter);
}

PageMapU64 PageStorageImpl::readImpl(
    NamespaceID ns_id,
    const PageIdU64s & page_ids,
    const ReadLimiterPtr & read_limiter,
    SnapshotPtr snapshot,
    bool throw_on_not_exist)
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
        PageMapU64 page_map = blob_store.read(page_entries, read_limiter);
        for (const auto & page_id_not_found : page_ids_not_found)
        {
            page_map.emplace(page_id_not_found.low, Page::invalidPage());
        }
        return page_map;
    }
}

PageMapU64 PageStorageImpl::readImpl(
    NamespaceID ns_id,
    const std::vector<PageReadFields> & page_fields,
    const ReadLimiterPtr & read_limiter,
    SnapshotPtr snapshot,
    bool throw_on_not_exist)
{
    GET_METRIC(tiflash_storage_page_command_count, type_read).Increment();
    if (!snapshot)
    {
        snapshot = this->getSnapshot("");
    }

    // get the entries from directory, keep track
    // for not found page_ids
    PageIdU64s page_ids_not_found;
    BlobStore<u128::BlobStoreTrait>::FieldReadInfos read_infos;
    for (const auto & [page_id, field_indices] : page_fields)
    {
        const auto & [id, entry] = throw_on_not_exist
            ? page_directory->getByID(buildV3Id(ns_id, page_id), snapshot)
            : page_directory->getByIDOrNull(buildV3Id(ns_id, page_id), snapshot);

        if (entry.isValid())
        {
            auto info = BlobStore<u128::BlobStoreTrait>::FieldReadInfo(buildV3Id(ns_id, page_id), entry, field_indices);
            read_infos.emplace_back(info);
        }
        else
        {
            page_ids_not_found.emplace_back(id);
        }
    }

    // read page data from blob_store
    PageMapU64 page_map = blob_store.read(read_infos, read_limiter);
    for (const auto & page_id_not_found : page_ids_not_found)
    {
        page_map.emplace(page_id_not_found, Page::invalidPage());
    }
    return page_map;
}

Page PageStorageImpl::readImpl(
    NamespaceID /*ns_id*/,
    const PageReadFields & /*page_field*/,
    const ReadLimiterPtr & /*read_limiter*/,
    SnapshotPtr /*snapshot*/,
    bool /*throw_on_not_exist*/)
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

bool PageStorageImpl::gcImpl(
    bool /*not_skip*/,
    const WriteLimiterPtr & write_limiter,
    const ReadLimiterPtr & read_limiter)
{
    return manager.gc(blob_store, *page_directory, write_limiter, read_limiter, nullptr, log);
}

void PageStorageImpl::registerExternalPagesCallbacks(const ExternalPageCallbacks & callbacks)
{
    manager.registerExternalPagesCallbacks(callbacks);
}

void PageStorageImpl::unregisterExternalPagesCallbacks(NamespaceID ns_id)
{
    manager.unregisterExternalPagesCallbacks(ns_id);
    // clean all external ids ptrs
    page_directory->unregisterNamespace(ns_id);
}

} // namespace DB::PS::V3
