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

#pragma once

#include <Common/Logger.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/WALStore.h>

namespace DB
{
namespace PS::V3
{
class PageStorageImpl : public DB::PageStorage
{
public:
    PageStorageImpl(
        String name,
        PSDiskDelegatorPtr delegator,
        const Config & config_,
        const FileProviderPtr & file_provider_);

    ~PageStorageImpl() override;

    static BlobStore::Config parseBlobConfig(const Config & config)
    {
        BlobStore::Config blob_config;

        blob_config.file_limit_size = config.blob_file_limit_size;
        blob_config.cached_fd_size = config.blob_cached_fd_size;
        blob_config.spacemap_type = config.blob_spacemap_type;
        blob_config.heavy_gc_valid_rate = config.blob_heavy_gc_valid_rate;
        blob_config.block_alignment_bytes = config.blob_block_alignment_bytes;

        return blob_config;
    }

    static WALStore::Config parseWALConfig(const Config & config)
    {
        WALStore::Config wal_config;

        wal_config.roll_size = config.wal_roll_size;
        wal_config.max_persisted_log_files = config.wal_max_persisted_log_files;
        wal_config.setRecoverMode(config.wal_recover_mode);

        return wal_config;
    }

    void reloadConfig() override;

    void restore() override;

    void drop() override;

    PageId getMaxId() override;

    PageId getNormalPageIdImpl(NamespaceId ns_id, PageId page_id, SnapshotPtr snapshot, bool throw_on_not_exist) override;

    DB::PageStorage::SnapshotPtr getSnapshot(const String & tracing_id) override;

    SnapshotsStatistics getSnapshotsStat() const override;

    FileUsageStatistics getFileUsageStatistics() const override;

    size_t getNumberOfPages() override;

    std::set<PageId> getAliveExternalPageIds(NamespaceId ns_id) override;

    void writeImpl(DB::WriteBatch && write_batch, const WriteLimiterPtr & write_limiter) override;

    DB::PageEntry getEntryImpl(NamespaceId ns_id, PageId page_id, SnapshotPtr snapshot) override;

    DB::Page readImpl(NamespaceId ns_id, PageId page_id, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot, bool throw_on_not_exist) override;

    PageMap readImpl(NamespaceId ns_id, const PageIds & page_ids, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot, bool throw_on_not_exist) override;

    PageIds readImpl(NamespaceId ns_id, const PageIds & page_ids, const PageHandler & handler, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot, bool throw_on_not_exist) override;

    PageMap readImpl(NamespaceId ns_id, const std::vector<PageReadFields> & page_fields, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot, bool throw_on_not_exist) override;

    Page readImpl(NamespaceId ns_id, const PageReadFields & page_field, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot, bool throw_on_not_exist) override;

    void traverseImpl(const std::function<void(const DB::Page & page)> & acceptor, SnapshotPtr snapshot) override;

    bool gcImpl(bool not_skip, const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter) override;

    void registerExternalPagesCallbacks(const ExternalPageCallbacks & callbacks) override;

    void unregisterExternalPagesCallbacks(NamespaceId ns_id) override;

    static bool isManifestsFileExists(const String & path);

    static void createManifestsFileIfNeed(const String & path);

#ifndef NDEBUG
    // Just for tests, refactor them out later
    DB::PageStorage::SnapshotPtr getSnapshot() { return getSnapshot(""); }
    DB::PageEntry getEntry(PageId page_id) { return getEntryImpl(TEST_NAMESPACE_ID, page_id, nullptr); }
    DB::Page read(PageId page_id) { return readImpl(TEST_NAMESPACE_ID, page_id, nullptr, nullptr, true); }
    PageMap read(const PageIds & page_ids) { return readImpl(TEST_NAMESPACE_ID, page_ids, nullptr, nullptr, true); }
    PageIds read(const PageIds & page_ids, const PageHandler & handler) { return readImpl(TEST_NAMESPACE_ID, page_ids, handler, nullptr, nullptr, true); }
    PageMap read(const std::vector<PageReadFields> & page_fields) { return readImpl(TEST_NAMESPACE_ID, page_fields, nullptr, nullptr, true); }
#endif

    friend class PageDirectoryFactory;
    friend class PageStorageControl;
#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    LoggerPtr log;

    PageDirectoryPtr page_directory;

    BlobStore blob_store;

    std::atomic<bool> gc_is_running = false;

    const static String manifests_file_name;

    std::mutex callbacks_mutex;
    using ExternalPageCallbacksContainer = std::unordered_map<NamespaceId, ExternalPageCallbacks>;
    ExternalPageCallbacksContainer callbacks_container;
};

} // namespace PS::V3
} // namespace DB
