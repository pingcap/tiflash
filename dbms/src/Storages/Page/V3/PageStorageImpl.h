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

#pragma once

#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Storages/Page/PageDefines.h>
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
        const PageStorageConfig & config_,
        const FileProviderPtr & file_provider_);

    ~PageStorageImpl() override;

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
    // clang-format off
    DB::PageStorage::SnapshotPtr getSnapshot() { return getSnapshot(""); }
    DB::PageEntry getEntry(PageId page_id) { return getEntryImpl(TEST_NAMESPACE_ID, page_id, nullptr); }
    DB::Page read(PageId page_id) { return readImpl(TEST_NAMESPACE_ID, page_id, nullptr, nullptr, true); }
    PageMap read(const PageIds & page_ids) { return readImpl(TEST_NAMESPACE_ID, page_ids, nullptr, nullptr, true); }
    PageMap read(const std::vector<PageReadFields> & page_fields) { return readImpl(TEST_NAMESPACE_ID, page_fields, nullptr, nullptr, true); }
    // clang-format on
#endif

    friend class PageDirectoryFactory;
    friend class PageStorageControlV3;
#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    enum class GCStageType
    {
        Unknown,
        OnlyInMem,
        FullGCNothingMoved,
        FullGC,
    };
    struct GCTimeStatistics
    {
        GCStageType stage = GCStageType::Unknown;
        bool executeNextImmediately() const { return stage == GCStageType::FullGC; };

        UInt64 total_cost_ms = 0;

        UInt64 compact_wal_ms = 0;
        UInt64 compact_directory_ms = 0;
        UInt64 compact_spacemap_ms = 0;
        // Full GC
        UInt64 full_gc_prepare_ms = 0;
        UInt64 full_gc_get_entries_ms = 0;
        UInt64 full_gc_blobstore_copy_ms = 0;
        UInt64 full_gc_apply_ms = 0;

        // GC external page
        UInt64 num_external_callbacks = 0;
        // Breakdown the duration for cleaning external pages
        // ms is usually too big for these operation, store by ns (10^-9)
        UInt64 external_page_scan_ns = 0;
        UInt64 external_page_get_alive_ns = 0;
        UInt64 external_page_remove_ns = 0;

    private:
        // Total time of cleaning external pages
        UInt64 clean_external_page_ms = 0;

    public:
        void finishCleanExternalPage(UInt64 clean_cost_ms);

        String toLogging() const;
    };

    GCTimeStatistics doGC(const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter);
    void cleanExternalPage(Stopwatch & gc_watch, GCTimeStatistics & statistics);

    LoggerPtr log;

    PageDirectoryPtr page_directory;

    BlobStore blob_store;

    std::atomic<bool> gc_is_running = false;

    const static String manifests_file_name;

    std::mutex callbacks_mutex;
    // Only std::map not std::unordered_map. We need insert/erase do not invalid other iterators.
    using ExternalPageCallbacksContainer = std::map<NamespaceId, std::shared_ptr<ExternalPageCallbacks>>;
    ExternalPageCallbacksContainer callbacks_container;
};

} // namespace PS::V3
} // namespace DB
