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
#include <Common/Stopwatch.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/GCDefines.h>
#include <Storages/Page/V3/PageDefines.h>
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

    PageIdU64 getMaxId() override;

    PageIdU64 getNormalPageIdImpl(NamespaceID ns_id, PageIdU64 page_id, SnapshotPtr snapshot, bool throw_on_not_exist)
        override;

    DB::PageStorage::SnapshotPtr getSnapshot(const String & tracing_id) override;

    SnapshotsStatistics getSnapshotsStat() const override;

    FileUsageStatistics getFileUsageStatistics() const override;

    size_t getNumberOfPages() override;

    std::set<PageIdU64> getAliveExternalPageIds(NamespaceID ns_id) override;

    void writeImpl(DB::WriteBatch && write_batch, const WriteLimiterPtr & write_limiter) override;

    // After this, all existing data file will be frozen and new coming writes
    // will be written to new data files.
    void freezeDataFiles();

    DB::PageEntry getEntryImpl(NamespaceID ns_id, PageIdU64 page_id, SnapshotPtr snapshot) override;

    DB::Page readImpl(
        NamespaceID ns_id,
        PageIdU64 page_id,
        const ReadLimiterPtr & read_limiter,
        SnapshotPtr snapshot,
        bool throw_on_not_exist) override;

    PageMapU64 readImpl(
        NamespaceID ns_id,
        const PageIdU64s & page_ids,
        const ReadLimiterPtr & read_limiter,
        SnapshotPtr snapshot,
        bool throw_on_not_exist) override;

    PageMapU64 readImpl(
        NamespaceID ns_id,
        const std::vector<PageReadFields> & page_fields,
        const ReadLimiterPtr & read_limiter,
        SnapshotPtr snapshot,
        bool throw_on_not_exist) override;

    Page readImpl(
        NamespaceID ns_id,
        const PageReadFields & page_field,
        const ReadLimiterPtr & read_limiter,
        SnapshotPtr snapshot,
        bool throw_on_not_exist) override;

    void traverseImpl(const std::function<void(const DB::Page & page)> & acceptor, SnapshotPtr snapshot) override;

    bool gcImpl(bool not_skip, const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter) override;

    void registerExternalPagesCallbacks(const ExternalPageCallbacks & callbacks) override;

    void unregisterExternalPagesCallbacks(NamespaceID ns_id) override;

#ifndef NDEBUG
    // Just for tests, refactor them out later
    // clang-format off
    DB::PageStorage::SnapshotPtr getSnapshot() { return getSnapshot(""); }
    DB::PageEntry getEntry(PageIdU64 page_id) { return getEntryImpl(TEST_NAMESPACE_ID, page_id, nullptr); }
    DB::Page read(PageIdU64 page_id) { return readImpl(TEST_NAMESPACE_ID, page_id, nullptr, nullptr, true); }
    PageMapU64 read(const PageIdU64s & page_ids) { return readImpl(TEST_NAMESPACE_ID, page_ids, nullptr, nullptr, true); }
    PageMapU64 read(const std::vector<PageReadFields> & page_fields) { return readImpl(TEST_NAMESPACE_ID, page_fields, nullptr, nullptr, true); }
    // clang-format on
#endif

    template <typename>
    friend class PageDirectoryFactory;
    template <typename>
    friend class PageStorageControlV3;
#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    LoggerPtr log;

    u128::PageDirectoryPtr page_directory;

    u128::BlobStoreType blob_store;

    u128::ExternalPageCallbacksManager manager;
};

} // namespace PS::V3
} // namespace DB
