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

#include <Storages/Page/V3/Blob/BlobStat.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDefines.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/WALStore.h>

namespace DB
{
class PSDiskDelegator;
using PSDiskDelegatorPtr = std::shared_ptr<PSDiskDelegator>;

namespace PS::V3
{
class WALStoreReader;
using WALStoreReaderPtr = std::shared_ptr<WALStoreReader>;

/**
  * A helper class for creating `PageDirectory` instance and restore data from disk.
  * During restoring data, we need to restore `BlobStore::BlobStats` at the same time.
  */
template <typename Trait>
class PageDirectoryFactory
{
public:
    using PageEntriesEdit = PageEntriesEdit<typename Trait::PageId>;
    using PageDirectoryPtr = std::unique_ptr<typename Trait::PageDirectory>;

public:
    PageVersion max_applied_ver;

    PageDirectoryFactory<Trait> & setBlobStore(typename Trait::BlobStore & blob_store)
    {
        blob_stats = &blob_store.blob_stats;
        return *this;
    }

    PageDirectoryPtr create(
        const String & storage_name,
        FileProviderPtr & file_provider,
        PSDiskDelegatorPtr & delegator,
        const WALConfig & config);

    PageDirectoryPtr createFromReader(const String & storage_name, WALStoreReaderPtr reader, WALStorePtr wal);

    // create a PageDirectory which can only be manipulated with memory-only operations
    PageDirectoryPtr dangerouslyCreateFromEditWithoutWAL(const String & storage_name, PageEntriesEdit & edit);

    // just for test
    PageDirectoryPtr createFromEditForTest(
        const String & storage_name,
        FileProviderPtr & file_provider,
        PSDiskDelegatorPtr & delegator,
        PageEntriesEdit & edit,
        UInt64 filter_seq = 0);

    // just for test
    PageDirectoryFactory<Trait> & setBlobStats(BlobStats & blob_stats_)
    {
        blob_stats = &blob_stats_;
        return *this;
    }

private:
    void loadFromDisk(const PageDirectoryPtr & dir, WALStoreReaderPtr && reader);
    void loadEdit(const PageDirectoryPtr & dir, const PageEntriesEdit & edit, bool force_apply, UInt64 filter_seq = 0);
    static void applyRecord(
        const PageDirectoryPtr & dir,
        const typename PageEntriesEdit::EditRecord & r,
        bool strict_check);
    static void updateMaxIdByRecord(const PageDirectoryPtr & dir, const typename PageEntriesEdit::EditRecord & r);

    void restoreBlobStats(const PageDirectoryPtr & dir);

    BlobStats * blob_stats = nullptr;

    // For debug tool
    template <typename T>
    friend class PageStorageControlV3;
    struct DebugOptions
    {
        bool dump_entries = false;
        bool apply_entries_to_directory = true;
    };
    DebugOptions debug;
};

namespace u128
{
struct FactoryTrait
{
    using PageId = PageIdV3Internal;
    using PageDirectory = PageDirectoryType;
    using BlobStore = BlobStoreType;
    using PageIdTrait = PageIdTrait;
    using Serializer = Serializer;
};
using PageDirectoryFactory = DB::PS::V3::PageDirectoryFactory<FactoryTrait>;
} // namespace u128
namespace universal
{
struct FactoryTrait
{
    using PageId = UniversalPageId;
    using PageDirectory = PageDirectoryType;
    using BlobStore = BlobStoreType;
    using PageIdTrait = PageIdTrait;
    using Serializer = Serializer;
};
using PageDirectoryFactory = DB::PS::V3::PageDirectoryFactory<FactoryTrait>;
} // namespace universal
} // namespace PS::V3
} // namespace DB
