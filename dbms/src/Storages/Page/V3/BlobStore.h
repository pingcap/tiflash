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

#include <Common/Exception.h>
#include <Interpreters/SettingsCommon.h>
#include <Storages/Page/FileUsage.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/V3/Blob/BlobConfig.h>
#include <Storages/Page/V3/Blob/BlobFile.h>
#include <Storages/Page/V3/Blob/BlobStat.h>
#include <Storages/Page/V3/PageDefines.h>
#include <Storages/Page/V3/PageDirectory/PageIdTrait.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/PageType.h>
#include <Storages/Page/V3/spacemap/SpaceMap.h>

#include <mutex>
#include <unordered_map>

namespace DB
{
class WriteBatch;
class UniversalWriteBatch;

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace PS::V3
{
template <typename Trait>
class BlobStore : private Allocator<false>
{
public:
    using PageId = typename Trait::PageId;
    using PageEntries = PageEntriesV3;
    using PageEntriesEdit = DB::PS::V3::PageEntriesEdit<PageId>;
    using GcEntries = std::vector<std::tuple<PageId, PageVersion, PageEntryV3>>;
    using GcEntriesMap = std::map<BlobFileId, GcEntries>;
    using PageIdAndEntry = std::pair<PageId, PageEntryV3>;
    using PageIdAndEntries = std::vector<PageIdAndEntry>;
    using PageMap = typename Trait::PageMap;

public:
    BlobStore(
        const String & storage_name,
        const FileProviderPtr & file_provider_,
        PSDiskDelegatorPtr delegator_,
        const BlobConfig & config,
        const PageTypeAndConfig & page_type_and_config_);

    void registerPaths();

    void reloadConfig(const BlobConfig & rhs);

    FileUsageStatistics getFileUsageStatistics() const;

    using PageTypeAndBlobIds = std::map<PageType, std::vector<BlobFileId>>;
    PageTypeAndBlobIds getGCStats();

    void gc(
        PageType page_type,
        const GcEntriesMap & entries_need_gc,
        const PageSize & total_page_size,
        PageEntriesEdit & edit,
        const WriteLimiterPtr & write_limiter = nullptr,
        const ReadLimiterPtr & read_limiter = nullptr);

    using PageTypeAndGcInfo = std::vector<std::tuple<PageType, GcEntriesMap, PageSize>>;
    PageEntriesEdit gc(
        const PageTypeAndGcInfo & page_type_and_gc_info,
        const WriteLimiterPtr & write_limiter = nullptr,
        const ReadLimiterPtr & read_limiter = nullptr);

    PageEntriesEdit write(
        typename Trait::WriteBatch && wb,
        PageType page_type = PageType::Normal,
        const WriteLimiterPtr & write_limiter = nullptr);

    // Freeze coming writes on all existing BlobFiles.
    // New writes will be written to new BlobFiles.
    void freezeBlobFiles();

    void removeEntries(const PageEntries & del_entries);

    PageMap read(PageIdAndEntries & entries, const ReadLimiterPtr & read_limiter = nullptr);

    Page read(const PageIdAndEntry & entry, const ReadLimiterPtr & read_limiter = nullptr);

    struct FieldReadInfo
    {
        PageId page_id;
        PageEntryV3 entry;
        std::vector<size_t> fields;

        FieldReadInfo(const PageId & id_, PageEntryV3 entry_, std::vector<size_t> fields_)
            : page_id(id_)
            , entry(entry_)
            , fields(std::move(fields_))
        {}
    };
    using FieldReadInfos = std::vector<FieldReadInfo>;
    PageMap read(FieldReadInfos & to_read, const ReadLimiterPtr & read_limiter = nullptr);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    PageEntriesEdit handleLargeWrite(
        typename Trait::WriteBatch && wb,
        PageType page_type,
        const WriteLimiterPtr & write_limiter = nullptr);

    void read(
        const PageId & page_id_v3,
        BlobFileId blob_id,
        BlobFileOffset offset,
        char * buffers,
        size_t size,
        const ReadLimiterPtr & read_limiter = nullptr,
        bool background = false);

    /**
     *  Ask BlobStats to get a span from BlobStat.
     *  We will lock BlobStats until we get a BlobStat that can hold the size.
     *  Then lock the BlobStat to get the span.
     */
    std::pair<BlobFileId, BlobFileOffset> getPosFromStats(size_t size, PageType page_type);

    /**
     *  Request a specific BlobStat to delete a certain span.
     *  We will lock the BlobStat until it have been makefree in memory.
     */
    void removePosFromStats(BlobFileId blob_id, BlobFileOffset offset, size_t size);

    String getBlobFileParentPath(BlobFileId blob_id);

    BlobFilePtr getBlobFile(BlobFileId blob_id);

    template <typename>
    friend class PageDirectoryFactory;
    template <typename>
    friend class PageStorageControlV3;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    PSDiskDelegatorPtr delegator;

    FileProviderPtr file_provider;
    BlobConfig config;
    PageTypeAndConfig page_type_and_config;

    LoggerPtr log;

    BlobStats blob_stats;

    std::mutex mtx_blob_files;
    std::unordered_map<BlobFileId, BlobFilePtr> blob_files;
};
namespace u128
{
struct BlobStoreTrait
{
    using PageId = PageIdV3Internal;
    using PageMap = std::map<PageIdU64, Page>;
    using PageIdTrait = PageIdTrait;
    using WriteBatch = DB::WriteBatch;
};
using BlobStoreType = BlobStore<BlobStoreTrait>;
} // namespace u128
namespace universal
{
struct BlobStoreTrait
{
    using PageId = UniversalPageId;
    using PageMap = std::map<PageId, Page>;
    using PageIdTrait = PageIdTrait;
    using WriteBatch = UniversalWriteBatch;
};
using BlobStoreType = BlobStore<BlobStoreTrait>;
} // namespace universal
} // namespace PS::V3
} // namespace DB
