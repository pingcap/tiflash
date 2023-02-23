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

#include <Common/Exception.h>
#include <Interpreters/SettingsCommon.h>
#include <Storages/Page/FileUsage.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/UniversalWriteBatch.h>
#include <Storages/Page/V3/Blob/BlobConfig.h>
#include <Storages/Page/V3/Blob/BlobFile.h>
#include <Storages/Page/V3/Blob/BlobStat.h>
#include <Storages/Page/V3/PageDirectory/ExternalIdTrait.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/spacemap/SpaceMap.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/PathPool.h>

#include <Storages/Page/universal/RemotePageReader.h>

#include <mutex>
#include <unordered_map>

namespace DB
{
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
    BlobStore(String storage_name, const FileProviderPtr & file_provider_, PSDiskDelegatorPtr delegator_, const BlobConfig & config, const String & remote_dir = "");

    void registerPaths();

    void reloadConfig(const BlobConfig & rhs);

    FileUsageStatistics getFileUsageStatistics() const;

    std::vector<BlobFileId> getGCStats();

    typename Trait::PageEntriesEdit
    gc(typename Trait::GcEntriesMap & entries_need_gc,
       const PageSize & total_page_size,
       const WriteLimiterPtr & write_limiter = nullptr,
       const ReadLimiterPtr & read_limiter = nullptr);

    typename Trait::PageEntriesEdit
    write(typename Trait::WriteBatch & wb, const WriteLimiterPtr & write_limiter = nullptr);

    void remove(const PageEntriesV3 & del_entries);

    typename Trait::PageMap read(typename Trait::PageIdAndEntries & entries, const ReadLimiterPtr & read_limiter = nullptr);

    Page read(const typename Trait::PageIdAndEntry & entry, const ReadLimiterPtr & read_limiter = nullptr);

    typename Trait::PageMap read(typename Trait::FieldReadInfos & to_read, const ReadLimiterPtr & read_limiter = nullptr);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    using ExternalIdTrait = typename Trait::ExternalIdTrait;

    typename Trait::PageEntriesEdit
    handleLargeWrite(typename Trait::WriteBatch & wb, const WriteLimiterPtr & write_limiter = nullptr);

    BlobFilePtr read(
        const typename Trait::PageId & page_id_v3,
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
    std::pair<BlobFileId, BlobFileOffset> getPosFromStats(size_t size);

    /**
     *  Request a specific BlobStat to delete a certain span.
     *  We will lock the BlobStat until it have been makefree in memory.
     */
    void removePosFromStats(BlobFileId blob_id, BlobFileOffset offset, size_t size);

    String getBlobFileParentPath(BlobFileId blob_id);

    BlobFilePtr getBlobFile(BlobFileId blob_id);

    template <typename>
    friend class PageDirectoryFactory;
    friend class PageStorageControlV3;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    PSDiskDelegatorPtr delegator;

    RemotePageReaderPtr remote_page_reader;

    FileProviderPtr file_provider;
    BlobConfig config;

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
    using PageEntriesEdit = PageEntriesEdit;
    using GcEntries = std::vector<std::tuple<PageId, PageVersion, PageEntryV3>>;
    using GcEntriesMap = std::map<BlobFileId, GcEntries>;
    using PageIdAndEntry = PageIDAndEntryV3;
    using PageIdAndEntries = PageIDAndEntriesV3;
    using PageMap = PageMap;

    using ExternalIdTrait = ExternalIdTrait;
    using WriteBatch = DB::WriteBatch;

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
};

using BlobStorePtr = std::shared_ptr<BlobStore<BlobStoreTrait>>;
} // namespace u128

namespace universal
{
struct BlobStoreTrait
{
    using PageId = UniversalPageId;
    using PageEntriesEdit = PageEntriesEdit;
    using GcEntries = std::vector<std::tuple<PageId, PageVersion, PageEntryV3>>;
    using GcEntriesMap = std::map<BlobFileId, GcEntries>;
    using PageIdAndEntry = std::pair<PageId, PageEntryV3>;
    using PageIdAndEntries = std::vector<PageIdAndEntry>;
    // TODO: universal pagemap/handler may should not filter by prefix
    using PageMap = std::map<PageId, DB::Page>;
    using PageHandler = std::function<void(PageId page_id, const DB::Page &)>;

    using ExternalIdTrait = ExternalIdTrait;
    using WriteBatch = UniversalWriteBatch;

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
};
using BlobStorePtr = std::shared_ptr<BlobStore<BlobStoreTrait>>;
} // namespace universal

} // namespace PS::V3
} // namespace DB
