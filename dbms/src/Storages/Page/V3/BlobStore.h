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
#include <Storages/Page/V3/Blob/BlobConfig.h>
#include <Storages/Page/V3/Blob/BlobFile.h>
#include <Storages/Page/V3/Blob/BlobStat.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/spacemap/SpaceMap.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/PathPool.h>

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
using PageIdAndVersionedEntries = std::vector<std::tuple<PageIdV3Internal, PageVersion, PageEntryV3>>;

class BlobStore : private Allocator<false>
{
public:
    BlobStore(String storage_name, const FileProviderPtr & file_provider_, PSDiskDelegatorPtr delegator_, const BlobConfig & config);

    void registerPaths();

    void reloadConfig(const BlobConfig & rhs);

    FileUsageStatistics getFileUsageStatistics() const;

    std::vector<BlobFileId> getGCStats();

    PageEntriesEdit gc(std::map<BlobFileId, PageIdAndVersionedEntries> & entries_need_gc,
                       const PageSize & total_page_size,
                       const WriteLimiterPtr & write_limiter = nullptr,
                       const ReadLimiterPtr & read_limiter = nullptr);

    PageEntriesEdit write(DB::WriteBatch & wb, const WriteLimiterPtr & write_limiter = nullptr);

    void remove(const PageEntriesV3 & del_entries);

    PageMap read(PageIDAndEntriesV3 & entries, const ReadLimiterPtr & read_limiter = nullptr);

    Page read(const PageIDAndEntryV3 & entry, const ReadLimiterPtr & read_limiter = nullptr);

    struct FieldReadInfo
    {
        PageIdV3Internal page_id;
        PageEntryV3 entry;
        std::vector<size_t> fields;

        FieldReadInfo(PageIdV3Internal id_, PageEntryV3 entry_, std::vector<size_t> fields_)
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

    PageEntriesEdit handleLargeWrite(DB::WriteBatch & wb, const WriteLimiterPtr & write_limiter = nullptr);

    BlobFilePtr read(const PageIdV3Internal & page_id_v3, BlobFileId blob_id, BlobFileOffset offset, char * buffers, size_t size, const ReadLimiterPtr & read_limiter = nullptr, bool background = false);

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

    friend class PageDirectoryFactory;
    friend class PageStorageControlV3;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    PSDiskDelegatorPtr delegator;

    FileProviderPtr file_provider;
    BlobConfig config;

    LoggerPtr log;

    BlobStats blob_stats;

    std::mutex mtx_blob_files;
    std::unordered_map<BlobFileId, BlobFilePtr> blob_files;
};
using BlobStorePtr = std::shared_ptr<BlobStore>;

} // namespace PS::V3
} // namespace DB
