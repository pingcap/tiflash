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

#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageEntriesEdit.h>

namespace DB
{
class PSDiskDelegator;
using PSDiskDelegatorPtr = std::shared_ptr<PSDiskDelegator>;

namespace PS::V3
{
class PageDirectory;
using PageDirectoryPtr = std::unique_ptr<PageDirectory>;
class WALStoreReader;
using WALStoreReaderPtr = std::shared_ptr<WALStoreReader>;

/**
  * A helper class for creating `PageDirectory` instance and restore data from disk.
  * During restoring data, we need to restore `BlobStore::BlobStats` at the same time.
  */
class PageDirectoryFactory
{
public:
    PageVersion max_applied_ver;
    PageIdV3Internal max_applied_page_id;

    PageDirectoryFactory & setBlobStore(BlobStore & blob_store)
    {
        blob_stats = &blob_store.blob_stats;
        return *this;
    }

    PageDirectoryPtr create(String storage_name, FileProviderPtr & file_provider, PSDiskDelegatorPtr & delegator, WALStore::Config config);


    // just for test
    PageDirectoryPtr createFromEdit(String storage_name, FileProviderPtr & file_provider, PSDiskDelegatorPtr & delegator, const PageEntriesEdit & edit);

    // just for test
    PageDirectoryFactory & setBlobStats(BlobStore::BlobStats & blob_stats_)
    {
        blob_stats = &blob_stats_;
        return *this;
    }

    friend class PageStorageControl;

private:
    PageDirectoryPtr create(String storage_name, //
                            FileProviderPtr & file_provider,
                            PSDiskDelegatorPtr & delegator,
                            WALStore::Config config,
                            bool not_enable_blob,
                            bool skip_mvcc_gc,
                            bool only_restore_snapshot_log);

    void loadFromDisk(const PageDirectoryPtr & dir, WALStoreReaderPtr && reader);
    void loadEdit(const PageDirectoryPtr & dir, const PageEntriesEdit & edit);
    static bool applyRecord(
        const PageDirectoryPtr & dir,
        const PageEntriesEdit::EditRecord & r,
        bool throw_on_error);

    BlobStore::BlobStats * blob_stats = nullptr;
};

} // namespace PS::V3

} // namespace DB
