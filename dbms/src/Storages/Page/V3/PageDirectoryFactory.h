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
    PageVersionType max_applied_ver;
    PageId max_applied_page_id;

    PageDirectoryFactory & setBlobStore(BlobStore & blob_store)
    {
        blob_stats = &blob_store.blob_stats;
        return *this;
    }

    PageDirectoryPtr create(FileProviderPtr & file_provider, PSDiskDelegatorPtr & delegator);

private:

    void loadFromDisk(const PageDirectoryPtr & dir, WALStoreReaderPtr && reader);

    BlobStore::BlobStats * blob_stats = nullptr;
};

} // namespace PS::V3

} // namespace DB
