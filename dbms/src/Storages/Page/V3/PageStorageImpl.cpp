#include <Encryption/FileProvider.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/PathPool.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes
namespace PS::V3
{
PageStorageImpl::PageStorageImpl(
    String name,
    PSDiskDelegatorPtr delegator_,
    const Config & config_,
    const FileProviderPtr & file_provider_)
    : DB::PageStorage(name, delegator_, config_, file_provider_)
    , blob_store(file_provider_, delegator->defaultPath(), blob_config)
{
    // TBD: init blob_store ptr.
}

PageStorageImpl::~PageStorageImpl() = default;


void PageStorageImpl::restore()
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void PageStorageImpl::drop()
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

PageId PageStorageImpl::getMaxId()
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

// FIXME: enable -Wunused-parameter
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
PageId PageStorageImpl::getNormalPageId(PageId page_id, SnapshotPtr snapshot)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

DB::PageStorage::SnapshotPtr PageStorageImpl::getSnapshot()
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

std::tuple<size_t, double, unsigned> PageStorageImpl::getSnapshotsStat() const
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void PageStorageImpl::write(DB::WriteBatch && write_batch, const WriteLimiterPtr & write_limiter)
{
    // Persist Page data to BlobStore
    PageEntriesEdit edit(write_batch.getWrites().size());

    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

DB::PageEntry PageStorageImpl::getEntry(PageId page_id, SnapshotPtr snapshot)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

DB::Page PageStorageImpl::read(PageId page_id, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot)
{
    // PageEntryV3 entry = page_directory.get(page_id, snapshot);
    // DB::Page page = blob_store.read(entry, read_limiter);
    // return page;
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

PageMap PageStorageImpl::read(const std::vector<PageId> & page_ids, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot)
{
    // PageIDAndEntriesV2 entries = page_directory.get(page_ids, snapshot);
    // DB::Page page = blob_store.read(entries, read_limiter);
    // return page;
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void PageStorageImpl::read(const std::vector<PageId> & page_ids, const PageHandler & handler, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

PageMap PageStorageImpl::read(const std::vector<PageReadFields> & page_fields, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void PageStorageImpl::traverse(const std::function<void(const DB::Page & page)> & acceptor, SnapshotPtr snapshot)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void PageStorageImpl::traversePageEntries(const std::function<void(PageId page_id, const DB::PageEntry & page)> & acceptor, SnapshotPtr snapshot)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

bool PageStorageImpl::gc(bool not_skip, const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter)
{
    // 1. Do the MVCC gc, clean up expired snapshot.
    // And get the expired entries.
    const auto & del_entries = page_directory.gc();

    // 2. Remove the expired entries in BlobStore.
    // It won't delete the data on the disk.
    // It will only update the SpaceMap which in memory.
    for (const auto & del_version_entry : del_entries)
    {
        blob_store.remove(del_version_entry);
    }

    // 3. Analyze the status of each Blob in order to obtain the Blobs that need to do `heavy GC`.
    // Blobs that do not need to do heavy GC will also do ftruncate to reduce space enlargement.
    const auto & blob_need_gc = blob_store.getGCStats();

    if (blob_need_gc.empty())
    {
        return true;
    }

    // 4. Filter out entries in MVCC by BlobId.
    // We also need to filter the version of the entry.
    // So that the `gc_apply` can proceed smoothly.
    auto [blob_gc_info, total_page_size] = page_directory.getEntriesByBlobIds(blob_need_gc);

    if (blob_gc_info.empty())
    {
        return true;
    }

    // 5. Do the BlobStore GC
    // After BlobStore GC, these entries will be migrated to a new blob.
    // Then we should notify MVCC apply the change.
    PageEntriesEdit gc_edit = blob_store.gc(blob_gc_info, total_page_size);
    if (gc_edit.empty())
    {
        throw Exception("Something wrong after BlobStore GC.", ErrorCodes::LOGICAL_ERROR);
    }

    // 6. MVCC gc apply
    // MVCC will apply the migrated entries.
    // Also it will generate a new version for these entries.
    // Note that if the process crash between step 5 and step 6, the stats in BlobStore will
    // be reset to correct state during restore. If any exception thrown, then some BlobFiles
    // will be remained as "read-only" files while entries in them are useless in actual.
    // Those BlobFiles should be cleaned during next restore.
    page_directory.gcApply(std::move(gc_edit), false);
    return true;
}

void PageStorageImpl::registerExternalPagesCallbacks(ExternalPagesScanner scanner, ExternalPagesRemover remover)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}
#pragma GCC diagnostic pop

} // namespace PS::V3
} // namespace DB
