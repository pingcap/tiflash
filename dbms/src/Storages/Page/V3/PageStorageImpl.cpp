#include <Encryption/FileProvider.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V3/PageDirectory.h>
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
    , log(getLogWithPrefix(nullptr, "PageStorage"))
    , blob_store(file_provider_, delegator->defaultPath(), blob_config)
{
}

PageStorageImpl::~PageStorageImpl() = default;


void PageStorageImpl::restore()
{
    // TODO: Speedup restoring
    CollapsingPageDirectory collapsing_directory;
    auto callback = [&collapsing_directory](PageEntriesEdit && edit) {
        collapsing_directory.apply(std::move(edit));
    };
    // Restore `collapsing_directory` from disk
    auto wal = WALStore::create(callback, file_provider, delegator, /*write_limiter*/ nullptr);
    // PageId max_page_id = collapsing_directory.max_applied_page_id; // TODO: return it to outer function

    page_directory = PageDirectory::create(collapsing_directory, std::move(wal));

    // TODO: restore BlobStore
}

void PageStorageImpl::drop()
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

PageId PageStorageImpl::getMaxId()
{
    return page_directory.getMaxId();
}

PageId PageStorageImpl::getNormalPageId(PageId /*page_id*/, SnapshotPtr /*snapshot*/)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

DB::PageStorage::SnapshotPtr PageStorageImpl::getSnapshot()
{
    return page_directory.createSnapshot();
}

std::tuple<size_t, double, unsigned> PageStorageImpl::getSnapshotsStat() const
{
    return page_directory.getSnapshotsStat();
}

void PageStorageImpl::write(DB::WriteBatch && write_batch, const WriteLimiterPtr & write_limiter)
{
    if (unlikely(write_batch.empty()))
        return;

    // Persist Page data to BlobStore
    auto edit = blob_store.write(write_batch, write_limiter);
    page_directory.apply(std::move(edit));
}

DB::PageEntry PageStorageImpl::getEntry(PageId page_id, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot();
    }

    try
    {
        const auto & [id, entry] = page_directory.get(page_id, snapshot);
        (void)id;
        // TODO : after `PageEntry` in page.h been moved to v2.
        // Then we don't copy from V3 to V2 format
        PageEntry entry_ret;
        entry_ret.file_id = entry.file_id;
        entry_ret.offset = entry.offset;
        entry_ret.size = entry.size;
        entry_ret.field_offsets = entry.field_offsets;
        entry_ret.checksum = entry.checksum;

        return entry_ret;
    }
    catch (DB::Exception & e)
    {
        LOG_FMT_WARNING(log, "{}", e.message());
        return {.file_id = INVALID_BLOBFILE_ID}; // return invalid PageEntry
    }
}

DB::Page PageStorageImpl::read(PageId page_id, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot();
    }

    auto page_entry = page_directory.get(page_id, snapshot);
    return blob_store.read(page_entry, read_limiter);
}

PageMap PageStorageImpl::read(const std::vector<PageId> & page_ids, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot();
    }

    auto page_entries = page_directory.get(page_ids, snapshot);
    return blob_store.read(page_entries, read_limiter);
}

void PageStorageImpl::read(const std::vector<PageId> & page_ids, const PageHandler & handler, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot();
    }

    auto page_entries = page_directory.get(page_ids, snapshot);
    blob_store.read(page_entries, handler, read_limiter);
}

PageMap PageStorageImpl::read(const std::vector<PageReadFields> & page_fields, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot();
    }

    BlobStore::FieldReadInfos read_infos;
    for (const auto & [page_id, field_indices] : page_fields)
    {
        const auto & [id, entry] = page_directory.get(page_id, snapshot);
        (void)id;
        auto info = BlobStore::FieldReadInfo(page_id, entry, field_indices);
        read_infos.emplace_back(info);
    }

    return blob_store.read(read_infos, read_limiter);
}

void PageStorageImpl::traverse(const std::function<void(const DB::Page & page)> & acceptor, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot();
    }

    // TODO: This could hold the read lock of `page_directory` for a long time
    const auto & page_ids = page_directory.getAllPageIds();
    for (const auto & valid_page : page_ids)
    {
        const auto & page_entries = page_directory.get(valid_page, snapshot);
        acceptor(blob_store.read(page_entries));
    }
}

bool PageStorageImpl::gc(bool /*not_skip*/, const WriteLimiterPtr & /*write_limiter*/, const ReadLimiterPtr & /*read_limiter*/)
{
    // If another thread is running gc, just return;
    bool v = false;
    if (!gc_is_running.compare_exchange_strong(v, true))
        return false;

    SCOPE_EXIT({
        bool is_running = true;
        gc_is_running.compare_exchange_strong(is_running, false);
    });

    /// Get all pending external pages and BlobFiles. Note that we should get external pages before BlobFiles.
    PathAndIdsVec external_pages;
    if (external_pages_scanner)
    {
        external_pages = external_pages_scanner();
    }

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
        if (external_pages_remover)
        {
            external_pages_remover(external_pages, page_directory.getAllPageIds());
        }
        return false;
    }

    // 4. Filter out entries in MVCC by BlobId.
    // We also need to filter the version of the entry.
    // So that the `gc_apply` can proceed smoothly.
    auto [blob_gc_info, total_page_size] = page_directory.getEntriesByBlobIds(blob_need_gc);

    if (blob_gc_info.empty())
    {
        if (external_pages_remover)
        {
            external_pages_remover(external_pages, page_directory.getAllPageIds());
        }
        return false;
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
    const auto & page_ids = page_directory.gcApply(std::move(gc_edit), external_pages_scanner != nullptr);

    if (external_pages_remover)
    {
        external_pages_remover(external_pages, page_ids);
    }

    return true;
}

void PageStorageImpl::registerExternalPagesCallbacks(ExternalPagesScanner scanner, ExternalPagesRemover remover)
{
    assert(scanner != nullptr);
    assert(remover != nullptr);
    external_pages_scanner = scanner;
    external_pages_remover = remover;
}

} // namespace PS::V3
} // namespace DB
