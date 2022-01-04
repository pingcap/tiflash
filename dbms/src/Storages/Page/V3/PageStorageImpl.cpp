#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageStorageImpl.h>

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
    PSDiskDelegatorPtr delegator,
    const Config & config_,
    const FileProviderPtr & file_provider_)
    : DB::PageStorage(name, delegator, config_, file_provider_)
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
    auto del_entries = page_directory.gc();

    for (const auto & del_version_entry : del_entries)
    {
        blob_store->remove(del_version_entry);
    }

    const auto & blob_need_gc = blob_store->getGCStats();
    if (blob_need_gc.empty())
    {
        return true;
    }

    auto [blob_gc_info, total_page_size] = page_directory.getEntriesFromBlobIds(blob_need_gc);

    if (blob_gc_info.empty())
    {
        return true;
    }

    const auto & copy_list = blob_store->gc(blob_gc_info, total_page_size);

    if (copy_list.empty())
    {
        throw Exception("Something wrong after BlobStore GC.", ErrorCodes::LOGICAL_ERROR);
    }

    page_directory.gcApply(copy_list);
    return true;
}

void PageStorageImpl::registerExternalPagesCallbacks(ExternalPagesScanner scanner, ExternalPagesRemover remover)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}
#pragma GCC diagnostic pop

} // namespace PS::V3
} // namespace DB
