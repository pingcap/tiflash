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
    // {
    //     {
    //     // FIXME: These code should be inside `PageEntriesEdit BlobStore::write(DB::WriteBatch & write_batch, const WriteLimiterPtr)`
    //     PageEntriesEdit edit(write_batch.getWrites().size());
    //     const size_t all_page_data_size [[maybe_unused]] = write_batch.getTotalDataSize();
    //     // allocate file_id, offset from space map
    //     size_t offset_in_file = xxx;
    //
    //     // persist data to BlobFile
    //     }
    //
    //     PageEntryV2 entry;
    //     size_t offset_in_allocated = 0;
    //     for (const auto & w : write_batch.getWrites())
    //     {
    //         switch (w.type)
    //         {
    //         case WriteBatch::WriteType::PUT:
    //         {
    //             // entry.file_id = xxx;
    //             entry.offset = offset_in_file + offset_in_allocated;
    //             offset_in_allocated += w.size;
    //             edit.put(w.page_id, entry);
    //             break;
    //         }
    //         case WriteBatch::WriteType::DEL:
    //         case WriteBatch::WriteType::REF:
    //         case WriteBatch::WriteType::UPSERT:
    //             // TODO: put others to edit
    //             break;
    //         }
    //     }
    //     return edit;
    // }

    // Apply the position of persisted page to MVCC PageMap
    // TODO: Making `write` pipeline, we may split page_directory.apply into smaller functions later
    // page_directory.apply(std::move(edit));

    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

DB::PageEntry PageStorageImpl::getEntry(PageId page_id, SnapshotPtr snapshot)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

DB::Page PageStorageImpl::read(PageId page_id, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot)
{
    // PageEntryV2 entry = page_directory.get(page_id, snapshot);
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
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void PageStorageImpl::registerExternalPagesCallbacks(ExternalPagesScanner scanner, ExternalPagesRemover remover)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}
#pragma GCC diagnostic pop

} // namespace PS::V3
} // namespace DB
