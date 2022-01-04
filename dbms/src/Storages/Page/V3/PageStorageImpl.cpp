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
    std::lock_guard<std::mutex> write_lock(pageid_mutex);
    // return page_directory.getSnapshot()->version()->maxId();
    return 0;
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
        return {}; // return invalid PageEntry
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

    const auto & page_ids = page_directory.getAllPageIds();
    for (const auto & valid_page : page_ids)
    {
        const auto & page_entries = page_directory.get(valid_page, snapshot);
        acceptor(blob_store.read(page_entries));
    }
}

void PageStorageImpl::traversePageEntries(const std::function<void(PageId page_id, const DB::PageEntry & page)> & acceptor, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot();
    }

    const auto & page_ids = page_directory.getAllPageIds();
    for (const auto & valid_page : page_ids)
    {
        [[maybe_unused]] const auto & page_entries = page_directory.get(valid_page, snapshot);

        // TODO V3 entry to PageEntry
        // acceptor(valid_page, page_entries.second);
    }
}

bool PageStorageImpl::gc(bool not_skip, const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter)
{
    auto del_entries = page_directory.gc();

    for (const auto & del_version_entry : del_entries)
    {
        blob_store.remove(del_version_entry);
    }

    const auto & blob_need_gc = blob_store.getGCStats();
    if (blob_need_gc.empty())
    {
        return true;
    }

    auto [blob_gc_info, total_page_size] = page_directory.getEntriesFromBlobIds(blob_need_gc);

    if (blob_gc_info.empty())
    {
        return true;
    }

    const auto & copy_list = blob_store.gc(blob_gc_info, total_page_size);

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
