#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V3/PageStorage.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes
namespace PS::V3
{
PageStorage::PageStorage(
    String name,
    PSDiskDelegatorPtr delegator,
    const Config & config_,
    const FileProviderPtr & file_provider_)
    : DB::PageStorage(name, delegator, config_, file_provider_)
{
}

PageStorage::~PageStorage() = default;


void PageStorage::restore()
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void PageStorage::drop()
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

PageId PageStorage::getMaxId()
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

// FIXME: enable -Wunused-parameter
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
PageId PageStorage::getNormalPageId(PageId page_id, SnapshotPtr snapshot)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

DB::PageStorage::SnapshotPtr PageStorage::getSnapshot()
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

std::tuple<size_t, double, unsigned> PageStorage::getSnapshotsStat() const
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void PageStorage::write(DB::WriteBatch && write_batch, const WriteLimiterPtr & write_limiter)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

DB::PageEntry PageStorage::getEntry(PageId page_id, SnapshotPtr snapshot)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

DB::Page PageStorage::read(PageId page_id, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

PageMap PageStorage::read(const std::vector<PageId> & page_ids, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void PageStorage::read(const std::vector<PageId> & page_ids, const PageHandler & handler, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

PageMap PageStorage::read(const std::vector<PageReadFields> & page_fields, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void PageStorage::traverse(const std::function<void(const DB::Page & page)> & acceptor, SnapshotPtr snapshot)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void PageStorage::traversePageEntries(const std::function<void(PageId page_id, const DB::PageEntry & page)> & acceptor, SnapshotPtr snapshot)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

bool PageStorage::gc(bool not_skip, const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void PageStorage::registerExternalPagesCallbacks(ExternalPagesScanner scanner, ExternalPagesRemover remover)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}
#pragma GCC diagnostic pop

} // namespace PS::V3
} // namespace DB
