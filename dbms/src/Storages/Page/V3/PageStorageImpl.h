#pragma once

#include <Common/LogWithPrefix.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDirectory.h>

namespace DB
{
namespace PS::V3
{
class PageStorageImpl : public DB::PageStorage
{
public:
    PageStorageImpl(
        String name,
        PSDiskDelegatorPtr delegator,
        const Config & config_,
        const FileProviderPtr & file_provider_);

    ~PageStorageImpl();

    void restore() override;

    void drop() override;

    PageId getMaxId(NamespaceId ns_id) override;

    PageId getNormalPageId(NamespaceId ns_id, PageId page_id, SnapshotPtr snapshot) override;

    DB::PageStorage::SnapshotPtr getSnapshot() override;

    std::tuple<size_t, double, unsigned> getSnapshotsStat() const override;

    void write(DB::WriteBatch && write_batch, const WriteLimiterPtr & write_limiter) override;

    DB::PageEntry getEntry(NamespaceId ns_id, PageId page_id, SnapshotPtr snapshot) override;

    DB::Page read(NamespaceId ns_id, PageId page_id, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot) override;

    PageMap read(NamespaceId ns_id, const std::vector<PageId> & page_ids, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot) override;

    void read(NamespaceId ns_id, const std::vector<PageId> & page_ids, const PageHandler & handler, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot) override;

    PageMap read(NamespaceId ns_id, const std::vector<PageReadFields> & page_fields, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot) override;

    void traverse(const std::function<void(const DB::Page & page)> & acceptor, SnapshotPtr snapshot) override;

    bool gc(bool not_skip, const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter) override;

    void registerExternalPagesCallbacks(const ExternalPageCallbacks & callbacks) override;

    void clearExternalPagesCallbacks();
#ifndef NDEBUG
    // Just for tests, refactor them out later
    void write(DB::WriteBatch && wb) { return write(std::move(wb), nullptr); }
    DB::PageEntry getEntry(PageId page_id) { return getEntry(TEST_NAMESPACE_ID, page_id, nullptr); }
    DB::Page read(PageId page_id) { return read(TEST_NAMESPACE_ID, page_id, nullptr, nullptr); }
    PageMap read(const std::vector<PageId> & page_ids) { return read(TEST_NAMESPACE_ID, page_ids, nullptr, nullptr); }
    void read(const std::vector<PageId> & page_ids, const PageHandler & handler) { return read(TEST_NAMESPACE_ID, page_ids, handler, nullptr, nullptr); }
    PageMap read(const std::vector<PageReadFields> & page_fields) { return read(TEST_NAMESPACE_ID, page_fields, nullptr, nullptr); }
    void traverse(const std::function<void(const DB::Page & page)> & acceptor) { return traverse(acceptor, nullptr); }
    bool gc() { return gc(false, nullptr, nullptr); }
#endif

    friend class PageDirectoryFactory;
#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    LogWithPrefixPtr log;

    PageDirectoryPtr page_directory;

    BlobStore::Config blob_config;

    BlobStore blob_store;

    std::atomic<bool> gc_is_running = false;

    std::mutex callbacks_mutex;
    using ExternalPageCallbacksContainer = std::vector<ExternalPageCallbacks>;
    ExternalPageCallbacksContainer callbacks_container;
};

} // namespace PS::V3
} // namespace DB
