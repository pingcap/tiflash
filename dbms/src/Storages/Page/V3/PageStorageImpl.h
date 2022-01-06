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

    PageId getMaxId() override;

    PageId getNormalPageId(PageId page_id, SnapshotPtr snapshot) override;

    DB::PageStorage::SnapshotPtr getSnapshot() override;

    std::tuple<size_t, double, unsigned> getSnapshotsStat() const override;

    void write(DB::WriteBatch && write_batch, const WriteLimiterPtr & write_limiter) override;

    DB::PageEntry getEntry(PageId page_id, SnapshotPtr snapshot) override;

    DB::Page read(PageId page_id, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot) override;

    PageMap read(const std::vector<PageId> & page_ids, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot) override;

    void read(const std::vector<PageId> & page_ids, const PageHandler & handler, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot) override;

    PageMap read(const std::vector<PageReadFields> & page_fields, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot) override;

    void traverse(const std::function<void(const DB::Page & page)> & acceptor, SnapshotPtr snapshot) override;

    bool gc(bool not_skip, const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter) override;

    void registerExternalPagesCallbacks(ExternalPagesScanner scanner, ExternalPagesRemover remover) override;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    LogWithPrefixPtr log;

    PageDirectory page_directory;

    // Used to `getMaxId`
    std::mutex pageid_mutex;

    BlobStore::Config blob_config;

    BlobStore blob_store;

    // A sequence number to keep ordering between multi-writers.
    std::atomic<WriteBatch::SequenceID> write_batch_seq = 0;

    ExternalPagesScanner external_pages_scanner = nullptr;
    ExternalPagesRemover external_pages_remover = nullptr;
};

} // namespace PS::V3
} // namespace DB
