#include <Storages/Page/PageStorage.h>

namespace DB::PS::V3
{
// TBD :
class PageStorageGCRules
{
public:
    enum FlatTypes
    {
        TRUNCATE_FILE_END = 1,
        SEQUENCE_AT_END = 2,
    };

    class PageStorageGCRule : Allocator<false>
    {
    public:
        bool measure();
        void flat();
        void getType();

    private:
        int type;
    };

    PageStorageGCRules(); //todo

private:
    std::vector<PageStorageGCRule> rules;
};

class PageStorage : public DB::PageStorage
{
public:
    PageStorage(String name,
                PSDiskDelegatorPtr delegator, //
                const Config & config_,
                const FileProviderPtr & file_provider_);

    ~PageStorage(){};

    void restore() override;

    void drop() override;

    PageId getMaxId() override;

    SnapshotPtr getSnapshot() override;

    std::tuple<size_t, double, unsigned> getSnapshotsStat() const override;

    void write(WriteBatch && write_batch, const WriteLimiterPtr & write_limiter = nullptr) override;

    PageEntry getEntry(PageId page_id, SnapshotPtr snapshot = {}) override;

    Page read(PageId page_id, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {}) override;

    PageMap read(const std::vector<PageId> & page_ids, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {}) override;

    void read(const std::vector<PageId> & page_ids, const PageHandler & handler, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {}) override;

    PageMap read(const std::vector<PageReadFields> & page_fields, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {}) override;

    void traverse(const std::function<void(const DB::Page & page)> & acceptor, SnapshotPtr snapshot = {}) override;

    void traversePageEntries(const std::function<void(PageId page_id, const PageEntry & page)> & acceptor, SnapshotPtr snapshot) override;

    PageId getNormalPageId(PageId page_id, SnapshotPtr snapshot = {}) override;

    bool gc(bool not_skip = false, const WriteLimiterPtr & write_limiter = nullptr, const ReadLimiterPtr & read_limiter = nullptr) override;

    void registerExternalPagesCallbacks(ExternalPagesScanner scanner, ExternalPagesRemover remover) override;
#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    Poco::Logger * log;
};

} // namespace DB::PS::V3