#include <Storages/Page/PageStorage.h>

#include <boost/core/noncopyable.hpp>
#include <map>
#include <tuple>

namespace DB
{

class DataCompactor : private boost::noncopyable
{
public:
    using ValidPages  = std::map<PageFileIdAndLevel, std::pair<size_t, PageIdSet>>;
    using SnapshotPtr = PageStorage::SnapshotPtr;

    // <Migrate PageFileId, num_pages>
    using MigrateInfos = std::map<PageFileIdAndLevel, size_t>;

    struct Result
    {
        bool   do_compaction     = false;
        size_t candidate_size    = 0;
        size_t num_migrate_pages = 0;
        size_t bytes_migrate     = 0; // only contain migrate data part
        size_t bytes_written     = 0; // written bytes of migrate file
    };

public:
    DataCompactor(const PageStorage & storage);

    std::tuple<Result, PageEntriesEdit>
    tryMigrate(const PageFileSet & page_files, SnapshotPtr && snapshot, const std::set<PageFileIdAndLevel> & writing_file_ids);


private:
    static ValidPages collectValidPagesInPageFile(const PageStorage::SnapshotPtr & snapshot);

    std::tuple<PageFileSet, size_t, size_t> selectCandidateFiles( // keep readable indent
        const PageFileSet &                  page_files,
        const ValidPages &                   file_valid_pages,
        const std::set<PageFileIdAndLevel> & writing_file_ids) const;

    std::tuple<PageEntriesEdit, size_t> //
    migratePages(const SnapshotPtr & snapshot,
                 const ValidPages &  file_valid_pages,
                 const PageFileSet & candidates,
                 const size_t        migrate_page_count) const;

    std::tuple<PageEntriesEdit, size_t> //
    mergeValidPages(PageStorage::MetaMergingQueue && merging_queue,
                    PageStorage::OpenReadFiles &&    data_readers,
                    const ValidPages &               file_valid_pages,
                    const SnapshotPtr &              snapshot,
                    const WriteBatch::SequenceID     compact_sequence,
                    PageFile &                       gc_file,
                    MigrateInfos &                   migrate_infos) const;

    PageIdAndEntries collectValidEntries(PageEntriesEdit && edits, const PageIdSet & valid_pages, const SnapshotPtr & snap) const;

    void logMigrationDetails(const MigrateInfos & infos, const PageFileIdAndLevel & migrate_file_id) const;

private:
    const String & storage_name;
    const String & storage_path;

    const PageStorage::Config & config;

    Poco::Logger * log;
    Poco::Logger * page_file_log;
};

} // namespace DB
