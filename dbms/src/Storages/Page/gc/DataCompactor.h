#include <Storages/Page/PageStorage.h>

namespace DB
{

class DataCompactor
{
public:
    using Candidates  = std::set<PageFileIdAndLevel>;
    using ValidPages  = std::map<PageFileIdAndLevel, std::pair<size_t, PageIds>>;
    using SnapshotPtr = PageStorage::SnapshotPtr;

    struct MigrateInfo
    {
        PageFileIdAndLevel     file_id   = {0, 0};
        size_t                 num_pages = 0;
        WriteBatch::SequenceID sequence  = 0;
    };

    struct Result
    {
        bool   do_compaction     = false;
        size_t candidate_size    = 0;
        size_t num_migrate_pages = 0;
        size_t bytes_migrate     = 0;
    };

public:
    DataCompactor(const PageStorage & storage);

    std::tuple<Result, PageEntriesEdit>
    tryMigrate(const PageFileSet & page_files, SnapshotPtr && snapshot, const std::set<PageFileIdAndLevel> & writing_file_ids);


private:
    static ValidPages collectValidPagesInPageFile(const PageStorage::SnapshotPtr & snapshot);

    std::tuple<Candidates, size_t, size_t> selectCandidateFiles( // keep readable indent
        const PageFileSet &                  page_files,
        const ValidPages &                   file_valid_pages,
        const std::set<PageFileIdAndLevel> & writing_file_ids) const;

    bool isMigrateFileExist(const PageFileIdAndLevel & file_id) const;

    PageEntriesEdit migratePages(const SnapshotPtr & snapshot,
                                 const ValidPages &  file_valid_pages,
                                 const Candidates &  candidates,
                                 const size_t        migrate_page_count) const;

    void logMigrationDetails(const std::vector<MigrateInfo> & infos, const PageFileIdAndLevel & migrate_file_id) const;

private:
    const String & storage_name;
    const String & storage_path;

    const PageStorage::Config & config;

    Poco::Logger * log;
    Poco::Logger * page_file_log;
};

} // namespace DB
