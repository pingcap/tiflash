#pragma once
#include <Storages/Page/V2/PageStorage.h>

#include <boost/core/noncopyable.hpp>
#include <map>
#include <tuple>

namespace DB::PS::V2
{
using WritingFilesSnapshot = PageStorage::WritingFilesSnapshot;

template <typename SnapshotPtr>
class DataCompactor : private boost::noncopyable
{
public:
    using ValidPages = std::map<PageFileIdAndLevel, std::pair<size_t, PageIdSet>>;

    // <Migrate PageFileId, num_pages>
    using MigrateInfos = std::vector<std::pair<PageFileIdAndLevel, size_t>>;

    struct Result
    {
        bool do_compaction = false;
        size_t candidate_size = 0;
        size_t num_migrate_pages = 0;
        size_t bytes_migrate = 0; // only contain migrate data part
        size_t bytes_written = 0; // written bytes of migrate file
    };

public:
    DataCompactor(const PageStorage & storage, PageStorage::Config gc_config, const WriteLimiterPtr & write_limiter_, const ReadLimiterPtr & read_limiter_);

    /**
     * Take a snapshot from PageStorage and try to migrate data if some PageFiles used rate is low.
     * The (id, level) of PageFile migrated: take the largest (id, level) of all migrate candidates,
     * use its (id, level+1) as the id and level of target PageFile.
     * All migrated data will be written as multiple WriteBatches with same sequence. To keep the
     * order of all PageFiles' meta, the sequence of WriteBatch should be maximum of all candidates'
     * WriteBatches. No matter we merge valid page(s) from that WriteBatch or not.
     *
     * Note that all types of PageFile in `page_files` should be `Formal`.
     * Those PageFile whose id in `writing_files`, theirs data will not be migrate.
     * 
     * Return DataCompactor::Result and entries edit should be applied to PageStorage's entries.
     */
    std::tuple<Result, PageEntriesEdit>
    tryMigrate(const PageFileSet & page_files, SnapshotPtr && snapshot, const WritingFilesSnapshot & writing_files);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    /**
     * Collect valid page of snapshot.
     * Return {
     *    (id, level): (valid size of this PageFile, [valid page ids, ...]),
     *    (id, level): ...,
     * }
     */
    static ValidPages collectValidPagesInPageFile(const SnapshotPtr & snapshot);

    struct CompactCandidates
    {
        // Those files with valid pages, we need to migrate those pages into a new PageFile
        PageFileSet compact_candidates;
        // Those files without valid pages, we need to log them down later
        PageFileSet files_without_valid_pages;
        // Those files have high vaild rate and big size, use hardlink to reduce write amplification
        PageFileSet hardlink_candidates;
        size_t total_valid_bytes;
        size_t num_migrate_pages;
    };

    bool
    isPageFileExistInAllPath(const PageFileIdAndLevel & file_id_and_level) const;

    CompactCandidates
    selectCandidateFiles(const PageFileSet & page_files,
                         const ValidPages & files_valid_pages,
                         const WritingFilesSnapshot & writing_files) const;

    std::tuple<PageEntriesEdit, size_t> //
    migratePages(const SnapshotPtr & snapshot,
                 const ValidPages & files_valid_pages,
                 const CompactCandidates & candidates,
                 const size_t migrate_page_count) const;

    std::tuple<PageEntriesEdit, size_t> //
    mergeValidPages(PageStorage::OpenReadFiles && data_readers,
                    const ValidPages & files_valid_pages,
                    const SnapshotPtr & snapshot,
                    const WriteBatch::SequenceID compact_sequence,
                    PageFile & gc_file,
                    MigrateInfos & migrate_infos) const;

    static PageIdAndEntries collectValidEntries(const PageIdSet & valid_pages, const SnapshotPtr & snap);

    void logMigrationDetails(const MigrateInfos & infos, const PageFileIdAndLevel & migrate_file_id) const;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    const String & storage_name;

    PSDiskDelegatorPtr delegator;
    FileProviderPtr file_provider;

    const PageStorage::Config config;

    Poco::Logger * log;
    Poco::Logger * page_file_log;

    const WriteLimiterPtr write_limiter;
    const ReadLimiterPtr read_limiter;
};

} // namespace DB::PS::V2
