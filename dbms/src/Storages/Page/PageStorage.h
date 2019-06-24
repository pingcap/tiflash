#pragma once

#include <functional>
#include <optional>
#include <set>
#include <shared_mutex>
#include <unordered_map>

#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageEntryMap.h>
#include <Storages/Page/PageFile.h>
#include <Storages/Page/WriteBatch.h>

namespace DB
{

/**
 * A storage system stored pages. Pages are serialized objects referenced by PageId. Store Page with the same PageId
 * will covered the old ones. The file used to persist the Pages called PageFile. The meta data of a Page, like the
 * latest PageFile the Page is stored , the offset in file, and checksum, are cached in memory. Users should call
 * #gc() constantly to clean up the sparse PageFiles and release disk space.
 *
 * This class is multi-threads safe. Support single thread write, and multi threads read.
 */
class PageStorage
{
public:
    struct Config
    {
        Config() {}

        bool sync_on_write = true;

        size_t file_roll_size  = PAGE_FILE_ROLL_SIZE;
        size_t file_max_size   = PAGE_FILE_MAX_SIZE;
        size_t file_small_size = PAGE_FILE_SMALL_SIZE;

        Float64 merge_hint_low_used_rate            = 0.35;
        size_t  merge_hint_low_used_file_total_size = PAGE_FILE_ROLL_SIZE;
        size_t  merge_hint_low_used_file_num        = 10;
    };

    using WriterPtr     = std::unique_ptr<PageFile::Writer>;
    using ReaderPtr     = std::shared_ptr<PageFile::Reader>;
    using OpenReadFiles = std::map<PageFileIdAndLevel, ReaderPtr>;

public:
    PageStorage(const String & storage_path, const Config & config_);

    PageId    getMaxId();
    PageEntry getEntry(PageId page_id);

    void    write(const WriteBatch & write_batch);
    Page    read(PageId page_id);
    PageMap read(const std::vector<PageId> & page_ids);
    void    read(const std::vector<PageId> & page_ids, PageHandler & handler);
    void    traverse(const std::function<void(const Page & page)> & acceptor);
    void    traversePageEntries(const std::function<void(PageId page_id, const PageEntry & page)> & acceptor);
    bool    gc();

    static std::set<PageFile, PageFile::Comparator>
    listAllPageFiles(const String & storage_path, bool remove_tmp_file, Poco::Logger * page_file_log);

private:
    PageFile::Writer & getWriter();
    ReaderPtr          getReader(const PageFileIdAndLevel & file_id_level);
    // gc helper functions
    using GcCandidates = std::set<PageFileIdAndLevel>;
    using GcLivesPages = std::map<PageFileIdAndLevel, std::pair<size_t, PageIds>>;
    GcCandidates gcSelectCandidateFiles(const std::set<PageFile, PageFile::Comparator> & page_files,
                                        const GcLivesPages &                             file_valid_pages,
                                        const PageFileIdAndLevel &                       writing_file_id_level,
                                        UInt64 &                                         candidate_total_size,
                                        size_t &                                         migrate_page_count) const;
    PageEntryMap gcMigratePages(const GcLivesPages & file_valid_pages, const GcCandidates & merge_files) const;
    void         gcUpdatePageMap(const PageEntryMap & gc_pages_map);

private:
    String storage_path;
    Config config;

    PageEntryMap page_entry_map;
    PageId       max_page_id = 0;

    PageFile  write_file;
    WriterPtr write_file_writer;

    OpenReadFiles open_read_files;
    std::mutex    open_read_files_mutex; // A mutex only used to protect open_read_files.

    Poco::Logger * page_file_log;
    Poco::Logger * log;

    std::mutex        write_mutex;
    std::shared_mutex read_mutex;
    std::mutex        gc_mutex; // A mutex used to protect gc
};

} // namespace DB
