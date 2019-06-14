#pragma once

#include <functional>
#include <optional>
#include <shared_mutex>
#include <unordered_map>

#include <Storages/Page/PageFile.h>

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

        bool sync_on_write = false;

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
    PageStorage(const std::string & storage_path, const Config & config_);

    PageId    getMaxId();
    PageCache getCache(PageId page_id);

    void    write(const WriteBatch & write_batch);
    Page    read(PageId page_id);
    PageMap read(const std::vector<PageId> & page_ids);
    void    traverse(std::function<void(const Page & page)> acceptor);
    void    traversePageCache(std::function<void(PageId page_id, const PageCache & page)> acceptor);
    bool    gc();

private:
    PageFile::Writer & getWriter();
    ReaderPtr          getReader(const PageFileIdAndLevel & file_id_level);

private:
    std::string storage_path;
    Config      config;

    PageCacheMap page_cache_map;
    PageId       max_page_id = 0;

    PageFile  write_file;
    WriterPtr write_file_writer;

    OpenReadFiles open_read_files;
    std::mutex    open_read_files_mutex; // A mutex only used to protect open_read_files.

    Logger * page_file_log;
    Logger * log;

    std::mutex        write_mutex;
    std::shared_mutex read_mutex;
};

} // namespace DB
