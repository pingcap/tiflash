#pragma once

#include <functional>
#include <optional>
#include <unordered_map>

#include <Storages/Page/PageFile.h>

namespace DB
{

/// Support single thread write, and multi-thread read.
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

    PageCache getCache(PageId page_id);

    void    write(const WriteBatch & write_batch);
    PageMap read(std::vector<PageId> page_ids);
    void    traverse(std::function<void(const Page & page)> acceptor);
    bool    gc();

private:
    PageFile::Writer & getWriter();
    ReaderPtr          getReader(const PageFileIdAndLevel & file_id_level);

private:
    std::string storage_path;
    Config      config;

    PageCacheMap page_cache_map;

    PageFile  write_file;
    WriterPtr write_file_writer;

    OpenReadFiles open_read_files;

    Logger * page_file_log;
    Logger * log;

    std::mutex mutex;
};

} // namespace DB
