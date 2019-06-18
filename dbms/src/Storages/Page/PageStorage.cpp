#include <set>

#include <IO/ReadBufferFromMemory.h>
#include <Storages/Page/PageStorage.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

std::set<PageFile, PageFile::Comparator> listAllPageFiles(std::string storage_path, bool remove_tmp_file, Logger * page_file_log)
{
    Poco::File folder(storage_path);
    if (!folder.exists())
        folder.createDirectories();
    std::vector<std::string> file_names;
    folder.list(file_names);

    if (file_names.empty())
        return {};

    std::set<PageFile, PageFile::Comparator> page_files;
    for (const auto & name : file_names)
    {
        auto [page_file, ok] = PageFile::recover(storage_path, name, page_file_log);
        if (ok)
        {
            page_files.insert(page_file);
        }
        else if (remove_tmp_file)
        {
            // Remove temporary file.
            Poco::File file(storage_path + "/" + name);
            file.remove(true);
        }
    }

    return page_files;
}

PageStorage::PageStorage(const std::string & storage_path_, const Config & config_)
    : storage_path(storage_path_), config(config_), page_file_log(&Logger::get("PageFile")), log(&Logger::get("PageStorage"))
{
    /// page_files are in ascending ordered by (file_id, level).
    auto page_files = listAllPageFiles(storage_path, true, page_file_log);
    for (auto & page_file : page_files)
    {
        const_cast<PageFile &>(page_file).readAndSetPageMetas(page_cache_map);

        // Only level 0 is writable.
        if (page_file.getLevel() == 0)
            write_file = page_file;
    }

    for (const auto & p : page_cache_map)
    {
        max_page_id = std::max(max_page_id, p.first);
    }
}

PageId PageStorage::getMaxId()
{
    std::lock_guard<std::mutex> write_lock(write_mutex);

    return max_page_id;
}

PageCache PageStorage::getCache(PageId page_id)
{
    std::shared_lock lock(read_mutex);

    auto it = page_cache_map.find(page_id);
    if (it != page_cache_map.end())
        return it->second;
    else
        return {};
}

PageFile::Writer & PageStorage::getWriter()
{
    bool is_writable = write_file.isValid() && write_file.getDataFileAppendPos() < config.file_roll_size;
    if (!is_writable)
    {
        write_file        = PageFile::newPageFile(write_file.getFileId() + 1, 0, storage_path, false, page_file_log);
        write_file_writer = write_file.createWriter(config.sync_on_write);
    }
    else if (!write_file_writer)
    {
        write_file_writer = write_file.createWriter(config.sync_on_write);
    }
    return *write_file_writer;
}

PageStorage::ReaderPtr PageStorage::getReader(const PageFileIdAndLevel & file_id_level)
{
    std::lock_guard<std::mutex> lock(open_read_files_mutex);

    auto & cached_reader = open_read_files[file_id_level];
    if (!cached_reader)
    {
        auto page_file = PageFile::openPageFileForRead(file_id_level.first, file_id_level.second, storage_path, page_file_log);
        cached_reader  = page_file.createReader();
    }
    return cached_reader;
}

void PageStorage::write(const WriteBatch & wb)
{
    PageCacheMap caches;
    {
        std::lock_guard<std::mutex> lock(write_mutex);
        getWriter().write(wb, caches);

        {
            std::unique_lock read_lock(read_mutex);

            for (const auto & w : wb.getWrites())
            {
                max_page_id = std::max(max_page_id, w.page_id);
                if (w.is_put)
                    page_cache_map[w.page_id] = caches[w.page_id];
                else
                    page_cache_map.erase(w.page_id);
            }
        }
    }
}

Page PageStorage::read(PageId page_id)
{
    std::shared_lock lock(read_mutex);

    auto it = page_cache_map.find(page_id);
    if (it == page_cache_map.end())
        throw Exception("Page " + DB::toString(page_id) + " not found", ErrorCodes::LOGICAL_ERROR);
    const auto &    page_cache    = it->second;
    auto            file_id_level = page_cache.fileIdLevel();
    PageIdAndCaches to_read       = {{page_id, page_cache}};
    auto            file_reader   = getReader(file_id_level);
    return file_reader->read(to_read)[page_id];
}

PageMap PageStorage::read(const std::vector<PageId> & page_ids)
{
    std::shared_lock lock(read_mutex);

    std::map<PageFileIdAndLevel, std::pair<PageIdAndCaches, ReaderPtr>> file_read_infos;
    for (auto page_id : page_ids)
    {
        auto it = page_cache_map.find(page_id);
        if (it == page_cache_map.end())
            throw Exception("Page " + DB::toString(page_id) + " not found", ErrorCodes::LOGICAL_ERROR);
        const auto & page_cache                  = it->second;
        auto         file_id_level               = page_cache.fileIdLevel();
        auto & [page_id_and_caches, file_reader] = file_read_infos[file_id_level];
        page_id_and_caches.emplace_back(page_id, page_cache);
        if (!file_reader)
            file_reader = getReader(file_id_level);
    }

    PageMap page_map;
    for (auto & [file_id_level, cache_and_reader] : file_read_infos)
    {
        (void)file_id_level;
        auto & page_id_and_caches = cache_and_reader.first;
        auto & reader             = cache_and_reader.second;
        auto   page_in_file       = reader->read(page_id_and_caches);
        for (auto & [page_id, page] : page_in_file)
            page_map.emplace(page_id, page);
    }
    return page_map;
}

void PageStorage::read(const std::vector<PageId> & page_ids, PageHandler & handler)
{
    std::shared_lock lock(read_mutex);

    std::map<PageFileIdAndLevel, std::pair<PageIdAndCaches, ReaderPtr>> file_read_infos;
    for (auto page_id : page_ids)
    {
        auto it = page_cache_map.find(page_id);
        if (it == page_cache_map.end())
            throw Exception("Page " + DB::toString(page_id) + " not found", ErrorCodes::LOGICAL_ERROR);
        const auto & page_cache                  = it->second;
        auto         file_id_level               = page_cache.fileIdLevel();
        auto & [page_id_and_caches, file_reader] = file_read_infos[file_id_level];
        page_id_and_caches.emplace_back(page_id, page_cache);
        if (!file_reader)
            file_reader = getReader(file_id_level);
    }

    for (auto & [file_id_level, cache_and_reader] : file_read_infos)
    {
        (void)file_id_level;
        auto & page_id_and_caches = cache_and_reader.first;
        auto & reader             = cache_and_reader.second;

        reader->read(page_id_and_caches, handler);
    }
}

void PageStorage::traverse(std::function<void(const Page & page)> acceptor)
{
    std::shared_lock lock(read_mutex);

    std::map<PageFileIdAndLevel, PageIds> file_and_pages;
    {
        for (const auto & [page_id, page_cache] : page_cache_map)
            file_and_pages[page_cache.fileIdLevel()].emplace_back(page_id);
    }

    for (const auto & p : file_and_pages)
    {
        auto pages = read(p.second);
        for (const auto & id_page : pages)
            acceptor(id_page.second);
    }
}

void PageStorage::traversePageCache(std::function<void(PageId page_id, const PageCache & page)> acceptor)
{
    std::shared_lock lock(read_mutex);

    for (const auto & [page_id, page_cache] : page_cache_map)
        acceptor(page_id, page_cache);
}


bool PageStorage::gc()
{
    auto page_files = listAllPageFiles(storage_path, true, page_file_log);
    if (page_files.empty())
        return false;

    PageFileIdAndLevel writing_file_id_level;
    {
        std::lock_guard<std::mutex> lock(write_mutex);
        writing_file_id_level = write_file.fileIdLevel();
    }

    std::set<PageFileIdAndLevel> merge_files;
    PageCacheMap                 gc_file_page_cache_map;

    {
        /// Select the GC candidates and write them into an new file.
        /// Since we don't update any shared informations, only a read lock is sufficient.

        std::shared_lock lock(read_mutex);

        std::map<PageFileIdAndLevel, std::pair<size_t, PageIds>> file_valid_pages;
        {
            for (const auto & [page_id, page_cache] : page_cache_map)
            {
                auto && [valid_size, valid_page_ids_in_file] = file_valid_pages[page_cache.fileIdLevel()];
                valid_size += page_cache.size;
                valid_page_ids_in_file.push_back(page_id);
            }
        }

        UInt64 candidate_total_size = 0;
        size_t migrate_page_count   = 0;
        for (auto & page_file : page_files)
        {
            auto   file_size = page_file.getDataFileSize();
            UInt64 valid_size;
            float  valid_rate;
            size_t valid_page_count;

            auto it = file_valid_pages.find(page_file.fileIdLevel());
            if (it == file_valid_pages.end())
            {
                valid_size       = 0;
                valid_rate       = 0;
                valid_page_count = 0;
            }
            else
            {
                valid_size       = it->second.first;
                valid_rate       = (float)valid_size / file_size;
                valid_page_count = it->second.second.size();
            }

            // Don't gc writing page file.
            bool is_candidate = page_file.fileIdLevel() != writing_file_id_level
                && (valid_rate < config.merge_hint_low_used_rate || file_size < config.file_small_size);
            if (!is_candidate)
                continue;

            merge_files.emplace(page_file.fileIdLevel());

            migrate_page_count += valid_page_count;
            candidate_total_size += valid_size;
            if (candidate_total_size >= config.file_max_size)
                break;
        }

        bool should_merge = merge_files.size() >= config.merge_hint_low_used_file_num
            || (merge_files.size() >= 2 && candidate_total_size >= config.merge_hint_low_used_file_total_size);
        if (!should_merge)
            return false;

        LOG_DEBUG(log, "GC decide to merge " << merge_files.size() << " files, containing " << migrate_page_count << " regions");

        if (migrate_page_count)
        {
            auto [largest_file_id, level] = *(merge_files.rbegin());
            PageFile gc_file              = PageFile::newPageFile(largest_file_id, level + 1, storage_path, true, page_file_log);

            {
                // No need to sync after each write. Do sync before closing is enough.
                auto gc_file_writer = gc_file.createWriter(false);

                for (const auto & file_id_level : merge_files)
                {
                    auto it = file_valid_pages.find(file_id_level);
                    if (it == file_valid_pages.end())
                    {
                        // This file does not contain any valid page.
                        continue;
                    }
                    const auto & page_ids = it->second.second;

                    PageFile to_merge_file
                        = PageFile::openPageFileForRead(file_id_level.first, file_id_level.second, storage_path, page_file_log);
                    auto to_merge_file_reader = to_merge_file.createReader();

                    PageIdAndCaches page_id_and_caches;
                    {
                        for (auto page_id : page_ids)
                        {
                            auto it2 = page_cache_map.find(page_id);
                            // This page is removed already.
                            if (it2 == page_cache_map.end())
                                continue;
                            const auto & page_cache = it2->second;
                            // This page is covered by newer file.
                            if (page_cache.fileIdLevel() != file_id_level)
                                continue;
                            page_id_and_caches.emplace_back(page_id, page_cache);
                        }
                    }

                    if (!page_id_and_caches.empty())
                    {
                        PageMap    pages = to_merge_file_reader->read(page_id_and_caches);
                        WriteBatch wb;
                        for (const auto & [page_id, page_cache] : page_id_and_caches)
                        {
                            auto & page = pages.find(page_id)->second;
                            wb.putPage(page_id,
                                       page_cache.tag,
                                       std::make_shared<ReadBufferFromMemory>(page.data.begin(), page.data.size()),
                                       page.data.size());
                        }

                        gc_file_writer->write(wb, gc_file_page_cache_map);
                    }
                }
            }

            if (gc_file_page_cache_map.empty())
            {
                gc_file.destroy();
            }
            else
            {
                gc_file.setFormal();
            }
        }
    }

    {
        /// Here we have to update the cache informations which readers need to synchronize, a write lock is needed.

        std::unique_lock lock(read_mutex);

        for (const auto & [page_id, page_cache] : gc_file_page_cache_map)
        {
            auto it = page_cache_map.find(page_id);
            if (it == page_cache_map.end())
                continue;
            auto & old_page_cache = it->second;
            // In case of page being updated during GC process.
            if (old_page_cache.fileIdLevel() < page_cache.fileIdLevel())
                old_page_cache = page_cache;
        }

        // TODO: potential bug: A read thread may just select a file F, while F is being GCed. And after GC, we remove F from
        // reader cache. But after that, A could come in and re-add F reader cache. It is not a very big issue, because
        // it only cause a hanging opened fd, which no one will use anymore.

        // Remove reader cache.
        for (const auto & [file_id, level] : merge_files)
        {
            open_read_files.erase({file_id, level});
        }
    }

    for (const auto & [file_id, level] : merge_files)
    {
        auto page_file = PageFile::openPageFileForRead(file_id, level, storage_path, page_file_log);
        page_file.destroy();
    }

    return true;
}

} // namespace DB