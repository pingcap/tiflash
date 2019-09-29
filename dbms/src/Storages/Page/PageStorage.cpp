#include <set>
#include <utility>

#include <Storages/Page/PageStorage.h>

#include <IO/ReadBufferFromMemory.h>
#include <Poco/File.h>
#include <common/logger_useful.h>
#include <ext/scope_guard.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

std::set<PageFile, PageFile::Comparator>
PageStorage::listAllPageFiles(const String & storage_path, bool remove_tmp_file, Logger * page_file_log)
{
    // collect all pages from `storage_path` and recover to `PageFile` objects
    Poco::File folder(storage_path);
    if (!folder.exists())
    {
        folder.createDirectories();
    }
    std::vector<std::string> file_names;
    folder.list(file_names);

    if (file_names.empty())
    {
        return {};
    }

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

PageStorage::PageStorage(String name, const String & storage_path_, const Config & config_)
    : storage_name(std::move(name)),
      storage_path(storage_path_),
      config(config_),
      versioned_page_entries(),
      page_file_log(&Poco::Logger::get("PageFile")),
      log(&Poco::Logger::get("PageStorage"))
{
    /// page_files are in ascending ordered by (file_id, level).
    auto page_files = PageStorage::listAllPageFiles(storage_path, /* remove_tmp_file= */ true, page_file_log);
    // recover current version from files

#ifdef DELTA_VERSION_SET
    for (auto & page_file : page_files)
    {
        PageEntriesEdit edit;
        const_cast<PageFile &>(page_file).readAndSetPageMetas(edit);

        // Only level 0 is writable.
        if (page_file.getLevel() == 0)
        {
            write_file = page_file;
        }
        // apply edit to new version
        versioned_page_entries.apply(edit);
    }
#else
    auto snapshot = versioned_page_entries.getSnapshot();

    typename PageEntriesVersionSet::BuilderType builder(snapshot->version(), true, log); // If there are invalid ref-pairs, just ignore that
    for (auto & page_file : page_files)
    {
        PageEntriesEdit edit;
        const_cast<PageFile &>(page_file).readAndSetPageMetas(edit);

        // Only level 0 is writable.
        if (page_file.getLevel() == 0)
        {
            write_file = page_file;
        }
        // apply edit to new version
        builder.apply(edit);
    }
    versioned_page_entries.restore(builder.build());
#endif
}

PageId PageStorage::getMaxId()
{
    std::lock_guard<std::mutex> write_lock(write_mutex);
    return versioned_page_entries.getSnapshot()->version()->maxId();
}

PageEntry PageStorage::getEntry(PageId page_id, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot();
    }

    try
    { // this may throw an exception if ref to non-exist page
        const auto entry = snapshot->version()->find(page_id);
        if (entry)
            return *entry; // A copy of PageEntry
        else
            return {}; // return invalid PageEntry
    }
    catch (DB::Exception & e)
    {
        LOG_WARNING(log, storage_name << " " << e.message());
        return {}; // return invalid PageEntry
    }
}

PageFile::Writer & PageStorage::getWriter()
{
    bool is_writable = write_file.isValid() && write_file.getDataFileAppendPos() < config.file_roll_size;
    if (!is_writable)
    {
        // create a new PageFile if old file is full
        write_file        = PageFile::newPageFile(write_file.getFileId() + 1, 0, storage_path, false, page_file_log);
        write_file_writer = write_file.createWriter(config.sync_on_write);
    }
    else if (write_file_writer == nullptr)
    {
        // create a Writer of current PageFile
        write_file_writer = write_file.createWriter(config.sync_on_write);
    }
    return *write_file_writer;
}

PageStorage::ReaderPtr PageStorage::getReader(const PageFileIdAndLevel & file_id_level)
{
    std::lock_guard<std::mutex> lock(open_read_files_mutex);

    auto & cached_reader = open_read_files[file_id_level];
    if (cached_reader == nullptr)
    {
        auto page_file = PageFile::openPageFileForRead(file_id_level.first, file_id_level.second, storage_path, page_file_log);
        cached_reader  = page_file.createReader();
    }
    return cached_reader;
}

void PageStorage::write(const WriteBatch & wb)
{
    if (wb.empty())
        return;

    PageEntriesEdit             edit;
    std::lock_guard<std::mutex> lock(write_mutex);
    getWriter().write(wb, edit);

    // Apply changes into versioned_page_entries(generate a new version)
    // If there are RefPages to non-exist Pages, just put the ref pair to new version
    // instead of throwing exception. Or we can't open PageStorage since we have already
    // persist the invalid ref pair into PageFile.
    versioned_page_entries.apply(edit);
}

PageStorage::SnapshotPtr PageStorage::getSnapshot()
{
    return versioned_page_entries.getSnapshot();
}

Page PageStorage::read(PageId page_id, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot();
    }

    const auto page_entry = snapshot->version()->find(page_id);
    if (!page_entry)
        throw Exception("Page " + DB::toString(page_id) + " not found", ErrorCodes::LOGICAL_ERROR);
    const auto       file_id_level = page_entry->fileIdLevel();
    PageIdAndEntries to_read       = {{page_id, *page_entry}};
    auto             file_reader   = getReader(file_id_level);
    return file_reader->read(to_read)[page_id];
}

PageMap PageStorage::read(const std::vector<PageId> & page_ids, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot();
    }

    std::map<PageFileIdAndLevel, std::pair<PageIdAndEntries, ReaderPtr>> file_read_infos;
    for (auto page_id : page_ids)
    {
        const auto page_entry = snapshot->version()->find(page_id);
        if (!page_entry)
            throw Exception("Page " + DB::toString(page_id) + " not found", ErrorCodes::LOGICAL_ERROR);
        auto file_id_level                       = page_entry->fileIdLevel();
        auto & [page_id_and_caches, file_reader] = file_read_infos[file_id_level];
        page_id_and_caches.emplace_back(page_id, *page_entry);
        if (file_reader == nullptr)
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

void PageStorage::read(const std::vector<PageId> & page_ids, const PageHandler & handler, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot();
    }

    std::map<PageFileIdAndLevel, std::pair<PageIdAndEntries, ReaderPtr>> file_read_infos;
    for (auto page_id : page_ids)
    {
        const auto page_entry = snapshot->version()->find(page_id);
        if (!page_entry)
            throw Exception("Page " + DB::toString(page_id) + " not found", ErrorCodes::LOGICAL_ERROR);
        auto file_id_level                       = page_entry->fileIdLevel();
        auto & [page_id_and_caches, file_reader] = file_read_infos[file_id_level];
        page_id_and_caches.emplace_back(page_id, *page_entry);
        if (file_reader == nullptr)
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

void PageStorage::traverse(const std::function<void(const Page & page)> & acceptor, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot();
    }

    std::map<PageFileIdAndLevel, PageIds> file_and_pages;
#ifdef DELTA_VERSION_SET
    {
        auto valid_pages_ids = snapshot->version()->validPageIds();
        for (auto page_id : valid_pages_ids)
        {
            const auto page_entry = snapshot->version()->find(page_id);
            if (unlikely(!page_entry))
                throw Exception("Page[" + DB::toString(page_id) + "] not found when traversing PageStorage", ErrorCodes::LOGICAL_ERROR);
            file_and_pages[page_entry->fileIdLevel()].emplace_back(page_id);
        }
    }
#else
    {
        for (auto iter = snapshot->version()->cbegin(); iter != snapshot->version()->cend(); ++iter)
        {
            const PageId      page_id    = iter.pageId();
            const PageEntry & page_entry = iter.pageEntry(); // this may throw an exception if ref to non-exist page
            file_and_pages[page_entry.fileIdLevel()].emplace_back(page_id);
        }
    }
#endif

    for (const auto & p : file_and_pages)
    {
        auto pages = read(p.second, snapshot);
        for (const auto & id_page : pages)
        {
            acceptor(id_page.second);
        }
    }
}

void PageStorage::traversePageEntries( //
    const std::function<void(PageId page_id, const PageEntry & page)> & acceptor,
    SnapshotPtr                                                         snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot();
    }

    // traverse over all Pages or RefPages
#ifdef DELTA_VERSION_SET
    auto valid_pages_ids = snapshot->version()->validPageIds();
    for (auto page_id : valid_pages_ids)
    {
        const auto page_entry = snapshot->version()->find(page_id);
        if (unlikely(!page_entry))
            throw Exception("Page[" + DB::toString(page_id) + "] not found when traversing PageStorage's entries",
                            ErrorCodes::LOGICAL_ERROR);
        acceptor(page_id, *page_entry);
    }
#else
    for (auto iter = snapshot->version()->cbegin(); iter != snapshot->version()->cend(); ++iter)
    {
        const PageId      page_id    = iter.pageId();
        const PageEntry & page_entry = iter.pageEntry(); // this may throw an exception if ref to non-exist page
        acceptor(page_id, page_entry);
    }
#endif
}


bool PageStorage::gc()
{
    std::lock_guard<std::mutex> gc_lock(gc_mutex);
    // get all PageFiles
    const auto page_files = PageStorage::listAllPageFiles(storage_path, true, page_file_log);
    if (page_files.empty())
    {
        return false;
    }

    PageFileIdAndLevel writing_file_id_level;
    {
        std::lock_guard<std::mutex> lock(write_mutex);
        writing_file_id_level = write_file.fileIdLevel();
    }

    std::set<PageFileIdAndLevel> merge_files;
    PageEntriesEdit              gc_file_entries_edit;

    {
        /// Select the GC candidates files and migrate valid pages into an new file.
        /// Acquire a snapshot version of page map, new edit on page map store in `gc_file_entries_edit`
        SnapshotPtr snapshot = this->getSnapshot();

        std::map<PageFileIdAndLevel, std::pair<size_t, PageIds>> file_valid_pages;
        {
            // Only scan over normal Pages, excluding RefPages
#ifdef DELTA_VERSION_SET
            auto valid_normal_page_ids = snapshot->version()->validNormalPageIds();
            for (auto page_id : valid_normal_page_ids)
            {
                const auto page_entry = snapshot->version()->findNormalPageEntry(page_id);
                if (unlikely(!page_entry))
                {
                    throw Exception("PageStorage GC: Normal Page " + DB::toString(page_id) + " not found.", ErrorCodes::LOGICAL_ERROR);
                }
                auto && [valid_size, valid_page_ids_in_file] = file_valid_pages[page_entry->fileIdLevel()];
                valid_size += page_entry->size;
                valid_page_ids_in_file.emplace_back(page_id);
            }
#else
            for (auto iter = snapshot->version()->pages_cbegin(); iter != snapshot->version()->pages_cend(); ++iter)
            {
                const PageId      page_id                    = iter->first;
                const PageEntry & page_entry                 = iter->second;
                auto && [valid_size, valid_page_ids_in_file] = file_valid_pages[page_entry.fileIdLevel()];
                valid_size += page_entry.size;
                valid_page_ids_in_file.emplace_back(page_id);
            }
#endif
        }

        // Select gc candidate files into `merge_files`
        UInt64 candidate_total_size = 0;
        size_t migrate_page_count   = 0;
        merge_files = gcSelectCandidateFiles(page_files, file_valid_pages, writing_file_id_level, candidate_total_size, migrate_page_count);

        bool should_merge = merge_files.size() >= config.merge_hint_low_used_file_num
            || (merge_files.size() >= 2 && candidate_total_size >= config.merge_hint_low_used_file_total_size);
        if (!should_merge)
        {
            LOG_TRACE(log,
                      storage_name << " GC exit without merging. merge file size: " << merge_files.size()
                                   << ", candidate size: " << candidate_total_size);
            return false;
        }

        // There are no valid pages to be migrated but valid ref pages, scan over all `merge_files` and do migrate.
        gc_file_entries_edit = gcMigratePages(snapshot, file_valid_pages, merge_files, migrate_page_count);
    }

    std::set<PageFileIdAndLevel> live_files;
    /// Here we have to apply edit to versioned_page_entries and generate a new version, then return all files that are in used
    live_files = versioned_page_entries.gcApply(gc_file_entries_edit);

    {
        // Remove obsolete files' reader cache that are not used by any version
        std::lock_guard<std::mutex> lock(open_read_files_mutex);
        for (const auto & page_file : page_files)
        {
            const auto page_id_and_lvl = page_file.fileIdLevel();
            if (page_id_and_lvl >= writing_file_id_level)
            {
                continue;
            }

            if (live_files.count(page_id_and_lvl) == 0)
            {
                open_read_files.erase(page_id_and_lvl);
            }
        }
    }

    // Delete obsolete files that are not used by any version, without lock
    gcRemoveObsoleteFiles(page_files, writing_file_id_level, live_files);
    return true;
}

PageStorage::GcCandidates PageStorage::gcSelectCandidateFiles( // keep readable indent
    const std::set<PageFile, PageFile::Comparator> & page_files,
    const GcLivesPages &                             file_valid_pages,
    const PageFileIdAndLevel &                       writing_file_id_level,
    UInt64 &                                         candidate_total_size,
    size_t &                                         migrate_page_count) const
{
    GcCandidates merge_files;
    for (auto & page_file : page_files)
    {
        const auto file_size        = page_file.getDataFileSize();
        UInt64     valid_size       = 0;
        float      valid_rate       = 0.0f;
        size_t     valid_page_count = 0;

        auto it = file_valid_pages.find(page_file.fileIdLevel());
        if (it != file_valid_pages.end())
        {
            valid_size       = it->second.first;
            valid_rate       = (float)valid_size / file_size;
            valid_page_count = it->second.second.size();
        }

        // Don't gc writing page file.
        bool is_candidate = (page_file.fileIdLevel() != writing_file_id_level)
            && (valid_rate < config.merge_hint_low_used_rate || file_size < config.file_small_size);
        if (!is_candidate)
        {
            continue;
        }

        merge_files.emplace(page_file.fileIdLevel());
        migrate_page_count += valid_page_count;
        candidate_total_size += valid_size;
        if (candidate_total_size >= config.file_max_size)
        {
            break;
        }
    }
    return merge_files;
}

PageEntriesEdit PageStorage::gcMigratePages(const SnapshotPtr &  snapshot,
                                            const GcLivesPages & file_valid_pages,
                                            const GcCandidates & merge_files,
                                            const size_t         migrate_page_count) const
{
    PageEntriesEdit gc_file_edit;

    // merge `merge_files` to PageFile which PageId = max of all `merge_files` and level = level + 1
    auto [largest_file_id, level] = *(merge_files.rbegin());

    {
        // In case that those files are hold by snapshot and do gcMigrate to same PageFile again, we need to check if gc_file is already exist.
        PageFile gc_file = PageFile::openPageFileForRead(largest_file_id, level + 1, storage_path, page_file_log);
        if (gc_file.isExist())
        {
            LOG_INFO(log, storage_name << " GC migration to PageFile_" << largest_file_id << "_" << level + 1 << " is done before.");
            return gc_file_edit;
        }
    }

    // Create a tmp PageFile for migration
    PageFile gc_file = PageFile::newPageFile(largest_file_id, level + 1, storage_path, /* is_tmp= */ true, page_file_log);
    LOG_INFO(log,
             storage_name << " GC decide to merge " << merge_files.size() << " files, containing " << migrate_page_count
                          << " regions to PageFile_" << largest_file_id << "_" << level + 1);

    // We should check these nums, if any of them is non-zero, we should set `gc_file` to formal.
    size_t num_successful_migrate_pages = 0;
    size_t num_valid_ref_pages          = 0;
    size_t num_del_page_meta            = 0;
    auto * current                      = snapshot->version();
    {
        PageEntriesEdit legacy_edit; // All page entries in `merge_files`
        // No need to sync after each write. Do sync before closing is enough.
        auto gc_file_writer = gc_file.createWriter(/* sync_on_write= */ false);

        for (const auto & file_id_level : merge_files)
        {
            PageFile to_merge_file = PageFile::openPageFileForRead(file_id_level.first, file_id_level.second, storage_path, page_file_log);
            // Note: This file may not contain any valid page, but valid RefPages which we need to migrate
            to_merge_file.readAndSetPageMetas(legacy_edit);

            auto it = file_valid_pages.find(file_id_level);
            if (it == file_valid_pages.end())
            {
                // This file does not contain any valid page.
                continue;
            }

            PageIdAndEntries page_id_and_entries; // The valid pages that we need to migrate to `gc_file`
            auto             to_merge_file_reader = to_merge_file.createReader();
            {
                const auto & page_ids = it->second.second;
                for (auto page_id : page_ids)
                {
                    try
                    {
                        const auto page_entry = current->findNormalPageEntry(page_id);
                        if (!page_entry)
                            continue;
                        // This page is covered by newer file.
                        if (page_entry->fileIdLevel() != file_id_level)
                            continue;
                        page_id_and_entries.emplace_back(page_id, *page_entry);
                        num_successful_migrate_pages += 1;
                    }
                    catch (DB::Exception & e)
                    {
                        // ignore if it2 is a ref to non-exist page
                        LOG_WARNING(log, storage_name << " Ignore invalid RefPage while gcMigratePages: " << e.message());
                    }
                }
            }

            if (!page_id_and_entries.empty())
            {
                // copy valid pages from `to_merge_file` to `gc_file`
                PageMap    pages = to_merge_file_reader->read(page_id_and_entries);
                WriteBatch wb;
                for (const auto & [page_id, page_cache] : page_id_and_entries)
                {
                    auto & page = pages.find(page_id)->second;
                    wb.putPage(page_id,
                               page_cache.tag,
                               std::make_shared<ReadBufferFromMemory>(page.data.begin(), page.data.size()),
                               page.data.size());
                }

                gc_file_writer->write(wb, gc_file_edit);
            }
        }

        {
            // Migrate valid RefPages and DelPage.
            WriteBatch batch;
            for (const auto & rec : legacy_edit.getRecords())
            {
                if (rec.type == WriteBatch::WriteType::REF)
                {
                    // Get `normal_page_id` from memory's `page_entry_map`. Note: can not get `normal_page_id` from disk,
                    // if it is a record of RefPage to another RefPage, the later ref-id is resolve to the actual `normal_page_id`.
                    auto [is_ref, normal_page_id] = current->isRefId(rec.page_id);
                    if (is_ref)
                    {
                        batch.putRefPage(rec.page_id, normal_page_id);
                        num_valid_ref_pages += 1;
                    }
                }
                else if (rec.type == WriteBatch::WriteType::DEL)
                {
                    // DelPage should be migrate to new PageFile
                    batch.delPage(rec.page_id);
                    num_del_page_meta += 1;
                }
            }
            gc_file_writer->write(batch, gc_file_edit);
        }
    } // free gc_file_writer and sync

    const auto id = gc_file.fileIdLevel();
    if (gc_file_edit.empty() && num_valid_ref_pages == 0 && num_del_page_meta == 0)
    {
        LOG_INFO(log, storage_name << " No valid pages, deleting PageFile_" << id.first << "_" << id.second);
        gc_file.destroy();
    }
    else
    {
        gc_file.setFormal();
        LOG_INFO(log,
                 storage_name << " GC have migrated " << num_successful_migrate_pages //
                              << " regions and " << num_valid_ref_pages               //
                              << " RefPages and " << num_del_page_meta                //
                              << " DelPage to PageFile_" << id.first << "_" << id.second);
    }
    return gc_file_edit;
}

/**
 * Delete obsolete files that are not used by any version
 * @param page_files            All available files in disk
 * @param writing_file_id_level The PageFile id which is writing to
 * @param live_files            The live files after gc
 */
void PageStorage::gcRemoveObsoleteFiles(const std::set<PageFile, PageFile::Comparator> & page_files,
                                        const PageFileIdAndLevel &                       writing_file_id_level,
                                        const std::set<PageFileIdAndLevel> &             live_files)
{
    for (const auto & page_file : page_files)
    {
        const auto page_id_and_lvl = page_file.fileIdLevel();
        if (page_id_and_lvl >= writing_file_id_level)
        {
            continue;
        }

        if (live_files.count(page_id_and_lvl) == 0)
        {
            // the page file is not used by any version, remove the page file in disk
            page_file.destroy();
        }
    }
}

} // namespace DB
