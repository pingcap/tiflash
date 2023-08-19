// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Storages/Page/V1/PageStorage.h>
#include <common/logger_useful.h>

#include <ext/scope_guard.h>
#include <set>
#include <utility>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

namespace PS::V1
{
std::set<PageFile, PageFile::Comparator>
PageStorage::listAllPageFiles(const String & storage_path, const FileProviderPtr & file_provider, Poco::Logger * page_file_log, ListPageFilesOption option)
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
        if (name == PageStorage::ARCHIVE_SUBDIR)
            continue;

        auto [page_file, page_file_type] = PageFile::recover(storage_path, file_provider, name, page_file_log);
        if (page_file_type == PageFile::Type::Formal)
            page_files.insert(page_file);
        else if (page_file_type == PageFile::Type::Legacy)
        {
            if (!option.ignore_legacy)
                page_files.insert(page_file);
        }
        else if (page_file_type == PageFile::Type::Checkpoint)
        {
            if (!option.ignore_checkpoint)
                page_files.insert(page_file);
        }
        else
        {
            // For Temp and Invalid
            if (option.remove_tmp_files)
            {
                if (page_file_type == PageFile::Type::Temp)
                {
                    page_file.deleteEncryptionInfo();
                }
                // Remove temporary file.
                Poco::File file(storage_path + "/" + name);
                file.remove(true);
            }
        }
    }

    return page_files;
}

std::optional<PageFile> PageStorage::tryGetCheckpoint(const String & storage_path, const FileProviderPtr & file_provider, Poco::Logger * page_file_logger, bool remove_old)
{
    Poco::File folder(storage_path);
    if (!folder.exists())
    {
        folder.createDirectories();
    }
    std::vector<std::string> file_names;
    folder.list(file_names);

    if (file_names.empty())
        return {};

    std::optional<PageFile> ret = {};
    std::vector<PageFile> checkpoints;
    for (const auto & name : file_names)
    {
        auto [page_file, page_file_type] = PageFile::recover(storage_path, file_provider, name, page_file_logger);
        if (page_file_type == PageFile::Type::Checkpoint)
        {
            ret = page_file;
            checkpoints.emplace_back(page_file);
        }
    }

    if (remove_old)
    {
        for (auto & page_file : checkpoints)
        {
            if (page_file.fileIdLevel() != ret->fileIdLevel())
                page_file.destroy();
        }
    }

    return ret;
}

PageStorage::PageStorage(String name, const String & storage_path_, const Config & config_, const FileProviderPtr & file_provider_)
    : storage_name(std::move(name))
    , storage_path(storage_path_)
    , config(config_)
    , file_provider(file_provider_)
    , page_file_log(&Poco::Logger::get("PageFile"))
    , log(&Poco::Logger::get("PageStorage"))
    , versioned_page_entries(config.version_set_config, log)
{
    /// page_files are in ascending ordered by (file_id, level).
    ListPageFilesOption opt;
    opt.remove_tmp_files = true;
    opt.ignore_checkpoint = true;
    auto page_files = PageStorage::listAllPageFiles(storage_path, file_provider, page_file_log, opt);
    // recover current version from both formal and legacy page files

#ifdef DELTA_VERSION_SET
    // Remove old checkpoints and archieve obsolete PageFiles that have not been archieved yet during gc for some reason.
    auto checkpoint_file = PageStorage::tryGetCheckpoint(storage_path, file_provider, page_file_log, true);
    if (checkpoint_file)
    {
        std::set<PageFile, PageFile::Comparator> page_files_to_archieve;
        for (auto itr = page_files.begin(); itr != page_files.end();)
        {
            if (itr->fileIdLevel() < checkpoint_file->fileIdLevel() //
                || itr->fileIdLevel() == checkpoint_file->fileIdLevel())
            {
                page_files_to_archieve.insert(*itr);
                itr = page_files.erase(itr);
            }
            else
            {
                itr++;
            }
        }

        archievePageFiles(page_files_to_archieve);
        page_files.insert(*checkpoint_file);
    }

    bool has_reusable_pagefile = false;
    PageFileId max_file_id = 0;
    for (const auto & page_file : page_files)
    {
        try
        {
            PageEntriesEdit edit;
            const_cast<PageFile &>(page_file).readAndSetPageMetas(edit);

            // Only level 0 is writable.
            if (page_file.getLevel() == 0 && page_file.getType() == PageFile::Type::Formal
                && page_file.reusableForWrite())
            {
                has_reusable_pagefile = true;
                write_file = page_file;
            }
            if (page_file.getFileId() > max_file_id)
            {
                max_file_id = page_file.getFileId();
            }

            // apply edit to new version
            versioned_page_entries.apply(edit);
        }
        catch (Exception & e)
        {
            /// Better diagnostics.
            e.addMessage("(while applying edit from " + page_file.folderPath() + " to PageStorage: " + storage_name + ")");
            throw;
        }
    }
    if (!has_reusable_pagefile)
    {
        auto page_file
            = PageFile::newPageFile(max_file_id + 1, 0, storage_path, file_provider, PageFile::Type::Formal, page_file_log);
        page_file.createEncryptionInfo();
        LOG_FMT_DEBUG(log, "{} No PageFile can be reused for write, create new PageFile_{}_0 for write", storage_name, (max_file_id + 1));
        write_file = page_file;
        write_file_writer = write_file.createWriter(config.sync_on_write, true);
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
    std::lock_guard write_lock(write_mutex);
    return versioned_page_entries.getSnapshot()->version()->maxId();
}

PageId PageStorage::getNormalPageId(PageId page_id, SnapshotPtr snapshot)
{
    if (!snapshot)
    {
        snapshot = this->getSnapshot();
    }

    auto [is_ref_id, normal_page_id] = snapshot->version()->isRefId(page_id);
    return is_ref_id ? normal_page_id : page_id;
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
        LOG_FMT_WARNING(log, "{} {}", storage_name, e.message());
        return {}; // return invalid PageEntry
    }
}

PageFile::Writer & PageStorage::getWriter()
{
    bool is_writable = write_file.isValid() && write_file.getDataFileAppendPos() < config.file_roll_size;
    if (!is_writable)
    {
        // create a new PageFile if old file is full
        write_file = PageFile::newPageFile(write_file.getFileId() + 1, 0, storage_path, file_provider, PageFile::Type::Formal, page_file_log);
        write_file_writer = write_file.createWriter(config.sync_on_write, true);
    }
    else if (write_file_writer == nullptr)
    {
        // create a Writer of current PageFile
        write_file_writer = write_file.createWriter(config.sync_on_write, false);
    }
    return *write_file_writer;
}

PageStorage::ReaderPtr PageStorage::getReader(const PageFileIdAndLevel & file_id_level)
{
    std::lock_guard lock(open_read_files_mutex);

    auto & pages_reader = open_read_files[file_id_level];
    if (pages_reader == nullptr)
    {
        auto page_file
            = PageFile::openPageFileForRead(file_id_level.first, file_id_level.second, storage_path, file_provider, PageFile::Type::Formal, page_file_log);
        pages_reader = page_file.createReader();
    }
    return pages_reader;
}

void PageStorage::write(const WriteBatch & wb)
{
    if (wb.empty())
        return;

    PageEntriesEdit edit;
    std::lock_guard lock(write_mutex);
    getWriter().write(wb, edit);

    // Apply changes into versioned_page_entries(generate a new version)
    // If there are RefPages to non-exist Pages, just put the ref pair to new version
    // instead of throwing exception. Or we can't open PageStorage since we have already
    // persist the invalid ref pair into PageFile.
    versioned_page_entries.apply(edit);

    for (const auto & w : wb.getWrites())
    {
        switch (w.type)
        {
        case WriteBatch::WriteType::DEL:
            deletes++;
            break;
        case WriteBatch::WriteType::PUT:
            puts++;
            break;
        case WriteBatch::WriteType::REF:
            refs++;
            break;
        case WriteBatch::WriteType::UPSERT:
            upserts++;
            break;
        default:
            throw Exception("Unexpected type " + DB::toString(static_cast<UInt64>(w.type)));
        }
    }
}

PageStorage::SnapshotPtr PageStorage::getSnapshot()
{
    return versioned_page_entries.getSnapshot();
}

size_t PageStorage::getNumSnapshots() const
{
    return versioned_page_entries.size();
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
    const auto file_id_level = page_entry->fileIdLevel();
    PageIdAndEntries to_read = {{page_id, *page_entry}};
    auto file_reader = getReader(file_id_level);
    return file_reader->read(to_read)[page_id];
}

PageMap PageStorage::read(const PageIds & page_ids, SnapshotPtr snapshot)
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
        auto file_id_level = page_entry->fileIdLevel();
        auto & [page_id_and_entries, file_reader] = file_read_infos[file_id_level];
        page_id_and_entries.emplace_back(page_id, *page_entry);
        if (file_reader == nullptr)
            file_reader = getReader(file_id_level);
    }

    PageMap page_map;
    for (auto & [file_id_level, entries_and_reader] : file_read_infos)
    {
        (void)file_id_level;
        auto & page_id_and_entries = entries_and_reader.first;
        auto & reader = entries_and_reader.second;
        auto page_in_file = reader->read(page_id_and_entries);
        for (auto & [page_id, page] : page_in_file)
            page_map.emplace(page_id, page);
    }
    return page_map;
}

void PageStorage::read(const PageIds & page_ids, const PageHandler & handler, SnapshotPtr snapshot)
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
        auto file_id_level = page_entry->fileIdLevel();
        auto & [page_id_and_entries, file_reader] = file_read_infos[file_id_level];
        page_id_and_entries.emplace_back(page_id, *page_entry);
        if (file_reader == nullptr)
            file_reader = getReader(file_id_level);
    }

    for (auto & [file_id_level, entries_and_reader] : file_read_infos)
    {
        (void)file_id_level;
        auto & page_id_and_entries = entries_and_reader.first;
        auto & reader = entries_and_reader.second;

        reader->read(page_id_and_entries, handler);
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
            const PageId page_id = iter.pageId();
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

void PageStorage::registerExternalPagesCallbacks(ExternalPagesScanner scanner, ExternalPagesRemover remover)
{
    assert(scanner != nullptr);
    assert(remover != nullptr);
    external_pages_scanner = scanner;
    external_pages_remover = remover;
}

bool PageStorage::gc()
{
    // If another thread is running gc, just return;
    bool v = false;
    if (!gc_is_running.compare_exchange_strong(v, true))
        return false;

    SCOPE_EXIT({
        bool is_running = true;
        gc_is_running.compare_exchange_strong(is_running, false);
    });

    LOG_FMT_TRACE(log, "{} Before gc, deletes[{}], puts[{}], refs[{}], upserts[{}]", storage_name, deletes, puts, refs, upserts);

    /// Get all pending external pages and PageFiles. Note that we should get external pages before PageFiles.
    PathAndIdsVec external_pages;
    if (external_pages_scanner)
    {
        external_pages = external_pages_scanner();
    }
    ListPageFilesOption opt;
    opt.remove_tmp_files = true;
    auto page_files = PageStorage::listAllPageFiles(storage_path, file_provider, page_file_log, opt);
    if (page_files.empty())
    {
        return false;
    }

    PageFileIdAndLevel writing_file_id_level;
    {
        std::lock_guard lock(write_mutex);
        writing_file_id_level = write_file.fileIdLevel();
    }

    {
        // Compact consecutive Legacy PageFiles into a snapshot
        page_files = gcCompactLegacy(std::move(page_files));
    }

    bool should_merge = false;
    UInt64 candidate_total_size = 0;

    std::set<PageFileIdAndLevel> merge_files;
    PageEntriesEdit gc_file_entries_edit;

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
                const PageId page_id = iter->first;
                const PageEntry & page_entry = iter->second;
                auto && [valid_size, valid_page_ids_in_file] = file_valid_pages[page_entry.fileIdLevel()];
                valid_size += page_entry.size;
                valid_page_ids_in_file.emplace_back(page_id);
            }
#endif
        }

        // Select gc candidate files into `merge_files`
        size_t migrate_page_count = 0;
        merge_files = gcSelectCandidateFiles(page_files, file_valid_pages, writing_file_id_level, candidate_total_size, migrate_page_count);
        should_merge = merge_files.size() >= config.merge_hint_low_used_file_num
            || (merge_files.size() >= 2 && candidate_total_size >= config.merge_hint_low_used_file_total_size);
        // There are no valid pages to be migrated but valid ref pages, scan over all `merge_files` and do migrate.
        if (should_merge)
        {
            gc_file_entries_edit = gcMigratePages(snapshot, file_valid_pages, merge_files, migrate_page_count);
        }
    }

    /// Here we have to apply edit to versioned_page_entries and generate a new version, then return all files that are in used
    auto [live_files, live_normal_pages] = versioned_page_entries.gcApply(gc_file_entries_edit, external_pages_scanner != nullptr);

    {
        // Remove obsolete files' reader cache that are not used by any version
        std::lock_guard lock(open_read_files_mutex);
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
    gcRemoveObsoleteData(page_files, writing_file_id_level, live_files);

    // Invoke callback with valid normal page id after gc.
    if (external_pages_remover)
    {
        external_pages_remover(external_pages, live_normal_pages);
    }

    if (!should_merge)
        LOG_FMT_TRACE(log, "{} GC exit without merging. merge file size: {}, candidate size: {}", storage_name, merge_files.size(), candidate_total_size);
    return should_merge;
}

PageStorage::GcCandidates PageStorage::gcSelectCandidateFiles( // keep readable indent
    const std::set<PageFile, PageFile::Comparator> & page_files,
    const GcLivesPages & file_valid_pages,
    const PageFileIdAndLevel & writing_file_id_level,
    UInt64 & candidate_total_size,
    size_t & migrate_page_count) const
{
    GcCandidates merge_files;
    for (const auto & page_file : page_files)
    {
        if (unlikely(page_file.getType() != PageFile::Type::Formal))
        {
            throw Exception("Try to pick PageFile_" + DB::toString(page_file.getFileId()) + "_" + DB::toString(page_file.getLevel()) + "("
                                + PageFile::typeToString(page_file.getType()) + ") as gc candidate, path: " + page_file.folderPath(),
                            ErrorCodes::LOGICAL_ERROR);
        }

        const auto file_size = page_file.getDataFileSize();
        UInt64 valid_size = 0;
        float valid_rate = 0.0f;
        size_t valid_page_count = 0;

        auto it = file_valid_pages.find(page_file.fileIdLevel());
        if (it != file_valid_pages.end())
        {
            valid_size = it->second.first;
            valid_rate = static_cast<float>(valid_size) / file_size;
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

std::set<PageFile, PageFile::Comparator> PageStorage::gcCompactLegacy(std::set<PageFile, PageFile::Comparator> && page_files)
{
    // Select PageFiles to compact
    std::set<PageFile, PageFile::Comparator> page_files_to_compact;
    for (const auto & page_file : page_files)
    {
        auto page_file_type = page_file.getType();

        if (page_file_type == PageFile::Type::Legacy)
            page_files_to_compact.emplace(page_file);
        else if (page_file_type == PageFile::Type::Checkpoint)
            page_files_to_compact.emplace(page_file);
        else
            break;
    }

    if (page_files_to_compact.size() < config.gc_compact_legacy_min_num)
    {
        // Nothing to compact, remove legacy/checkpoint page files since we
        // don't do gc on them later.
        for (auto itr = page_files.begin(); itr != page_files.end(); /* empty */)
        {
            const auto & page_file = *itr;
            if (page_file.getType() == PageFile::Type::Legacy || page_file.getType() == PageFile::Type::Checkpoint)
            {
                itr = page_files.erase(itr);
            }
            else
            {
                itr++;
            }
        }
        return std::move(page_files);
    }

    // Build a version_set with snapshot
    PageEntriesVersionSetWithDelta version_set(config.version_set_config, log);
    for (const auto & page_file : page_files_to_compact)
    {
        PageEntriesEdit edit;
        const_cast<PageFile &>(page_file).readAndSetPageMetas(edit);

        version_set.apply(edit);
    }

    auto snapshot = version_set.getSnapshot();
    WriteBatch wb;
    prepareSnapshotWriteBatch(snapshot, wb);

    // Use the largest id-level in page_files_to_compact as Snapshot's
    const PageFileIdAndLevel largest_id_level = page_files_to_compact.rbegin()->fileIdLevel();
    {
        const auto smallest_id_level = page_files_to_compact.begin()->fileIdLevel();
        LOG_FMT_INFO(log, "{} Compact legacy PageFile_{}_{} to PageFile_{}_{} into checkpoint PageFile_{}_{}", storage_name, smallest_id_level.first, smallest_id_level.second, largest_id_level.first, largest_id_level.second, largest_id_level.first, largest_id_level.second);
    }
    auto checkpoint_file = PageFile::newPageFile(largest_id_level.first, largest_id_level.second, storage_path, file_provider, PageFile::Type::Temp, log);
    {
        auto checkpoint_writer = checkpoint_file.createWriter(false, true);

        PageEntriesEdit edit;
        checkpoint_writer->write(wb, edit);
    }

    checkpoint_file.setCheckpoint();

    // Archieve obsolete PageFiles
    {
        std::set<PageFile, PageFile::Comparator> page_files_to_archieve;
        for (auto itr = page_files.begin(); itr != page_files.end();)
        {
            const auto & page_file = *itr;
            if (page_file.fileIdLevel() < largest_id_level || page_file.fileIdLevel() == largest_id_level)
            {
                page_files_to_archieve.insert(page_file);
                // Remove page files have been archieved
                itr = page_files.erase(itr);
            }
            else if (page_file.getType() == PageFile::Type::Legacy)
            {
                // Remove legacy page files since we don't do gc on them later
                itr = page_files.erase(itr);
            }
            else
            {
                itr++;
            }
        }

        archievePageFiles(page_files_to_archieve);
    }

    return std::move(page_files);
}

void PageStorage::prepareSnapshotWriteBatch(const SnapshotPtr snapshot, WriteBatch & wb)
{
    // First Ingest exists pages with normal_id
    auto normal_ids = snapshot->version()->validNormalPageIds();
    for (const auto & page_id : normal_ids)
    {
        auto page = snapshot->version()->findNormalPageEntry(page_id);
        // upsert page size in checkpoint is 0
        wb.upsertPage(page_id, page->tag, nullptr, 0);
    }

    // After ingesting normal_pages, we will ref them manually to ensure the ref-count is correct.
    auto ref_ids = snapshot->version()->validPageIds();
    for (const auto & page_id : ref_ids)
    {
        auto ori_id = snapshot->version()->isRefId(page_id).second;
        wb.putRefPage(page_id, ori_id);
    }
}

void PageStorage::archievePageFiles(const std::set<PageFile, PageFile::Comparator> & page_files)
{
    if (page_files.empty())
        return;

    const Poco::Path archive_path(storage_path, PageStorage::ARCHIVE_SUBDIR);
    Poco::File archive_dir(archive_path);
    if (!archive_dir.exists())
        archive_dir.createDirectory();

    for (const auto & page_file : page_files)
    {
        Poco::Path path(page_file.folderPath());
        auto dest = archive_path.toString() + "/" + path.getFileName();
        Poco::File file(path);
        if (file.exists())
            file.moveTo(dest);
    }
    LOG_FMT_INFO(log, "{} archive {} files to {}", storage_name, page_files.size(), archive_path.toString());
}

PageEntriesEdit PageStorage::gcMigratePages(const SnapshotPtr & snapshot,
                                            const GcLivesPages & file_valid_pages,
                                            const GcCandidates & merge_files,
                                            const size_t migrate_page_count) const
{
    PageEntriesEdit gc_file_edit;
    if (merge_files.empty())
        return gc_file_edit;

    // merge `merge_files` to PageFile which PageId = max of all `merge_files` and level = level + 1
    auto [largest_file_id, level] = *(merge_files.rbegin());

    {
        // In case that those files are hold by snapshot and do gcMigrate to same PageFile again, we need to check if gc_file is already exist.
        PageFile gc_file = PageFile::openPageFileForRead(largest_file_id, level + 1, storage_path, file_provider, PageFile::Type::Formal, page_file_log);
        if (gc_file.isExist())
        {
            LOG_FMT_INFO(log, "{} GC migration to PageFile_{}_{} is done before.", storage_name, largest_file_id, level + 1);
            return gc_file_edit;
        }
    }

    // Create a tmp PageFile for migration
    PageFile gc_file = PageFile::newPageFile(largest_file_id, level + 1, storage_path, file_provider, PageFile::Type::Temp, page_file_log);
    LOG_FMT_INFO(log, "{} GC decide to merge {} files, containing {} regions to PageFile_{}_{}", storage_name, merge_files.size(), migrate_page_count, gc_file.getFileId(), gc_file.getLevel());

    // We should check these nums, if any of them is non-zero, we should set `gc_file` to formal.
    size_t num_successful_migrate_pages = 0;
    size_t num_valid_ref_pages = 0;
    size_t num_del_page_meta = 0;
    const auto * current = snapshot->version();
    {
        PageEntriesEdit legacy_edit; // All page entries in `merge_files`
        // No need to sync after each write. Do sync before closing is enough.
        auto gc_file_writer = gc_file.createWriter(/* sync_on_write= */ false, true);

        for (const auto & file_id_level : merge_files)
        {
            PageFile to_merge_file = PageFile::openPageFileForRead(
                file_id_level.first,
                file_id_level.second,
                storage_path,
                file_provider,
                PageFile::Type::Formal,
                page_file_log);
            // Note: This file may not contain any valid page, but valid RefPages which we need to migrate
            to_merge_file.readAndSetPageMetas(legacy_edit);

            auto it = file_valid_pages.find(file_id_level);
            if (it == file_valid_pages.end())
            {
                LOG_FMT_TRACE(log, "{} No valid pages from PageFile_{}_{} to PageFile_{}_{}", storage_name, file_id_level.first, file_id_level.second, gc_file.getFileId(), gc_file.getLevel());
                continue;
            }

            PageIdAndEntries page_id_and_entries; // The valid pages that we need to migrate to `gc_file`
            auto to_merge_file_reader = to_merge_file.createReader();
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
                        LOG_FMT_WARNING(log, "{} Ignore invalid RefPage while gcMigratePages: {}", storage_name, e.message());
                    }
                }
            }

            if (!page_id_and_entries.empty())
            {
                // copy valid pages from `to_merge_file` to `gc_file`
                PageMap pages = to_merge_file_reader->read(page_id_and_entries);
                WriteBatch wb;
                for (const auto & [page_id, page_entry] : page_id_and_entries)
                {
                    auto & page = pages.find(page_id)->second;
                    wb.upsertPage(page_id,
                                  page_entry.tag,
                                  std::make_shared<ReadBufferFromMemory>(page.data.begin(), page.data.size()),
                                  page.data.size());
                }

                gc_file_writer->write(wb, gc_file_edit);
            }

            LOG_FMT_TRACE(log, "{} Migrate {} pages from PageFile_{}_{} to PageFile_{}_{}", storage_name, page_id_and_entries.size(), file_id_level.first, file_id_level.second, gc_file.getFileId(), gc_file.getLevel());
        }

#if 0
        /// Do NOT need to migrate RefPage and DelPage since we only remove PageFile's data but keep meta.
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
#endif
    } // free gc_file_writer and sync

    const auto id = gc_file.fileIdLevel();
    if (gc_file_edit.empty() && num_valid_ref_pages == 0 && num_del_page_meta == 0)
    {
        LOG_FMT_INFO(log, "{} No valid pages, deleting PageFile_{}_{}", storage_name, id.first, id.second);
        gc_file.destroy();
    }
    else
    {
        gc_file.setFormal();
        LOG_FMT_INFO(log, "{} GC have migrated {} regions and {} RefPages and {} DelPage to PageFile_{}_{}", storage_name, num_successful_migrate_pages, num_valid_ref_pages, num_del_page_meta, id.first, id.second);
    }
    return gc_file_edit;
}

/**
 * Delete obsolete files that are not used by any version
 * @param page_files            All available files in disk
 * @param writing_file_id_level The PageFile id which is writing to
 * @param live_files            The live files after gc
 */
void PageStorage::gcRemoveObsoleteData(std::set<PageFile, PageFile::Comparator> & page_files,
                                       const PageFileIdAndLevel & writing_file_id_level,
                                       const std::set<PageFileIdAndLevel> & live_files)
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
            /// The page file is not used by any version, remove the page file's data in disk.
            /// Page file's meta is left and will be compacted later.
            const_cast<PageFile &>(page_file).setLegacy();
        }
    }
}

} // namespace PS::V1
} // namespace DB
