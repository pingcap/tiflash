#include <queue>
#include <set>
#include <utility>

#include <Storages/Page/PageStorage.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <common/logger_useful.h>
#include <ext/scope_guard.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

PageFileSet PageStorage::listAllPageFiles(const String & storage_path, Poco::Logger * page_file_log, ListPageFilesOption option)
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

    PageFileSet page_files;
    for (const auto & name : file_names)
    {
        auto [page_file, page_file_type] = PageFile::recover(storage_path, name, page_file_log);
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
                // Remove temporary file.
                Poco::File file(storage_path + "/" + name);
                file.remove(true);
            }
        }
    }

    return page_files;
}

PageStorage::PageStorage(String name, const String & storage_path_, const Config & config_)
    : storage_name(std::move(name)),
      storage_path(storage_path_),
      config(config_),
      write_files(std::max(1UL, config.num_write_slots)),
      page_file_log(&Poco::Logger::get("PageFile")),
      log(&Poco::Logger::get("PageStorage")),
      versioned_page_entries(config.version_set_config, log)
{
    // at least 1 write slots
    config.num_write_slots = std::max(1UL, config.num_write_slots);
}

void PageStorage::restore()
{
    /// page_files are in ascending ordered by (file_id, level).
    ListPageFilesOption opt;
    opt.remove_tmp_files   = true;
    opt.ignore_legacy      = false;
    opt.ignore_checkpoint  = false;
    PageFileSet page_files = PageStorage::listAllPageFiles(storage_path, page_file_log, opt);

    /// Restore current version from both formal and legacy page files

    MetaMergingQueue merging_queue;
    for (auto & page_file : page_files)
    {
        if (!(page_file.getType() == PageFile::Type::Formal || page_file.getType() == PageFile::Type::Legacy
              || page_file.getType() == PageFile::Type::Checkpoint))
            throw Exception("Try to recover from " + page_file.toString() + ", illegal type.", ErrorCodes::LOGICAL_ERROR);

        auto reader = const_cast<PageFile &>(page_file).createMetaMergingReader();
        // Read one WriteBatch
        reader->moveNext();
        merging_queue.push(std::move(reader));
    }

    auto [checkpoint_wb_sequence, page_files_to_archieve]
        = restoreFromCheckpoints(merging_queue, versioned_page_entries, storage_name, log);

    while (!merging_queue.empty())
    {
        auto reader = merging_queue.top();
        merging_queue.pop();
        LOG_TRACE(log, storage_name << " recovering from " + reader->toString());
        // If no checkpoint, we apply all edits.
        // Else restroed from checkpoint, if checkpoint's WriteBatch sequence number is 0, we need to apply
        // all edits after that checkpoint too. If checkpoint's WriteBatch sequence number is not 0, we
        // apply WriteBatch edits only if its WriteBatch sequence is bigger than checkpoint.
        if (!checkpoint_wb_sequence.has_value() || //
            (checkpoint_wb_sequence.has_value()
             && (*checkpoint_wb_sequence == 0 || *checkpoint_wb_sequence < reader->writeBatchSequence())))
        {
            try
            {
                auto edits = reader->getEdits();
                versioned_page_entries.apply(edits);
            }
            catch (Exception & e)
            {
                /// Better diagnostics.
                e.addMessage("(while applying edit to PageStorage: " + storage_name + " with " + reader->toString() + ")");
                throw;
            }
        }
        if (reader->hasNext())
        {
            reader->moveNext();
            merging_queue.push(std::move(reader));
        }
        else
        {
            reader->setPageFileOffsets();
        }
    }

    if (!page_files_to_archieve.empty())
    {
        // Remove old checkpoints and archieve obsolete PageFiles that have not been archieved yet during gc for some reason.
        archievePageFiles(page_files_to_archieve);
        for (auto & pf : page_files_to_archieve)
        {
            if (auto iter = page_files.find(pf); iter != page_files.end())
                page_files.erase(iter);
        }
    }

    for (auto & page_file : page_files)
    {
        // We need to keep a PageFile with largest FileID in write_files
        if (page_file.getLevel() == 0)
            write_files[0] = page_file;
    }
    // TODO: reuse some page_files
    // fill write_files
    for (auto & page_file : write_files)
    {
        auto writer = getWriter(page_file);
        idle_writers.emplace_back(std::move(writer));
    }
}

std::pair<std::optional<WriteBatch::SequenceID>, PageFileSet> //
PageStorage::restoreFromCheckpoints(PageStorage::MetaMergingQueue & merging_queue,
                                    VersionedPageEntries &          version_set,
                                    const String &                  storage_name,
                                    Poco::Logger *                  logger)
{
    PageFileSet page_files_to_archieve;
    // The sequence number of checkpoint. We should ignore the WriteBatch with
    // smaller number than checkpoint's.
    WriteBatch::SequenceID checkpoint_wb_sequence = 0;

    std::vector<PageFile> checkpoints;

    PageEntriesEdit    last_checkpoint_edits;
    PageFileIdAndLevel last_checkpoint_file_id;
    while (!merging_queue.empty() //
           && merging_queue.top()->belongingPageFile().getType() == PageFile::Type::Checkpoint)
    {
        auto reader = merging_queue.top();
        merging_queue.pop();

        last_checkpoint_edits   = reader->getEdits();
        last_checkpoint_file_id = reader->fileIdLevel();
        checkpoint_wb_sequence  = reader->writeBatchSequence();

        checkpoints.emplace_back(reader->belongingPageFile());
    }
    if (!checkpoints.empty())
    {
        // Old checkpoints can be removed
        for (size_t i = 0; i < checkpoints.size() - 1; ++i)
            page_files_to_archieve.emplace(checkpoints[i]);
        try
        {
            // Apply edits from latest checkpoint
            version_set.apply(last_checkpoint_edits);
        }
        catch (Exception & e)
        {
            /// TODO: Better diagnostics.
            throw;
        }

        if (checkpoint_wb_sequence == 0)
        {
            while (merging_queue.top()->fileIdLevel() <= last_checkpoint_file_id)
            {
                auto reader = merging_queue.top();
                LOG_INFO(logger,
                         storage_name << " Removing old PageFile: " + reader->belongingPageFile().toString()
                                 + " after restore checkpoint PageFile_"
                                      << last_checkpoint_file_id.first << "_" << last_checkpoint_file_id.second);
                if (reader->writeBatchSequence() != 0)
                {
                    throw Exception("Try to removing old PageFile: " + reader->belongingPageFile().toString()
                                        + " after restore checkpoint PageFile_" + DB::toString(last_checkpoint_file_id.first) + "_"
                                        + DB::toString(last_checkpoint_file_id.second)
                                        + ", but write batch sequence is: " + DB::toString(reader->writeBatchSequence()),
                                    ErrorCodes::LOGICAL_ERROR);
                }

                // this file can be removed later
                page_files_to_archieve.emplace(reader->belongingPageFile());
                merging_queue.pop();
            }
        }
        return {checkpoint_wb_sequence, page_files_to_archieve};
    }
    return {std::nullopt, page_files_to_archieve};
}

PageId PageStorage::getMaxId()
{
    std::lock_guard<std::mutex> write_lock(write_mutex);
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
        LOG_WARNING(log, storage_name << " " << e.message());
        return {}; // return invalid PageEntry
    }
}

PageStorage::WriterPtr PageStorage::getWriter(PageFile & page_file)
{
    WriterPtr write_file_writer;

    bool is_writable = page_file.isValid() && page_file.getType() == PageFile::Type::Formal //
        && page_file.getDataFileAppendPos() < config.file_roll_size                         //
        && page_file.getMetaFileAppendPos() < config.file_meta_roll_size;
    if (is_writable)
    {
        write_file_writer = page_file.createWriter(config.sync_on_write);
    }
    else
    {
        PageFileIdAndLevel max_writing_id_lvl{0, 0};
        for (const auto & pf : write_files)
            max_writing_id_lvl = std::max(max_writing_id_lvl, pf.fileIdLevel());
        page_file = PageFile::newPageFile(max_writing_id_lvl.first + 1, 0, storage_path, PageFile::Type::Formal, page_file_log);
        LOG_DEBUG(log, storage_name << " create new PageFile_" + DB::toString(max_writing_id_lvl.first + 1) + "_0 for write.");
        write_file_writer = page_file.createWriter(config.sync_on_write);
    }
    return write_file_writer;
}

PageStorage::ReaderPtr PageStorage::getReader(const PageFileIdAndLevel & file_id_level)
{
    std::lock_guard<std::mutex> lock(open_read_files_mutex);

    auto & pages_reader = open_read_files[file_id_level];
    if (pages_reader == nullptr)
    {
        auto page_file
            = PageFile::openPageFileForRead(file_id_level.first, file_id_level.second, storage_path, PageFile::Type::Formal, page_file_log);
        pages_reader = page_file.createReader();
    }
    return pages_reader;
}

void PageStorage::write(const WriteBatch & wb)
{
    if (unlikely(wb.empty()))
        return;

    if (unlikely(config.num_write_slots > 1 && wb.getSequence() == 0))
        throw Exception(storage_name + " with num_write_slots=" + DB::toString(config.num_write_slots)
                            + ", need to set sequence number for  keep order between write batches",
                        ErrorCodes::LOGICAL_ERROR);

    WriterPtr file_to_write = nullptr;
    {
        // Lock to wait for one idle writer
        std::unique_lock lock(write_mutex);
        write_mutex_cv.wait(lock, [this] { return !idle_writers.empty(); });
        file_to_write = std::move(idle_writers.front());
        idle_writers.pop_front();
    }

    PageEntriesEdit edit;
    file_to_write->write(wb, edit);

    {
        // Return writer to idle queue
        std::unique_lock lock(write_mutex);

        // Look if we need to roll to new PageFile
        size_t index = 0;
        for (size_t i = 0; i < write_files.size(); ++i)
        {
            if (write_files[i].fileIdLevel() == file_to_write->fileIdLevel())
            {
                index = i;
                break;
            }
        }
        auto & page_file   = write_files[index];
        bool   is_writable = page_file.isValid()                        //
            && page_file.getDataFileAppendPos() < config.file_roll_size //
            && page_file.getMetaFileAppendPos() < config.file_meta_roll_size;
        if (!is_writable)
        {
            file_to_write = nullptr; // reset writer first
            PageFileIdAndLevel max_writing_id_lvl{0, 0};
            for (const auto & pf : write_files)
                max_writing_id_lvl = std::max(max_writing_id_lvl, pf.fileIdLevel());
            LOG_DEBUG(log, storage_name << " create new PageFile_" + DB::toString(max_writing_id_lvl.first + 1) + "_0 for write.");
            page_file     = PageFile::newPageFile(max_writing_id_lvl.first + 1, 0, storage_path, PageFile::Type::Formal, page_file_log);
            file_to_write = page_file.createWriter(config.sync_on_write);
        }

        idle_writers.emplace_back(std::move(file_to_write));

        write_mutex_cv.notify_one(); // wake up any paused thread for write
    }

    // Apply changes into versioned_page_entries(generate a new version)
    // If there are RefPages to non-exist Pages, just put the ref pair to new version
    // instead of throwing exception. Or we can't open PageStorage since we have already
    // persist the invalid ref pair into PageFile.
    versioned_page_entries.apply(edit);

    for (auto & w : wb.getWrites())
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
            throw Exception("Unexpected write type " + DB::toString((UInt64)w.type));
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
        auto file_id_level                        = page_entry->fileIdLevel();
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
        auto & reader              = entries_and_reader.second;
        auto   page_in_file        = reader->read(page_id_and_entries);
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
        auto file_id_level                        = page_entry->fileIdLevel();
        auto & [page_id_and_entries, file_reader] = file_read_infos[file_id_level];
        page_id_and_entries.emplace_back(page_id, *page_entry);
        if (file_reader == nullptr)
            file_reader = getReader(file_id_level);
    }

    for (auto & [file_id_level, entries_and_reader] : file_read_infos)
    {
        (void)file_id_level;
        auto & page_id_and_entries = entries_and_reader.first;
        auto & reader              = entries_and_reader.second;

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
    auto valid_pages_ids = snapshot->version()->validPageIds();
    for (auto page_id : valid_pages_ids)
    {
        const auto page_entry = snapshot->version()->find(page_id);
        if (unlikely(!page_entry))
            throw Exception("Page[" + DB::toString(page_id) + "] not found when traversing PageStorage's entries",
                            ErrorCodes::LOGICAL_ERROR);
        acceptor(page_id, *page_entry);
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

    LOG_TRACE(log,
              storage_name << " Before gc, deletes[" << deletes << "], puts[" << puts << "], refs[" << refs << "], upserts[" << upserts
                           << "]");

    /// Get all pending external pages and PageFiles. Note that we should get external pages before PageFiles.
    PathAndIdsVec external_pages;
    if (external_pages_scanner)
    {
        external_pages = external_pages_scanner();
    }
    ListPageFilesOption opt;
    opt.remove_tmp_files = true;
    auto page_files      = PageStorage::listAllPageFiles(storage_path, page_file_log, opt);
    if (page_files.empty())
    {
        return false;
    }

    std::set<PageFileIdAndLevel> writing_file_id_levels;
    PageFileIdAndLevel           min_writing_file_id_level;
    {
        std::lock_guard<std::mutex> lock(write_mutex);
        for (size_t i = 1; i < write_files.size(); ++i)
        {
            writing_file_id_levels.insert(write_files[i].fileIdLevel());
        }
        min_writing_file_id_level = *writing_file_id_levels.begin();
    }

    // Try to compact consecutive Legacy PageFiles into a snapshot
    page_files = gcCompactLegacy(std::move(page_files), writing_file_id_levels);

    bool   should_merge         = false;
    UInt64 candidate_total_size = 0;

    std::set<PageFileIdAndLevel> merge_files;
    PageEntriesEdit              gc_file_entries_edit;

    {
        /// Select the GC candidates files and migrate valid pages into an new file.
        /// Acquire a snapshot version of page map, new edit on page map store in `gc_file_entries_edit`
        SnapshotPtr snapshot = this->getSnapshot();

        std::map<PageFileIdAndLevel, std::pair<size_t, PageIds>> file_valid_pages;
        {
            // Only scan over normal Pages, excluding RefPages
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
        }

        // Select gc candidate files into `merge_files`
        size_t migrate_page_count = 0;
        merge_files
            = gcSelectCandidateFiles(page_files, file_valid_pages, min_writing_file_id_level, candidate_total_size, migrate_page_count);
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
        std::lock_guard<std::mutex> lock(open_read_files_mutex);
        for (const auto & page_file : page_files)
        {
            const auto page_id_and_lvl = page_file.fileIdLevel();
            if (page_id_and_lvl >= min_writing_file_id_level)
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
    gcRemoveObsoleteData(page_files, min_writing_file_id_level, live_files);

    // Invoke callback with valid normal page id after gc.
    if (external_pages_remover)
    {
        external_pages_remover(external_pages, live_normal_pages);
    }

    if (!should_merge)
        LOG_TRACE(log,
                  storage_name << " GC exit without merging. merge file size: " << merge_files.size()
                               << ", candidate size: " << candidate_total_size);
    return should_merge;
}

PageStorage::GcCandidates PageStorage::gcSelectCandidateFiles( // keep readable indent
    const PageFileSet &        page_files,
    const GcLivesPages &       file_valid_pages,
    const PageFileIdAndLevel & writing_file_id_level,
    UInt64 &                   candidate_total_size,
    size_t &                   migrate_page_count) const
{
    GcCandidates merge_files;
    for (auto & page_file : page_files)
    {
        if (unlikely(page_file.getType() != PageFile::Type::Formal))
        {
            throw Exception("Try to pick PageFile_" + DB::toString(page_file.getFileId()) + "_" + DB::toString(page_file.getLevel()) + "("
                                + PageFile::typeToString(page_file.getType()) + ") as gc candidate, path: " + page_file.folderPath(),
                            ErrorCodes::LOGICAL_ERROR);
        }

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
        bool is_candidate = (page_file.fileIdLevel() < writing_file_id_level)
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

PageFileSet PageStorage::gcCompactLegacy(PageFileSet && page_files, const std::set<PageFileIdAndLevel> & writing_file_id_levels)
{
    // Select PageFiles to compact
    PageFileSet page_files_to_compact;
    // Build a version_set with snapshot
    PageEntriesVersionSetWithDelta version_set(config.version_set_config, log);
    WriteBatch::SequenceID         largest_wb_sequence = 0;
    {
        MetaMergingQueue merging_queue;
        for (auto & page_file : page_files)
        {
            auto reader = const_cast<PageFile &>(page_file).createMetaMergingReader();
            // Read one valid WriteBatch
            reader->moveNext();
            merging_queue.push(std::move(reader));
        }

        std::optional<WriteBatch::SequenceID> checkpoint_wb_sequence;
        std::tie(checkpoint_wb_sequence, page_files_to_compact) = restoreFromCheckpoints(merging_queue, version_set, storage_name, log);

        while (!merging_queue.empty())
        {
            auto reader = merging_queue.top();
            merging_queue.pop();
            // We don't want to do compaction on formal / writing files. If any, just stop collecting `page_files_to_compact`.
            if (writing_file_id_levels.count(reader->fileIdLevel()) != 0)
                break;
            if (reader->belongingPageFile().getType() == PageFile::Type::Formal)
                break;

            // If no checkpoint, we apply all edits.
            // Else restroed from checkpoint, if checkpoint's WriteBatch sequence number is 0, we need to apply
            // all edits after that checkpoint too. If checkpoint's WriteBatch sequence number is not 0, we
            // apply WriteBatch edits only if its WriteBatch sequence is bigger than checkpoint.
            if (!checkpoint_wb_sequence.has_value() || //
                (checkpoint_wb_sequence.has_value()
                 && (*checkpoint_wb_sequence == 0 || *checkpoint_wb_sequence < reader->writeBatchSequence())))
            {
                LOG_TRACE(log, storage_name << " gcCompactLegacy recovering from " + reader->toString());
                try
                {
                    auto edits = reader->getEdits();
                    version_set.apply(edits);
                    largest_wb_sequence = std::max(largest_wb_sequence, reader->writeBatchSequence());
                }
                catch (Exception & e)
                {
                    /// Better diagnostics.
                    e.addMessage("(PageStorage: " + storage_name + " while applying edit in gcCompactLegacy with " + reader->toString()
                                 + ")");
                    throw;
                }
            }
            if (reader->hasNext())
            {
                reader->moveNext();
                merging_queue.push(std::move(reader));
            }
            else
            {
                // We apply all edit of belonging PageFile, do compaction on it.
                page_files_to_compact.emplace(reader->belongingPageFile());
            }
        }
    }

    if (page_files_to_compact.size() < config.gc_compact_legacy_min_num)
    {
        LOG_DEBUG(log,
                  storage_name << " gcCompactLegacy exit without compaction, candidate size: " << page_files_to_compact.size() << " < "
                               << config.gc_compact_legacy_min_num);
        // Nothing to compact, remove legacy/checkpoint page files since we
        // don't do gc on them later.
        for (auto itr = page_files.begin(); itr != page_files.end(); /* empty */)
        {
            auto & page_file = *itr;
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

    auto snapshot = version_set.getSnapshot();
    auto wb       = prepareSnapshotWriteBatch(snapshot, largest_wb_sequence);

    // Use the largest id-level in page_files_to_compact as Snapshot's
    const PageFileIdAndLevel largest_id_level = page_files_to_compact.rbegin()->fileIdLevel();
    {
        std::stringstream ss;
        ss << "[";
        for (const auto & page_file : page_files_to_compact)
            ss << "<" << page_file.getFileId() << "," << page_file.getLevel() << ">,";
        ss << "]";
        LOG_INFO(log,
                 storage_name << " Compact legacy PageFile " << ss.str() //
                              << " into checkpoint PageFile_" << largest_id_level.first << "_" << largest_id_level.second);
    }
    auto checkpoint_file = PageFile::newPageFile(largest_id_level.first, largest_id_level.second, storage_path, PageFile::Type::Temp, log);
    {
        auto checkpoint_writer = checkpoint_file.createWriter(false);

        PageEntriesEdit edit;
        checkpoint_writer->write(wb, edit);
    }

    checkpoint_file.setCheckpoint();

    // Archieve obsolete PageFiles
    {
        for (auto itr = page_files.begin(); itr != page_files.end();)
        {
            auto & page_file = *itr;
            if (page_files_to_compact.count(page_file) > 0)
            {
                // Remove page files have been compacted
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

        archievePageFiles(page_files_to_compact);
    }

    return std::move(page_files);
}

WriteBatch PageStorage::prepareSnapshotWriteBatch(const SnapshotPtr snapshot, const WriteBatch::SequenceID wb_sequence)
{
    WriteBatch wb;
    // First Ingest exists pages with normal_id
    auto normal_ids = snapshot->version()->validNormalPageIds();
    for (auto & page_id : normal_ids)
    {
        auto page = snapshot->version()->findNormalPageEntry(page_id);
        wb.upsertPage(page_id, page->tag, nullptr, page->size);
    }

    // After ingesting normal_pages, we will ref them manually to ensure the ref-count is correct.
    auto ref_ids = snapshot->version()->validPageIds();
    for (auto & page_id : ref_ids)
    {
        auto ori_id = snapshot->version()->isRefId(page_id).second;
        wb.putRefPage(page_id, ori_id);
    }

    wb.setSequence(wb_sequence);
    return wb;
}

void PageStorage::archievePageFiles(const PageFileSet & page_files)
{
    if (page_files.empty())
        return;

    const String archieve_path = storage_path + "/.archieve/";
    Poco::File   archieve_dir(archieve_path);
    if (!archieve_dir.exists())
        archieve_dir.createDirectory();

    for (auto & page_file : page_files)
    {
        Poco::Path path(page_file.folderPath());
        auto       dest = archieve_path + path.getBaseName();
        Poco::File file(path);
        if (file.exists())
            file.moveTo(dest);
    }
    LOG_INFO(log, storage_name << " archieve " + DB::toString(page_files.size()) + " to " + archieve_path);
}

PageEntriesEdit PageStorage::gcMigratePages(const SnapshotPtr &  snapshot,
                                            const GcLivesPages & file_valid_pages,
                                            const GcCandidates & merge_files,
                                            const size_t         migrate_page_count) const
{
    PageEntriesEdit gc_file_edit;
    if (merge_files.empty())
        return gc_file_edit;

    // merge `merge_files` to PageFile which PageId = max of all `merge_files` and level = level + 1
    auto [largest_file_id, level] = *(merge_files.rbegin());

    {
        // In case that those files are hold by snapshot and do gcMigrate to same PageFile again, we need to check if gc_file is already exist.
        PageFile gc_file = PageFile::openPageFileForRead(largest_file_id, level + 1, storage_path, PageFile::Type::Formal, page_file_log);
        if (gc_file.isExist())
        {
            LOG_INFO(log, storage_name << " GC migration to PageFile_" << largest_file_id << "_" << level + 1 << " is done before.");
            return gc_file_edit;
        }
    }

    // Create a tmp PageFile for migration
    PageFile gc_file = PageFile::newPageFile(largest_file_id, level + 1, storage_path, PageFile::Type::Temp, page_file_log);
    LOG_INFO(log,
             storage_name << " GC decide to merge " << merge_files.size() << " files, containing " << migrate_page_count
                          << " regions to PageFile_" << gc_file.getFileId() << "_" << gc_file.getLevel());

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
            PageFile to_merge_file = PageFile::openPageFileForRead(
                file_id_level.first, file_id_level.second, storage_path, PageFile::Type::Formal, page_file_log);

            WriteBatch::SequenceID max_wb_sequence = 0;
            to_merge_file.readAndSetPageMetas(legacy_edit, max_wb_sequence);

            auto it = file_valid_pages.find(file_id_level);
            if (it == file_valid_pages.end())
            {
                // This file does not contain any valid page.
                LOG_TRACE(log,
                          storage_name << " No valid pages from PageFile_"                                      //
                                       << file_id_level.first << "_" << file_id_level.second << " to PageFile_" //
                                       << gc_file.getFileId() << "_" << gc_file.getLevel());
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
                for (const auto & [page_id, page_entry] : page_id_and_entries)
                {
                    auto & page = pages.find(page_id)->second;
                    wb.upsertPage(page_id,
                                  page_entry.tag,
                                  std::make_shared<ReadBufferFromMemory>(page.data.begin(), page.data.size()),
                                  page.data.size());
                }
                wb.setSequence(max_wb_sequence);

                gc_file_writer->write(wb, gc_file_edit);
            }

            LOG_TRACE(log,
                      storage_name << " Migrate " << page_id_and_entries.size() << " pages from PageFile_"  //
                                   << file_id_level.first << "_" << file_id_level.second << " to PageFile_" //
                                   << gc_file.getFileId() << "_" << gc_file.getLevel());
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
 * @param writing_file_id_level The min PageFile id which is writing to
 * @param live_files            The live files after gc
 */
void PageStorage::gcRemoveObsoleteData(PageFileSet &                        page_files,
                                       const PageFileIdAndLevel &           writing_file_id_level,
                                       const std::set<PageFileIdAndLevel> & live_files)
{
    for (auto & page_file : page_files)
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

} // namespace DB
