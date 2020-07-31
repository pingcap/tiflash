#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/gc/DataCompactor.h>
#include <Storages/Page/gc/LegacyCompactor.h>
#include <Storages/Page/gc/restoreFromCheckpoints.h>
#include <Storages/PathCapacityMetrics.h>
#include <common/logger_useful.h>

#include <ext/scope_guard.h>
#include <queue>
#include <set>
#include <utility>

// #define PAGE_STORAGE_UTIL_DEBUGGGING

#ifdef PAGE_STORAGE_UTIL_DEBUGGGING
extern DB::WriteBatch::SequenceID debugging_recover_stop_sequence;
#endif

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

void PageStorage::StatisticsInfo::mergeEdits(const PageEntriesEdit & edit)
{
    for (const auto & record : edit.getRecords())
    {
        if (record.type == WriteBatch::WriteType::DEL)
            deletes++;
        else if (record.type == WriteBatch::WriteType::PUT)
            puts++;
        else if (record.type == WriteBatch::WriteType::REF)
            refs++;
        else if (record.type == WriteBatch::WriteType::UPSERT)
            upserts++;
    }
}

String PageStorage::StatisticsInfo::toString() const
{
    std::stringstream ss;
    ss << puts << " puts and " << refs << " refs and " //
       << deletes << " deletes and " << upserts << " upserts";
    return ss.str();
}

PageFileSet PageStorage::listAllPageFiles(const String &              storage_path,
                                          const FileProviderPtr &     file_provider,
                                          Poco::Logger *              page_file_log,
                                          const ListPageFilesOption & option)
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
        // Ignore archive directory.
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
                // Remove temporary file.
                Poco::File file(storage_path + "/" + name);
                file.remove(true);
            }
        }
    }

    return page_files;
}

PageStorage::PageStorage(String                  name,
                         const String &          storage_path_,
                         const Config &          config_,
                         const FileProviderPtr & file_provider_,
                         TiFlashMetricsPtr       metrics_,
                         PathCapacityMetricsPtr  global_capacity_)
    : storage_name(std::move(name)),
      storage_path(storage_path_),
      config(config_),
      file_provider(file_provider_),
      write_files(std::max(1UL, config.num_write_slots)),
      page_file_log(&Poco::Logger::get("PageFile")),
      log(&Poco::Logger::get("PageStorage")),
      versioned_page_entries(storage_name, config.version_set_config, log),
      metrics(std::move(metrics_)),
      global_capacity(std::move(global_capacity_))
{
    // at least 1 write slots
    config.num_write_slots = std::max(1UL, config.num_write_slots);
}

void PageStorage::restore()
{
    LOG_INFO(log, storage_name << " begin to restore from path: " << storage_path);

    /// page_files are in ascending ordered by (file_id, level).
    ListPageFilesOption opt;
    opt.remove_tmp_files = true;
#ifdef PAGE_STORAGE_UTIL_DEBUGGGING
    opt.remove_tmp_files = false;
#endif
    opt.ignore_legacy      = false;
    opt.ignore_checkpoint  = false;
    PageFileSet page_files = PageStorage::listAllPageFiles(storage_path, file_provider, page_file_log, opt);

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

    StatisticsInfo restore_info;
    /// First try to recover from latest checkpoint
    std::optional<PageFile>               checkpoint_file;
    std::optional<WriteBatch::SequenceID> checkpoint_sequence;
    PageFileSet                           page_files_to_remove;
    std::tie(checkpoint_file, checkpoint_sequence, page_files_to_remove)
        = restoreFromCheckpoints(merging_queue, versioned_page_entries, restore_info, storage_name, log);
    (void)checkpoint_file;
    if (checkpoint_sequence)
        write_batch_seq = *checkpoint_sequence;

    while (!merging_queue.empty())
    {
        auto reader = merging_queue.top();
        merging_queue.pop();

#ifdef PAGE_STORAGE_UTIL_DEBUGGGING
        if (debugging_recover_stop_sequence != 0 && reader->writeBatchSequence() > debugging_recover_stop_sequence)
        {
            LOG_TRACE(log, storage_name << " debugging early stop on sequence: " << debugging_recover_stop_sequence);
            break;
        }
#endif

        // If no checkpoint, we apply all edits.
        // Else restroed from checkpoint, if checkpoint's WriteBatch sequence number is 0, we need to apply
        // all edits after that checkpoint too. If checkpoint's WriteBatch sequence number is not 0, we
        // apply WriteBatch edits only if its WriteBatch sequence is larger than or equal to checkpoint.
        if (!checkpoint_sequence.has_value() || //
            (checkpoint_sequence.has_value() && (*checkpoint_sequence == 0 || *checkpoint_sequence <= reader->writeBatchSequence())))
        {
            try
            {
                // LOG_TRACE(log, storage_name << " recovering from " + reader->toString());
                auto edits = reader->getEdits();
                versioned_page_entries.apply(edits);
                restore_info.mergeEdits(edits);
                write_batch_seq = reader->writeBatchSequence();
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
            // Continue to merge next WriteBatch.
            reader->moveNext();
            merging_queue.push(std::move(reader));
        }
        else
        {
            // Set belonging PageFile's offset and close reader.
            LOG_TRACE(log, storage_name << " merge done from " + reader->toString());
            reader->setPageFileOffsets();
        }
    }

    if (!page_files_to_remove.empty())
    {
        // Remove old checkpoints and archive obsolete PageFiles that have not been archived yet during gc for some reason.
#ifdef PAGE_STORAGE_UTIL_DEBUGGGING
        LOG_TRACE(log, storage_name << " These file whould be archive:");
        for (auto & pf : page_files_to_remove)
            LOG_TRACE(log, storage_name << pf.toString());
#else
        archivePageFiles(page_files_to_remove);
#endif
        removePageFilesIf(page_files, [&page_files_to_remove](const PageFile & pf) -> bool { return page_files_to_remove.count(pf) > 0; });
    }

    // TODO: reuse some page_files
    // fill write_files
    size_t total_recover_bytes = 0;
    for (auto & page_file : page_files)
    {
        total_recover_bytes += page_file.getDiskSize();
        // We need to keep a PageFile with largest FileID in write_files
        if (page_file.getLevel() == 0)
            write_files[0] = page_file;
    }
    if (global_capacity)
        global_capacity->addUsedSize(storage_path, total_recover_bytes);
#ifndef PAGE_STORAGE_UTIL_DEBUGGGING
    for (auto & page_file : write_files)
    {
        auto writer = getWriter(page_file);
        idle_writers.emplace_back(std::move(writer));
    }
#endif

    statistics = restore_info;
    {
        auto   snapshot  = getSnapshot();
        size_t num_pages = snapshot->version()->numPages();
        LOG_INFO(log,
                 storage_name << " restore " << num_pages << " pages, write batch sequence: " << write_batch_seq //
                              << ", " << statistics.toString());
    }
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
        write_file_writer = page_file.createWriter(config.sync_on_write, false, false);
    }
    else
    {
        PageFileIdAndLevel max_writing_id_lvl{0, 0};
        for (const auto & pf : write_files)
            max_writing_id_lvl = std::max(max_writing_id_lvl, pf.fileIdLevel());
        page_file
            = PageFile::newPageFile(max_writing_id_lvl.first + 1, 0, storage_path, file_provider, PageFile::Type::Formal, page_file_log);
        LOG_DEBUG(log, storage_name << " create new PageFile_" + DB::toString(max_writing_id_lvl.first + 1) + "_0 for write.");
        write_file_writer = page_file.createWriter(config.sync_on_write, true, true);
    }
    return write_file_writer;
}

PageStorage::ReaderPtr PageStorage::getReader(const PageFileIdAndLevel & file_id_level)
{
    std::lock_guard<std::mutex> lock(open_read_files_mutex);

    auto & pages_reader = open_read_files[file_id_level];
    if (pages_reader == nullptr)
    {
        auto page_file = PageFile::openPageFileForRead(
            file_id_level.first, file_id_level.second, storage_path, file_provider, PageFile::Type::Formal, page_file_log);
        if (unlikely(!page_file.isExist()))
            throw Exception("Try to create reader for " + page_file.toString() + ", but PageFile is broken, check "
                                + page_file.folderPath(),
                            ErrorCodes::LOGICAL_ERROR);
        pages_reader = page_file.createReader();
    }
    return pages_reader;
}

void PageStorage::write(WriteBatch && wb)
{
    if (unlikely(wb.empty()))
        return;

    WriterPtr file_to_write = nullptr;
    {
        // Lock to wait for one idle writer
        std::unique_lock lock(write_mutex);
        write_mutex_cv.wait(lock, [this] { return !idle_writers.empty(); });
        file_to_write = std::move(idle_writers.front());
        idle_writers.pop_front();
    }

    PageEntriesEdit edit;
    wb.setSequence(++write_batch_seq); // Set sequence number to keep ordering between writers.
    size_t bytes_written = file_to_write->write(wb, edit);
    if (global_capacity)
        global_capacity->addUsedSize(storage_path, bytes_written);

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
            LOG_DEBUG(log,
                      storage_name << " PageFile_" << page_file.getFileId()
                                   << "_0 is full, create new PageFile_" + DB::toString(max_writing_id_lvl.first + 1) + "_0 for write");
            page_file = PageFile::newPageFile(
                max_writing_id_lvl.first + 1, 0, storage_path, file_provider, PageFile::Type::Formal, page_file_log);
            file_to_write = page_file.createWriter(config.sync_on_write, true, true);
        }

        idle_writers.emplace_back(std::move(file_to_write));

        write_mutex_cv.notify_one(); // wake up any paused thread for write
    }

    // Apply changes into versioned_page_entries(generate a new version)
    // If there are RefPages to non-exist Pages, just put the ref pair to new version
    // instead of throwing exception. Or we can't open PageStorage since we have already
    // persist the invalid ref pair into PageFile.
    versioned_page_entries.apply(edit);

    statistics.mergeEdits(edit);
}

PageStorage::SnapshotPtr PageStorage::getSnapshot()
{
    return versioned_page_entries.getSnapshot();
}

size_t PageStorage::getNumSnapshots() const
{
    return versioned_page_entries.numSnapshots();
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
        {
            try
            {
                file_reader = getReader(file_id_level);
            }
            catch (DB::Exception & e)
            {
                e.addMessage("(while reading Page[" + DB::toString(page_id) + "] of " + storage_name + ")");
                throw;
            }
        }
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
        {
            try
            {
                file_reader = getReader(file_id_level);
            }
            catch (DB::Exception & e)
            {
                e.addMessage("(while reading Page[" + DB::toString(page_id) + "] of " + storage_name + ")");
                throw;
            }
        }
    }

    for (auto & [file_id_level, entries_and_reader] : file_read_infos)
    {
        (void)file_id_level;
        auto & page_id_and_entries = entries_and_reader.first;
        auto & reader              = entries_and_reader.second;

        reader->read(page_id_and_entries, handler);
    }
}

PageMap PageStorage::read(const std::vector<PageReadFields> & page_fields, SnapshotPtr snapshot)
{
    if (!snapshot)
        snapshot = this->getSnapshot();


    std::map<PageFileIdAndLevel, std::pair<ReaderPtr, PageFile::Reader::FieldReadInfos>> file_read_infos;
    for (const auto & [page_id, field_indices] : page_fields)
    {
        const auto page_entry = snapshot->version()->find(page_id);
        if (!page_entry)
            throw Exception("Page " + DB::toString(page_id) + " not found", ErrorCodes::LOGICAL_ERROR);
        const auto file_id_level          = page_entry->fileIdLevel();
        auto & [file_reader, field_infos] = file_read_infos[file_id_level];
        field_infos.emplace_back(page_id, *page_entry, field_indices);
        if (file_reader == nullptr)
        {
            try
            {
                file_reader = getReader(file_id_level);
            }
            catch (DB::Exception & e)
            {
                e.addMessage("(while reading Page[" + DB::toString(page_id) + "] of " + storage_name + ")");
                throw;
            }
        }
    }

    PageMap page_map;
    for (auto & [file_id_level, entries_and_reader] : file_read_infos)
    {
        (void)file_id_level;
        auto & reader       = entries_and_reader.first;
        auto & fields_infos = entries_and_reader.second;
        auto   page_in_file = reader->read(fields_infos);
        for (auto & [page_id, page] : page_in_file)
            page_map.emplace(page_id, std::move(page));
    }
    return page_map;
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

void PageStorage::drop()
{
    LOG_DEBUG(log, storage_name << " is going to drop");

    ListPageFilesOption opt;
    opt.ignore_checkpoint = false;
    opt.ignore_legacy     = false;
    opt.remove_tmp_files  = false;
    auto page_files       = PageStorage::listAllPageFiles(storage_path, file_provider, page_file_log, opt);

    // TODO: count how many bytes in "archive" directory.
    size_t bytes_to_remove = 0;
    for (const auto & page_file : page_files)
    {
        bytes_to_remove += page_file.getDiskSize();
        page_file.destroy();
    }

    if (Poco::File directory(storage_path); directory.exists())
        directory.remove(true);

    global_capacity->freeUsedSize(storage_path, bytes_to_remove);

    LOG_INFO(log, storage_name << " drop done.");
}

struct GCDebugInfo
{
    PageFileIdAndLevel min_file_id;
    PageFile::Type     min_file_type;
    PageFileIdAndLevel max_file_id;
    PageFile::Type     max_file_type;
    size_t             page_files_size;

    size_t num_files_archive_in_compact_legacy = 0;
    size_t num_bytes_written_in_compact_legacy = 0;

    DataCompactor<PageStorage::SnapshotPtr>::Result compact_result;

    // bytes written during gc
    size_t bytesWritten() const { return num_bytes_written_in_compact_legacy + compact_result.bytes_written; }

    PageStorage::StatisticsInfo gc_apply_stat;

    size_t num_files_remove_data = 0;
    size_t num_bytes_remove_data = 0;
};

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

    LOG_TRACE(log, storage_name << " Before gc, " << statistics.toString());

    /// Get all pending external pages and PageFiles. Note that we should get external pages before PageFiles.
    PathAndIdsVec external_pages;
    if (external_pages_scanner)
    {
        external_pages = external_pages_scanner();
    }
    ListPageFilesOption opt;
    opt.remove_tmp_files = true;
    auto page_files      = PageStorage::listAllPageFiles(storage_path, file_provider, page_file_log, opt);

    std::set<PageFileIdAndLevel> writing_file_id_levels;
    PageFileIdAndLevel           min_writing_file_id_level;
    {
        std::lock_guard<std::mutex> lock(write_mutex);
        for (size_t i = 0; i < write_files.size(); ++i)
        {
            writing_file_id_levels.insert(write_files[i].fileIdLevel());
        }
        min_writing_file_id_level = *writing_file_id_levels.begin();

        /// If writer has not been used for too long, close the opened file fd of them.
        for (auto & writer : idle_writers)
        {
            writer->tryCloseIdleFd(config.open_file_max_idle_time);
        }
    }

    GCDebugInfo debugging_info;
    // Helper function for apply edits and clean up before gc exit.
    auto apply_and_cleanup = [&, this](PageEntriesEdit && gc_edits) -> void {
        /// Here we have to apply edit to versioned_page_entries and generate a new version, then return all files that are in used
        auto [live_files, live_normal_pages] = versioned_page_entries.gcApply(gc_edits, external_pages_scanner != nullptr);
        debugging_info.gc_apply_stat.mergeEdits(gc_edits);

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
        std::tie(debugging_info.num_files_remove_data, debugging_info.num_bytes_remove_data)
            = gcRemoveObsoleteData(page_files, min_writing_file_id_level, live_files);

        if (global_capacity)
        {
            global_capacity->addUsedSize(storage_path, debugging_info.bytesWritten());
            global_capacity->freeUsedSize(storage_path, debugging_info.num_bytes_remove_data);
        }

        // Invoke callback with valid normal page id after gc.
        if (external_pages_remover)
        {
            external_pages_remover(external_pages, live_normal_pages);
        }
    };

    // Ignore page files that maybe writing to.
    {
        PageFileSet removed_page_files;
        for (const auto & pf : page_files)
        {
            if (pf.fileIdLevel() >= min_writing_file_id_level)
                continue;
            removed_page_files.emplace(pf);
        }
        page_files.swap(removed_page_files);
        if (page_files.size() < 3)
        {
            // Apply empty edit and cleanup.
            apply_and_cleanup(PageEntriesEdit{});
            LOG_TRACE(log, storage_name << " GC exit with no files to gc.");
            return false;
        }
    }

    Stopwatch watch;
    if (metrics)
    {
        GET_METRIC(metrics, tiflash_storage_page_gc_count, type_exec).Increment();
    }
    SCOPE_EXIT({
        if (metrics)
        {
            GET_METRIC(metrics, tiflash_storage_page_gc_duration_seconds, type_exec).Observe(watch.elapsedSeconds());
        }
    });

    debugging_info.min_file_id     = page_files.begin()->fileIdLevel();
    debugging_info.min_file_type   = page_files.begin()->getType();
    debugging_info.max_file_id     = page_files.rbegin()->fileIdLevel();
    debugging_info.max_file_type   = page_files.rbegin()->getType();
    debugging_info.page_files_size = page_files.size();

    {
        // Try to compact consecutive Legacy PageFiles into a snapshot
        LegacyCompactor compactor(*this);
        PageFileSet     page_files_to_archive;
        std::tie(page_files, page_files_to_archive, debugging_info.num_bytes_written_in_compact_legacy)
            = compactor.tryCompact(std::move(page_files), writing_file_id_levels);
        archivePageFiles(page_files_to_archive);
        debugging_info.num_files_archive_in_compact_legacy = page_files_to_archive.size();
    }

    PageEntriesEdit gc_file_entries_edit;
    {
        /// Select the GC candidates files and migrate valid pages into an new file.
        /// Acquire a snapshot version of page map, new edit on page map store in `gc_file_entries_edit`
        Stopwatch watch_migrate;

        DataCompactor<PageStorage::SnapshotPtr> compactor(*this);
        std::tie(debugging_info.compact_result, gc_file_entries_edit)
            = compactor.tryMigrate(page_files, getSnapshot(), writing_file_id_levels);

        // We only care about those time cost in actually doing compaction on page data.
        if (debugging_info.compact_result.do_compaction && metrics)
        {
            GET_METRIC(metrics, tiflash_storage_page_gc_duration_seconds, type_migrate).Observe(watch_migrate.elapsedSeconds());
        }
    }

    apply_and_cleanup(std::move(gc_file_entries_edit));

    LOG_INFO(log,
             storage_name << " GC exit within " << DB::toString(watch.elapsedSeconds(), 2) << " sec. PageFiles from [" //
                          << debugging_info.min_file_id.first << "," << debugging_info.min_file_id.second              //
                          << "," << PageFile::typeToString(debugging_info.min_file_type) << "] to ["                   //
                          << debugging_info.max_file_id.first << "," << debugging_info.max_file_id.second              //
                          << "," << PageFile::typeToString(debugging_info.max_file_type)
                          << "], size: " + DB::toString(debugging_info.page_files_size) //
                          << ", compact legacy archive files: " << debugging_info.num_files_archive_in_compact_legacy
                          << ", remove data files: " << debugging_info.num_files_remove_data
                          << ", gc apply: " << debugging_info.gc_apply_stat.toString());
    return debugging_info.compact_result.do_compaction;
}

void PageStorage::archivePageFiles(const PageFileSet & page_files)
{
    if (page_files.empty())
        return;

    const Poco::Path archive_path(storage_path, PageStorage::ARCHIVE_SUBDIR);
    Poco::File       archive_dir(archive_path);
    if (!archive_dir.exists())
        archive_dir.createDirectory();

    for (auto & page_file : page_files)
    {
        Poco::Path path(page_file.folderPath());
        auto       dest = archive_path.toString() + "/" + path.getFileName();
        Poco::File file(path);
        if (file.exists())
            file.moveTo(dest);
    }
    LOG_INFO(log, storage_name << " archive " + DB::toString(page_files.size()) + " files to " + archive_path.toString());
}

/**
 * Delete obsolete files that are not used by any version
 * @param page_files            All available files in disk
 * @param writing_file_id_level The min PageFile id which is writing to
 * @param live_files            The live files after gc
 * @return how many data removed, how many bytes removed
 */
std::tuple<size_t, size_t> //
PageStorage::gcRemoveObsoleteData(PageFileSet &                        page_files,
                                  const PageFileIdAndLevel &           writing_file_id_level,
                                  const std::set<PageFileIdAndLevel> & live_files)
{
    size_t num_data_removed  = 0;
    size_t num_bytes_removed = 0;
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
            // LOG_INFO(log, storage_name << " remove data " << page_file.toString());
            num_bytes_removed += const_cast<PageFile &>(page_file).setLegacy();
            num_data_removed += 1;
        }
    }
    return {num_data_removed, num_bytes_removed};
}

} // namespace DB
