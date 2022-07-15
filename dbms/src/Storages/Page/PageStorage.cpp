#include <Common/FailPoint.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Encryption/FileProvider.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/gc/DataCompactor.h>
#include <Storages/Page/gc/LegacyCompactor.h>
#include <Storages/Page/gc/restoreFromCheckpoints.h>
#include <Storages/Page/mvcc/utils.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/PathPool.h>
#include <common/logger_useful.h>

#include <ext/scope_guard.h>
#include <queue>
#include <set>
#include <utility>

#ifdef FIU_ENABLE
#include <Common/randomSeed.h>

#include <pcg_random.hpp>
#endif

#ifdef PAGE_STORAGE_UTIL_DEBUGGGING
extern DB::WriteBatch::SequenceID debugging_recover_stop_sequence;
#endif

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

namespace FailPoints
{
extern const char random_slow_page_storage_write[];
extern const char random_exception_after_page_storage_sequence_acquired[];
} // namespace FailPoints

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

bool PageStorage::StatisticsInfo::equals(const StatisticsInfo & rhs)
{
    return puts == rhs.puts && refs == rhs.refs && deletes == rhs.deletes && upserts == rhs.upserts;
}

void PageStorage::Config::reload(const PageStorage::Config & rhs)
{
    // Reload is not atomic, but should be good enough

    // Reload gc threshold
    gc_max_valid_rate            = rhs.gc_max_valid_rate;
    gc_min_bytes                 = rhs.gc_min_bytes;
    gc_min_files                 = rhs.gc_min_files;
    gc_min_legacy_num            = rhs.gc_min_legacy_num;
    prob_do_gc_when_write_is_low = rhs.prob_do_gc_when_write_is_low;
    // Reload fd idle time
    open_file_max_idle_time = rhs.open_file_max_idle_time;
}

String PageStorage::Config::toDebugString() const
{
    std::stringstream ss;
    ss << "PageStorage::Config {gc_min_files:" << gc_min_files << ", gc_min_bytes:" << gc_min_bytes
       << ", gc_max_valid_rate:" << DB::toString(gc_max_valid_rate, 3) << ", gc_min_legacy_num:" << gc_min_legacy_num
       << ", gc_max_expect_legacy: " << DB::toString(gc_max_expect_legacy_files)
       << ", gc_max_valid_rate_bound: " << DB::toString(gc_max_valid_rate_bound, 3)
       << ", prob_do_gc_when_write_is_low:" << prob_do_gc_when_write_is_low
       << ", open_file_max_idle_time:" << open_file_max_idle_time.count() << "}";
    return ss.str();
}

PageFormat::Version PageStorage::getMaxDataVersion(const FileProviderPtr & file_provider, PSDiskDelegatorPtr & delegator)
{
    Poco::Logger *      log = &Poco::Logger::get("PageStorage::getMaxDataVersion");
    ListPageFilesOption option;
    option.ignore_checkpoint = true;
    option.ignore_legacy     = true;
    option.remove_tmp_files  = false;
    auto page_files          = listAllPageFiles(file_provider, delegator, log, option);
    if (page_files.empty())
        return STORAGE_FORMAT_CURRENT.page;

    bool                all_empty          = true;
    PageFormat::Version max_binary_version = PageFormat::V1;
    PageFormat::Version temp_version       = STORAGE_FORMAT_CURRENT.page;
    for (auto iter = page_files.rbegin(); iter != page_files.rend(); ++iter)
    {
        // Skip those files without valid meta
        if (iter->getMetaFileSize() == 0)
            continue;

        // Simply check the last non-empty PageFile is good enough
        all_empty   = false;
        auto reader = PageFile::MetaMergingReader::createFrom(const_cast<PageFile &>(*iter));
        while (reader->hasNext())
        {
            // Continue to read the binary version of next WriteBatch.
            reader->moveNext(&temp_version);
            max_binary_version = std::max(max_binary_version, temp_version);
        }
        LOG_DEBUG(log, "getMaxDataVersion done from " + reader->toString() << " [max version=" << max_binary_version << "]");
        break;
    }
    max_binary_version = (all_empty ? STORAGE_FORMAT_CURRENT.page : max_binary_version);
    return max_binary_version;
}

PageFileSet PageStorage::listAllPageFiles(const FileProviderPtr &     file_provider,
                                          PSDiskDelegatorPtr &        delegator,
                                          Poco::Logger *              page_file_log,
                                          const ListPageFilesOption & option)
{
    // collect all pages from `delegator` and recover to `PageFile` objects
    // [(dir0, [name0, name1, name2, ...]), (dir1, [name0, name1, ...]), ...]
    std::vector<std::pair<String, Strings>> all_file_names;
    {
        std::vector<std::string> file_names;
        for (const auto & p : delegator->listPaths())
        {
            Poco::File directory(p);
            if (!directory.exists())
                directory.createDirectories();
            file_names.clear();
            directory.list(file_names);
            all_file_names.emplace_back(std::make_pair(p, std::move(file_names)));
            file_names.clear();
        }
    }

    if (all_file_names.empty())
        return {};

    PageFileSet page_files;
    for (const auto & [directory, names] : all_file_names)
    {
        for (const auto & name : names)
        {
            // Ignore archive directory.
            if (name == PageStorage::ARCHIVE_SUBDIR)
                continue;

            auto [page_file, page_file_type] = PageFile::recover(directory, file_provider, name, page_file_log);
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
            else if (page_file_type == PageFile::Type::Temp)
            {
                if (option.remove_tmp_files)
                {
                    page_file.deleteEncryptionInfo();
                    // Remove temp files.
                    if (Poco::File file(directory + "/" + name); file.exists())
                        file.remove(true);
                }
            }
            else
            {
                // Remove invalid files.
                if (Poco::File file(directory + "/" + name); option.remove_invalid_files && file.exists())
                    file.remove(true);
            }
        }
    }

    return page_files;
}

PageStorage::PageStorage(String                  name,
                         PSDiskDelegatorPtr      delegator_, //
                         const Config &          config_,
                         const FileProviderPtr & file_provider_,
                         TiFlashMetricsPtr       metrics_)
    : storage_name(std::move(name)),
      delegator(std::move(delegator_)),
      config(config_),
      file_provider(file_provider_),
      write_files(std::max(1UL, config.num_write_slots)),
      page_file_log(&Poco::Logger::get("PageFile")),
      log(&Poco::Logger::get("PageStorage")),
      versioned_page_entries(storage_name, config.version_set_config, log),
      metrics(std::move(metrics_))
{
    // at least 1 write slots
    config.num_write_slots = std::max(1UL, config.num_write_slots);
    /// align write slots with numPathsForDelta
    const size_t num_paths = delegator->numPaths();
    if (num_paths >= config.num_write_slots)
    {
        // Extend the number of slots to make full use of all disks
        config.num_write_slots = num_paths;
    }
    else if (num_paths * 2 > config.num_write_slots)
    {
        // Extend the number of slots so that each path have 2 write slots. Avoid skew writes on different paths.
        config.num_write_slots = num_paths * 2;
    }
    write_files.resize(config.num_write_slots);
}

static inline bool isPageFileSizeFitsWritable(const PageFile & pf, const PageStorage::Config & config)
{
    return pf.getDataFileAppendPos() < config.file_roll_size && pf.getMetaFileAppendPos() < config.file_meta_roll_size;
}

void PageStorage::restore()
{
    LOG_INFO(log,
             storage_name << " begin to restore data from disk. [path=" << delegator->defaultPath()
                          << "] [num_writers=" << write_files.size() << "]");

    /// page_files are in ascending ordered by (file_id, level).
    ListPageFilesOption opt;
    opt.remove_tmp_files = true;
#ifdef PAGE_STORAGE_UTIL_DEBUGGGING
    opt.remove_tmp_files = false;
#endif
    opt.ignore_legacy = false;
    opt.ignore_checkpoint = false;
    opt.remove_invalid_files = true;
    PageFileSet page_files = PageStorage::listAllPageFiles(file_provider, delegator, page_file_log, opt);

    /// Restore current version from both formal and legacy page files

    MetaMergingQueue merging_queue;
    for (auto & page_file : page_files)
    {
        if (!(page_file.getType() == PageFile::Type::Formal || page_file.getType() == PageFile::Type::Legacy
              || page_file.getType() == PageFile::Type::Checkpoint))
            throw Exception("Try to recover from " + page_file.toString() + ", illegal type.", ErrorCodes::LOGICAL_ERROR);

        if (auto reader = PageFile::MetaMergingReader::createFrom(const_cast<PageFile &>(page_file)); //
            reader->hasNext())
        {
            // Read one WriteBatch
            reader->moveNext();
            merging_queue.push(std::move(reader));
        }
        // else the file doesn't contain any valid meta, just skip it.
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
        const auto cur_sequence = reader->writeBatchSequence();
        if (!checkpoint_sequence.has_value() || //
            (checkpoint_sequence.has_value() && (*checkpoint_sequence == 0 || *checkpoint_sequence <= cur_sequence)))
        {
            if (unlikely(cur_sequence > write_batch_seq + 1))
            {
                LOG_WARNING(log,
                            storage_name << " restore skip non-continuous sequence from " << write_batch_seq << " to " << cur_sequence
                                         << ", {" << reader->toString() << "}");
            }

            try
            {
                // LOG_TRACE(log, storage_name << " recovering from " + reader->toString());
                auto edits = reader->getEdits();
                versioned_page_entries.apply(edits);
                restore_info.mergeEdits(edits);
                write_batch_seq = cur_sequence;
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
        LOG_TRACE(log, storage_name << " These file would be archive:");
        for (auto & pf : page_files_to_remove)
            LOG_TRACE(log, storage_name << pf.toString());
#else
        // when restore `PageStorage`, the `PageFile` in `page_files_to_remove` is not counted in the total size,
        // so no need to remove its' size here again.
        archivePageFiles(page_files_to_remove, false);
#endif
        removePageFilesIf(page_files, [&page_files_to_remove](const PageFile & pf) -> bool { return page_files_to_remove.count(pf) > 0; });
    }

    // Fill write_files
    PageFileIdAndLevel max_page_file_id_lvl{0, 0};
    {
        const size_t        num_delta_paths = delegator->numPaths();
        std::vector<size_t> next_write_fill_idx(num_delta_paths);
        std::iota(next_write_fill_idx.begin(), next_write_fill_idx.end(), 0);
        // Only insert location of PageFile when it storing delta data
        for (auto & page_file : page_files)
        {
            max_page_file_id_lvl = std::max(max_page_file_id_lvl, page_file.fileIdLevel());
            // Checkpoint file is always stored on `delegator`'s default path, so no need to insert it's location here
            size_t idx_in_delta_paths = delegator->addPageFileUsedSize(
                page_file.fileIdLevel(),
                page_file.getDiskSize(),
                page_file.parentPath(),
                /*need_insert_location*/ page_file.getType() != PageFile::Type::Checkpoint);
            // Try best to reuse writable page files
            if (page_file.reusableForWrite() && isPageFileSizeFitsWritable(page_file, config))
            {
                auto & writing_files    = write_files[next_write_fill_idx[idx_in_delta_paths]];
                writing_files.file      = page_file;
                writing_files.persisted = PersistState{.meta_offset = page_file.getMetaFileAppendPos(), .sequence = write_batch_seq};

                // Next slot for writing files
                next_write_fill_idx[idx_in_delta_paths] = (next_write_fill_idx[idx_in_delta_paths] + num_delta_paths) % write_files.size();
            }
        }
    }

#ifndef PAGE_STORAGE_UTIL_DEBUGGGING
    std::vector<String> store_paths = delegator->listPaths();
    for (size_t i = 0; i < write_files.size(); ++i)
    {
        auto writer = checkAndRenewWriter(write_files[i], max_page_file_id_lvl, /*parent_path_hint=*/store_paths[i % store_paths.size()]);
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

// Check whether `page_file` is writable or not, renew `page_file` if need and return its belonging writer.
// - Writable, reuse `old_writer` if it is not a nullptr, otherwise, create a new writer from `page_file`
// - Not writable, renew the `page_file` and its belonging writer.
//   The <id,level> of the new `page_file` is <max_id + 1, 0> of all `write_files`
PageStorage::WriterPtr PageStorage::checkAndRenewWriter( //
    WritingPageFile &         writing_file,
    PageFileIdAndLevel        max_page_file_id_lvl_hint,
    const String &            parent_path_hint,
    PageStorage::WriterPtr && old_writer,
    const String &            logging_msg)
{
    WriterPtr write_file_writer;

    PageFile & page_file   = writing_file.file;
    bool       is_writable = page_file.isValid() && page_file.getType() == PageFile::Type::Formal //
        && isPageFileSizeFitsWritable(page_file, config);
    if (is_writable)
    {
        if (old_writer)
        {
            // Reuse the old_writer instead of creating a new writer
            write_file_writer.swap(old_writer);
        }
        else
        {
            // Create a new writer from `page_file`
            write_file_writer = page_file.createWriter(config.sync_on_write, false);
        }
    }
    else
    {
        /// Create a new PageFile and generate its belonging writer

        String pf_parent_path = delegator->defaultPath();
        if (old_writer)
        {
            // If the old_writer is not NULL, use the same parent path to create a new writer.
            // So the number of writers on different paths keep unchanged.
            pf_parent_path = old_writer->parentPath();
            // Reset writer to ensure all data have been flushed.
            old_writer.reset();
        }
        else if (!parent_path_hint.empty())
        {
            // Check whether caller has defined a hint path
            pf_parent_path = parent_path_hint;
        }

        PageFileIdAndLevel max_writing_id_lvl{max_page_file_id_lvl_hint};
        for (const auto & wf : write_files)
            max_writing_id_lvl = std::max(max_writing_id_lvl, wf.file.fileIdLevel());
        delegator->addPageFileUsedSize( //
            PageFileIdAndLevel(max_writing_id_lvl.first + 1, 0),
            0,
            pf_parent_path,
            /*need_insert_location*/ true);
        LOG_DEBUG(log,
                  storage_name << logging_msg << " create new PageFile_" << DB::toString(max_writing_id_lvl.first + 1)
                               << "_0 for write [path=" << pf_parent_path << "]");
        // Renew the `file` and `persisted.meta_offset`, keep `persisted.sequence` unchanged.
        writing_file.file
            = PageFile::newPageFile(max_writing_id_lvl.first + 1, 0, pf_parent_path, file_provider, PageFile::Type::Formal, page_file_log);
        writing_file.persisted.meta_offset = 0;

        write_file_writer = writing_file.file.createWriter(config.sync_on_write, true);
    }
    return write_file_writer;
}

PageStorage::ReaderPtr PageStorage::getReader(const PageFileIdAndLevel & file_id_level)
{
    std::lock_guard<std::mutex> lock(open_read_files_mutex);

    auto & pages_reader = open_read_files[file_id_level];
    if (pages_reader == nullptr)
    {
        String pf_parent_path = delegator->getPageFilePath(file_id_level);
        auto   page_file      = PageFile::openPageFileForRead(
            file_id_level.first, file_id_level.second, pf_parent_path, file_provider, PageFile::Type::Formal, page_file_log);
        if (unlikely(!page_file.isExist()))
            throw Exception("Try to create reader for " + page_file.toString() + ", but PageFile is broken, check "
                                + page_file.folderPath(),
                            ErrorCodes::LOGICAL_ERROR);
        pages_reader = page_file.createReader();
    }
    return pages_reader;
}

void PageStorage::write(WriteBatch && wb, const RateLimiterPtr & rate_limiter)
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
    try
    {
        wb.setSequence(++write_batch_seq); // Set sequence number to keep ordering between writers.
#ifdef FIU_ENABLE
        static int num_call = 0;
        num_call++;
#endif
        fiu_do_on(FailPoints::random_slow_page_storage_write, {
            if (num_call % 10 == 7)
            {
                pcg64                     rng(randomSeed());
                std::chrono::milliseconds ms{std::uniform_int_distribution(0, 900)(rng)}; // 0~900 milliseconds
                LOG_WARNING(log,
                            "Failpoint random_slow_page_storage_write sleep for " //
                                << ms.count() << "ms, WriteBatch with sequence=" << wb.getSequence());
                std::this_thread::sleep_for(ms);
            }
        });
        fiu_do_on(FailPoints::random_exception_after_page_storage_sequence_acquired, {
            if (num_call % 10 == 7)
                throw Exception("Failpoint random_exception_after_page_storage_sequence_acquired is triggered, WriteBatch with sequence="
                                    + DB::toString(wb.getSequence()) + " has been canceled",
                                ErrorCodes::FAIL_POINT_ERROR);
        });
        size_t bytes_written = file_to_write->write(wb, edit, rate_limiter);
        delegator->addPageFileUsedSize(file_to_write->fileIdLevel(),
                                       bytes_written,
                                       file_to_write->parentPath(),
                                       /*need_insert_location*/ false);
    }
    catch (...)
    {
        // Return writer into idle queue if exception thrown
        std::unique_lock lock(write_mutex);
        idle_writers.emplace_back(std::move(file_to_write));
        write_mutex_cv.notify_one(); // wake up any paused thread for write
        throw;
    }

    {
        // Return writer to idle queue
        std::unique_lock lock(write_mutex);
        size_t           index = 0;
        for (size_t i = 0; i < write_files.size(); ++i)
        {
            if (write_files[i].file.fileIdLevel() == file_to_write->fileIdLevel())
            {
                index = i;
                break;
            }
        }
        auto & writing_file    = write_files[index];
        writing_file.persisted = PersistState{.meta_offset = writing_file.file.getMetaFileAppendPos(), .sequence = wb.getSequence()};

        // Check whether we need to roll to new PageFile and its writer
        const auto logging_msg = " PageFile_" + DB::toString(writing_file.file.getFileId()) + "_0 is full,";
        file_to_write          = checkAndRenewWriter(writing_file, {0, 0}, "", std::move(file_to_write), logging_msg);

        idle_writers.emplace_back(std::move(file_to_write));

        write_mutex_cv.notify_one(); // wake up any paused thread for write
    }

    // Apply changes into versioned_page_entries(generate a new version)
    // If there are RefPages to non-exist Pages, just put the ref pair to new version
    // instead of throwing exception. Or we can't open PageStorage since we have already
    // persist the invalid ref pair into PageFile.
    versioned_page_entries.apply(edit);

    {
        std::unique_lock lock(write_mutex);
        statistics.mergeEdits(edit);
    }
}

PageStorage::SnapshotPtr PageStorage::getSnapshot()
{
    return versioned_page_entries.getSnapshot();
}

std::tuple<size_t, double, unsigned> PageStorage::getSnapshotsStat() const
{
    return versioned_page_entries.getSnapshotsStat();
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
    opt.ignore_legacy = false;
    opt.remove_tmp_files = false;
    opt.remove_invalid_files = false;
    auto page_files = PageStorage::listAllPageFiles(file_provider, delegator, page_file_log, opt);

    for (const auto & page_file : page_files)
    {
        // All checkpoint file is stored on `delegator`'s default path and we didn't record it's location as other types of PageFile,
        // so we need set `remove_from_default_path` to true to distinguish this situation.
        delegator->removePageFile(page_file.fileIdLevel(), page_file.getDiskSize(), /*meta_left*/ false, /*remove_from_default_path*/ page_file.getType() == PageFile::Type::Checkpoint);
    }

    /// FIXME: Note that these drop directories actions are not atomic, may leave some broken files on disk.

    // drop data paths
    for (const auto & path : delegator->listPaths())
    {
        if (Poco::File directory(path); directory.exists())
            file_provider->deleteDirectory(path, false, true);
    }

    LOG_INFO(log, storage_name << " drop done.");
}

struct GcContext
{
    PageFileIdAndLevel min_file_id;
    PageFile::Type     min_file_type;
    PageFileIdAndLevel max_file_id;
    PageFile::Type     max_file_type;
    size_t             num_page_files   = 0;
    size_t             num_legacy_files = 0;

    size_t num_files_archive_in_compact_legacy = 0;
    size_t num_bytes_written_in_compact_legacy = 0;

    DataCompactor<PageStorage::SnapshotPtr>::Result compact_result;

    // bytes written during gc
    size_t bytesWritten() const { return num_bytes_written_in_compact_legacy + compact_result.bytes_written; }

    PageStorage::StatisticsInfo gc_apply_stat;

    size_t num_files_remove_data = 0;
    size_t num_bytes_remove_data = 0;

    PageStorage::Config calculateGcConfig(const PageStorage::Config & config) const
    {
        PageStorage::Config res = config;
        // Each legacy is about serval hundred KiB or serval MiB
        // It means each time `gc` is called, we will read `num_legacy_file` * serval MiB
        // Do more agressive GC if there are too many Legacy files
        if (num_legacy_files > 50)
        {
            if (num_legacy_files > config.gc_max_expect_legacy_files)
            {
                // Hope that almost all files can be selected to migrate data to a new PageFile
                // Note that it should be 1.0 so that we can compact legacy files under extremely situation.
                res.gc_max_valid_rate = config.gc_max_valid_rate_bound;
            }
            else
            {
                res.gc_max_valid_rate = 0.65;
            }
            res.gc_min_files = 3;
            res.gc_min_bytes = PAGE_FILE_ROLL_SIZE / 2;
        }
        else if (num_legacy_files > 20)
        {
            res.gc_max_valid_rate = 0.40;
            res.gc_min_files      = 6;
            res.gc_min_bytes      = PAGE_FILE_ROLL_SIZE / 4 * 3;
        }
        return res;
    }
};

enum class GCType
{
    Normal = 0,
    Skip,
    LowWrite,
};

static String fileInfoToString(const PageFileIdAndLevel & id, const PageFile::Type type)
{
    return "[" + DB::toString(id.first) + "," + DB::toString(id.second) + "," + PageFile::typeToString(type) + "]";
}

void PageStorage::getWritingSnapshot(std::lock_guard<std::mutex> &, WritingFilesSnapshot & writing_snapshot) const
{
    for (const auto & wf : write_files)
    {
        writing_snapshot.states.emplace(wf.file.fileIdLevel(), wf.persisted);
    }
}

PageFileIdAndLevel PageStorage::WritingFilesSnapshot::minFileIDLevel() const
{
    if (likely(!states.empty()))
        return states.cbegin()->first;
    throw Exception("There is no writing files!", ErrorCodes::LOGICAL_ERROR);
}

WriteBatch::SequenceID PageStorage::WritingFilesSnapshot::minPersistedSequence() const
{
    if (unlikely(states.empty()))
        throw Exception("There is no writing files! Can not get min persisted sequence", ErrorCodes::LOGICAL_ERROR);

    auto                   iter = states.begin();
    WriteBatch::SequenceID seq  = iter->second.sequence;
    for (/**/; iter != states.end(); ++iter)
    {
        seq = std::min(seq, iter->second.sequence);
    }
    return seq;
}

bool PageStorage::gc(bool not_skip)
{
    // If another thread is running gc, just return;
    bool v = false;
    if (!gc_is_running.compare_exchange_strong(v, true))
        return false;

    SCOPE_EXIT({
        bool is_running = true;
        gc_is_running.compare_exchange_strong(is_running, false);
    });


    /// Get all pending external pages and PageFiles. Note that we should get external pages before PageFiles.
    PathAndIdsVec external_pages;
    if (external_pages_scanner)
    {
        external_pages = external_pages_scanner();
    }
    ListPageFilesOption opt;
    opt.remove_tmp_files = true;
    opt.remove_invalid_files = false;
    auto page_files = PageStorage::listAllPageFiles(file_provider, delegator, page_file_log, opt);
    if (unlikely(page_files.empty()))
    {
        // In case the directory are removed by accident
        LOG_WARNING(log, storage_name << " There are no page files while running GC");
        return false;
    }

    GcContext gc_context;
    gc_context.min_file_id    = page_files.begin()->fileIdLevel();
    gc_context.min_file_type  = page_files.begin()->getType();
    gc_context.max_file_id    = page_files.rbegin()->fileIdLevel();
    gc_context.max_file_type  = page_files.rbegin()->getType();
    gc_context.num_page_files = page_files.size();

    WritingFilesSnapshot writing_files_snapshot;
    StatisticsInfo       statistics_snapshot; // statistics snapshot copy with lock protection
    {
        std::lock_guard<std::mutex> lock(write_mutex);
        getWritingSnapshot(lock, writing_files_snapshot);

        /// If writer has not been used for too long, close the opened file fd of them.
        // Only close the idle fds in `idle_writers` under the `write_mutex` protection,
        // not all writable page files, or we may happen to close a fd in the middle of
        // writing.
        for (auto & writer : idle_writers)
        {
            writer->tryCloseIdleFd(config.open_file_max_idle_time);
        }
        statistics_snapshot = statistics;
    }
    PageFileIdAndLevel min_writing_file_id_level = writing_files_snapshot.minFileIDLevel();
    LOG_TRACE(log, storage_name << " Before gc, " << statistics_snapshot.toString());

    // Helper function for apply edits and clean up before gc exit.
    auto apply_and_cleanup = [&, this](PageEntriesEdit && gc_edits) -> void {
        /// Here we have to apply edit to versioned_page_entries and generate a new version, then return all files that are in used
        auto [live_files, live_normal_pages] = versioned_page_entries.gcApply(gc_edits, external_pages_scanner != nullptr);
        gc_context.gc_apply_stat.mergeEdits(gc_edits);

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

            // Close idle reader.
            // Other read threads may take a shared pointer of reader for reading. We can not prevent other readers
            // by locking at `open_read_files_mutex`. Instead of closing file descriptor, free the shared pointer
            // of idle readers here. The file descriptor will be closed after all readers done.
            for (auto iter = open_read_files.begin(); iter != open_read_files.end(); /*empty*/)
            {
                if (iter->second->isIdle(config.open_file_max_idle_time))
                    iter = open_read_files.erase(iter);
                else
                    ++iter;
            }
        }

        // Delete obsolete files that are not used by any version, without lock
        std::tie(gc_context.num_files_remove_data, gc_context.num_bytes_remove_data)
            = gcRemoveObsoleteData(page_files, min_writing_file_id_level, live_files);

        // Invoke callback with valid normal page id after gc.
        if (external_pages_remover)
        {
            external_pages_remover(external_pages, live_normal_pages);
        }
    };

    GCType gc_type = GCType::Normal;
    // Ignore page files that maybe writing to.
    do
    {
        if (not_skip) // For page_storage_ctl, don't skip the GC
        {
            gc_type = GCType::Normal;
            break;
        }
        /// Strategies to reduce useless GC actions.
        if (page_files.size() < 3)
        {
            // If only few page files, running gc is useless.
            gc_type = GCType::Skip;
        }
        else if (last_gc_statistics.equals(statistics_snapshot))
        {
            // No write since last gc. Give it a chance for running GC, ensure that we are able to
            // reclaim disk usage when PageStorage is read-only in extreme cases.
            if (DB::MVCC::utils::randInt(0, 1000) < config.prob_do_gc_when_write_is_low)
                gc_type = GCType::LowWrite;
            else
                gc_type = GCType::Skip;
        }

        // Shorcut for early exit GC routine.
        if (gc_type == GCType::Skip)
        {
            // Apply empty edit and cleanup.
            apply_and_cleanup(PageEntriesEdit{});
            LOG_TRACE(log, storage_name << " GC exit with no files to gc.");
            return false;
        }
    } while (0);

    Stopwatch watch;
    if (metrics)
    {
        if (gc_type == GCType::LowWrite)
            GET_METRIC(metrics, tiflash_storage_page_gc_count, type_low_write).Increment();
        else
            GET_METRIC(metrics, tiflash_storage_page_gc_count, type_exec).Increment();
    }
    SCOPE_EXIT({
        if (metrics)
        {
            GET_METRIC(metrics, tiflash_storage_page_gc_duration_seconds, type_exec).Observe(watch.elapsedSeconds());
        }
    });


#if !defined(NDEBUG)
    // Should not remove any {Formal/Legacy/Checkpoint} PageFiles before running LegacyCompactor, or we may skip some
    // WriteBatches while compacting legacy files.
    if (gc_context.num_page_files != page_files.size()                                                                              //
        || gc_context.min_file_id != page_files.begin()->fileIdLevel() || gc_context.min_file_type != page_files.begin()->getType() //
        || gc_context.max_file_id != page_files.rbegin()->fileIdLevel() || gc_context.max_file_type != page_files.rbegin()->getType())
    {
        throw Exception("Some page files are removed before running GC, should not happen [num_files="
                            + DB::toString(gc_context.num_page_files) + "] from "
                            + fileInfoToString(gc_context.min_file_id, gc_context.min_file_type) + " to "
                            + fileInfoToString(gc_context.max_file_id, gc_context.max_file_type) //
                            + " [real_num_files=" + DB::toString(page_files.size()) + "] from "
                            + fileInfoToString(page_files.begin()->fileIdLevel(), page_files.begin()->getType()) + " to "
                            + fileInfoToString(page_files.rbegin()->fileIdLevel(), page_files.rbegin()->getType()),
                        ErrorCodes::LOGICAL_ERROR);
    }
#endif

    // Count how many PageFiles are "legacy" or "checkpoint", we need to adjust
    // the GC param by `num_legacy_files`.
    for (const auto & page_file : page_files)
    {
        gc_context.num_legacy_files
            += static_cast<size_t>((page_file.getType() == PageFile::Type::Legacy) || (page_file.getType() == PageFile::Type::Checkpoint));
    }

    {
        // Try to compact consecutive Legacy PageFiles into a snapshot.
        // Legacy and checkpoint files will be removed from `page_files` after `tryCompact`.
        LegacyCompactor compactor(*this);
        PageFileSet     page_files_to_archive;
        std::tie(page_files, page_files_to_archive, gc_context.num_bytes_written_in_compact_legacy)
            = compactor.tryCompact(std::move(page_files), writing_files_snapshot);
        archivePageFiles(page_files_to_archive, true);
        gc_context.num_files_archive_in_compact_legacy = page_files_to_archive.size();
    }

    PageEntriesEdit gc_file_entries_edit;
    {
        /// Select the GC candidates files and migrate valid pages into an new file.
        /// Acquire a snapshot version of page map, new edit on page map store in `gc_file_entries_edit`
        Stopwatch watch_migrate;

        // Calculate a config by the gc context, maybe do a more aggressive GC
        DataCompactor<PageStorage::SnapshotPtr> compactor(*this, gc_context.calculateGcConfig(config));
        std::tie(gc_context.compact_result, gc_file_entries_edit) = compactor.tryMigrate(page_files, getSnapshot(), writing_files_snapshot);

        // We only care about those time cost in actually doing compaction on page data.
        if (gc_context.compact_result.do_compaction && metrics)
        {
            GET_METRIC(metrics, tiflash_storage_page_gc_duration_seconds, type_migrate).Observe(watch_migrate.elapsedSeconds());
        }
    }

    apply_and_cleanup(std::move(gc_file_entries_edit));

    // Simply copy without any locks, it should be fine since we only use it to skip useless GC routine when this PageStorage is cold.
    last_gc_statistics = statistics_snapshot;

    {
        std::stringstream ss;
        const auto        elapsed_sec = watch.elapsedSeconds();
        ss << storage_name << " GC exit within " << DB::toString(elapsed_sec, 2) << " sec. PageFiles from " //
           << fileInfoToString(gc_context.min_file_id, gc_context.min_file_type) << " to "
           << fileInfoToString(gc_context.max_file_id, gc_context.max_file_type) //
           << ", min writing " << fileInfoToString(min_writing_file_id_level, PageFile::Type::Formal)
           << ", num files: " << gc_context.num_page_files << ", num legacy:" << gc_context.num_legacy_files
           << ", compact legacy archive files: " << gc_context.num_files_archive_in_compact_legacy
           << ", remove data files: " << gc_context.num_files_remove_data << ", gc apply: " << gc_context.gc_apply_stat.toString();
        // Log warning if the GC run for a long time.
        constexpr double EXIST_LONG_GC = 30.0;
        if (elapsed_sec > EXIST_LONG_GC)
            LOG_WARNING(log, ss.str());
        else
            LOG_INFO(log, ss.str());
    }
    return gc_context.compact_result.do_compaction;
}

void PageStorage::archivePageFiles(const PageFileSet & page_files, bool remove_size)
{
    const Poco::Path archive_path(delegator->defaultPath(), PageStorage::ARCHIVE_SUBDIR);
    Poco::File       archive_dir(archive_path);
    do
    {
        // Clean archive file no matter `page_files` is empty or not.
        if (page_files.empty())
            break;

        if (!archive_dir.exists())
            archive_dir.createDirectory();

        for (auto & page_file : page_files)
        {
            Poco::Path path(page_file.folderPath());
            auto       dest = archive_path.toString() + "/" + path.getFileName();
            if (Poco::File file(path); file.exists())
            {
                // To ensure the atomic of deletion, move to the `archive` dir first and then remove the PageFile dir.
                auto file_size = remove_size ? page_file.getDiskSize() : 0;
                file.moveTo(dest);
                file.remove(true);
                page_file.deleteEncryptionInfo();
                // All checkpoint file is stored on `delegator`'s default path and we didn't record it's location as other types of PageFile,
                // so we need set `remove_from_default_path` to true to distinguish this situation.
                delegator->removePageFile(page_file.fileIdLevel(), file_size, /*meta_left*/ false, /*remove_from_default_path*/ page_file.getType() == PageFile::Type::Checkpoint);
            }
        }
        LOG_INFO(log, storage_name << " archive " + DB::toString(page_files.size()) + " files to " + archive_path.toString());
    } while (0);

    // Maybe there are a large number of files left on disk by TiFlash version v4.0.0~v4.0.11, or some files left on disk
    // by unexpected crash in the middle of archiving PageFiles.
    // In order not to block the GC thread for a long time and make the IO smooth, only remove
    // `MAX_NUM_OF_FILE_TO_REMOVED` files at maximum.
    Strings archive_page_files;
    if (!archive_dir.exists())
        return;

    archive_dir.list(archive_page_files);
    if (archive_page_files.empty())
        return;

    const size_t MAX_NUM_OF_FILE_TO_REMOVED = 30;
    size_t       num_removed                = 0;
    for (const auto & pf_dir : archive_page_files)
    {
        if (Poco::File file(Poco::Path(archive_path, pf_dir)); file.exists())
        {
            file.remove(true);
            ++num_removed;
        }

        if (num_removed >= MAX_NUM_OF_FILE_TO_REMOVED)
        {
            break;
        }
    }
    size_t num_left = archive_page_files.size() > num_removed ? (archive_page_files.size() - num_removed) : 0;
    LOG_INFO(log,
             storage_name << " clean " << num_removed << " files in archive dir, " << num_left
                          << " files are left to be clean in the next round.");
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
            // https://stackoverflow.com/questions/9726375/stdset-iterator-automatically-const
            // Don't touch the <file_id, level> that are used for the sorting then you could
            // work around by using a const_cast
            size_t bytes_removed = const_cast<PageFile &>(page_file).setLegacy();
            // All checkpoint file is stored on `delegator`'s default path and we didn't record it's location as other types of PageFile,
            // so we need set `remove_from_default_path` to true to distinguish this situation.
            delegator->removePageFile(page_id_and_lvl, bytes_removed, /*meta_left*/ true, /*remove_from_default_path*/ page_file.getType() == PageFile::Type::Checkpoint);
            num_data_removed += 1;
        }
    }
    return {num_data_removed, num_bytes_removed};
}

} // namespace DB
