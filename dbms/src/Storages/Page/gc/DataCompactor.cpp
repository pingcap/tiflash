#include <IO/ReadBufferFromMemory.h>
#include <Storages/Page/gc/DataCompactor.h>
#include <Storages/PathPool.h>

#ifndef NDEBUG
#include <Storages/Page/mock/MockUtils.h>
#endif

namespace DB
{

template <typename SnapshotPtr>
DataCompactor<SnapshotPtr>::DataCompactor(const PageStorage & storage, PageStorage::Config gc_config)
    : storage_name(storage.storage_name),
      delegator(storage.delegator),
      file_provider(storage.getFileProvider()),
      config(std::move(gc_config)),
      log(storage.log),
      page_file_log(storage.page_file_log)
{
}

template <typename SnapshotPtr>
std::tuple<typename DataCompactor<SnapshotPtr>::Result, PageEntriesEdit> //
DataCompactor<SnapshotPtr>::tryMigrate(                                  //
    const PageFileSet &                  page_files,
    SnapshotPtr &&                       snapshot,
    const std::set<PageFileIdAndLevel> & writing_file_ids)
{
    ValidPages valid_pages = collectValidPagesInPageFile(snapshot);

    // Select gc candidate files
    Result      result;
    PageFileSet candidates;
    std::tie(candidates, result.bytes_migrate, result.num_migrate_pages) = selectCandidateFiles(page_files, valid_pages, writing_file_ids);

    result.candidate_size = candidates.size();
    result.do_compaction
        = result.candidate_size >= config.gc_min_files || (candidates.size() >= 2 && result.bytes_migrate >= config.gc_min_bytes);

    // Scan over all `candidates` and do migrate.
    PageEntriesEdit migrate_entries_edit;
    if (result.do_compaction)
    {
        std::tie(migrate_entries_edit, result.bytes_written) = migratePages(snapshot, valid_pages, candidates, result.num_migrate_pages);
    }
    else
    {
        LOG_DEBUG(log,
                  storage_name << " DataCompactor::tryMigrate exit without compaction, [candidates size=" //
                               << result.candidate_size << "] [total byte size=" << result.bytes_migrate << "], Config{ "
                               << config.toDebugString() << " }");
    }

    return {result, std::move(migrate_entries_edit)};
}

template <typename SnapshotPtr>
typename DataCompactor<SnapshotPtr>::ValidPages //
DataCompactor<SnapshotPtr>::collectValidPagesInPageFile(const SnapshotPtr & snapshot)
{
    ValidPages valid_pages;
    // Only scan over normal Pages, excluding RefPages
    auto valid_normal_page_ids = snapshot->version()->validNormalPageIds();
    for (auto page_id : valid_normal_page_ids)
    {
        const auto page_entry = snapshot->version()->findNormalPageEntry(page_id);
        if (unlikely(!page_entry))
        {
            throw Exception("PageStorage GC: Normal Page " + DB::toString(page_id) + " not found.", ErrorCodes::LOGICAL_ERROR);
        }
        auto && [valid_size, valid_page_ids_in_file] = valid_pages[page_entry->fileIdLevel()];
        valid_size += page_entry->size;
        valid_page_ids_in_file.emplace(page_id);
    }
    return valid_pages;
}

template <typename SnapshotPtr>
std::tuple<PageFileSet, size_t, size_t>           //
DataCompactor<SnapshotPtr>::selectCandidateFiles( // keep readable indent
    const PageFileSet &                  page_files,
    const ValidPages &                   files_valid_pages,
    const std::set<PageFileIdAndLevel> & writing_file_ids) const
{
#ifdef PAGE_STORAGE_UTIL_DEBUGGGING
    LOG_TRACE(log, storage_name << " input size of candidates: " << page_files.size());
#endif

    PageFileSet candidates;
    size_t      candidate_total_size = 0;
    size_t      num_migrate_pages    = 0;
    for (auto & page_file : page_files)
    {
        if (unlikely(page_file.getType() != PageFile::Type::Formal))
        {
            throw Exception("Try to pick " + page_file.toString() + " as gc candidate, path: " + page_file.folderPath(),
                            ErrorCodes::LOGICAL_ERROR);
        }

        const auto file_size        = page_file.getDataFileSize();
        UInt64     valid_size       = 0;
        float      valid_rate       = 0.0f;
        size_t     valid_page_count = 0;

        if (auto it = files_valid_pages.find(page_file.fileIdLevel()); it != files_valid_pages.end())
        {
            valid_size       = it->second.first;
            valid_rate       = (float)valid_size / file_size;
            valid_page_count = it->second.second.size();
        }

        // Don't gc writing page file.
        bool is_candidate = (writing_file_ids.count(page_file.fileIdLevel()) == 0)
            && (valid_rate < config.gc_max_valid_rate //
                || file_size < config.file_small_size //
                || config.gc_max_valid_rate >= 1.0    // all page file will be picked
            );
#ifdef PAGE_STORAGE_UTIL_DEBUGGGING
        LOG_TRACE(log,
                  storage_name << " " << page_file.toString() << " [valid rate=" << DB::toString(valid_rate, 2)
                               << "] [file size=" << file_size << "]");
#endif
        if (!is_candidate)
        {
            continue;
        }

        candidates.emplace(page_file);
        num_migrate_pages += valid_page_count;
        candidate_total_size += valid_size;
        if (candidate_total_size >= config.file_max_size)
        {
            break;
        }
    }
    return {candidates, candidate_total_size, num_migrate_pages};
}

template <typename SnapshotPtr>
std::tuple<PageEntriesEdit, size_t>       //
DataCompactor<SnapshotPtr>::migratePages( //
    const SnapshotPtr & snapshot,
    const ValidPages &  files_valid_pages,
    const PageFileSet & candidates,
    const size_t        migrate_page_count) const
{
    if (candidates.empty())
        return {PageEntriesEdit{}, 0};

    // merge `candidates` to PageFile which PageId = max of all `candidates` and level = level + 1
    auto [largest_file_id, level] = candidates.rbegin()->fileIdLevel();
    const PageFileIdAndLevel migrate_file_id{largest_file_id, level + 1};

    // In case that those files are hold by snapshot and do migratePages to same `migrate_file_id` again, we need to check
    // whether gc_file (and its legacy file) is already exist.
    //
    // For example:
    //   First round:
    //     PageFile_998_0, PageFile_999_0, PageFile_1000_0
    //        ^                                ^
    //        └────────────────────────────────┘
    //   Only PageFile_998_0 and PageFile_1000_0 are picked as candidates, it will generate PageFile_1000_1 for storing
    //   GC data in this round.
    //
    //   Second round:
    //     PageFile_998_0, PageFile_999_0, PageFile_1000_0
    //        ^                ^               ^
    //        └────────────────┵───────────────┘
    //   Some how PageFile_1000_0 don't get deleted (maybe there is a snapshot that need to read Pages inside it) and
    //   we start a new round of GC. PageFile_998_0(again), PageFile_999_0(new), PageFile_1000_0(again) are picked into
    //   candidates and 1000_0 is the largest file_id.
    {
        // We need to check existence for multi disks deployment, or we may generate the same file id
        // among different disks, some of them will be ignored by the PageFileSet because of duplicated
        // file id while restoring from disk
        const auto paths = delegator->listPaths();
        for (const auto & pf_parent_path : paths)
        {
            if (PageFile::isPageFileExist(migrate_file_id, pf_parent_path, file_provider, PageFile::Type::Formal, page_file_log)
                || PageFile::isPageFileExist(migrate_file_id, pf_parent_path, file_provider, PageFile::Type::Legacy, page_file_log))
            {
                LOG_INFO(log,
                         storage_name << " GC migration to PageFile_" //
                                      << migrate_file_id.first << "_" << migrate_file_id.second << " is done before.");
                return {PageEntriesEdit{}, 0};
            }
        }
        // else the PageFile is not exists in all paths, continue migration.
    }

    // Choose one path for creating a tmp PageFile for migration
    const String pf_parent_path = delegator->choosePath(migrate_file_id);
    PageFile     gc_file        = PageFile::newPageFile(
        migrate_file_id.first, migrate_file_id.second, pf_parent_path, file_provider, PageFile::Type::Temp, page_file_log);
    LOG_INFO(log,
             storage_name << " GC decide to migrate " << candidates.size() << " files, containing " << migrate_page_count
                          << " pages to PageFile_" << gc_file.getFileId() << "_" << gc_file.getLevel() << ", path " << pf_parent_path);

    PageEntriesEdit gc_file_edit;
    size_t          bytes_written = 0;
    MigrateInfos    migrate_infos;
    {
        PageStorage::OpenReadFiles data_readers;
        // To keep the order of all PageFiles' meta, `compact_seq` should be maximum of all candidates.
        // No matter we merge valid page(s) from it or not.
        WriteBatch::SequenceID compact_seq = 0;
        for (auto & page_file : candidates)
        {
            if (page_file.getType() != PageFile::Type::Formal)
            {
                throw Exception("Try to migrate pages from invalid type PageFile: " + page_file.toString()
                                    + ", path: " + page_file.folderPath(),
                                ErrorCodes::LOGICAL_ERROR);
            }

            if (auto it = files_valid_pages.find(page_file.fileIdLevel()); it == files_valid_pages.end())
            {
                // This file does not contain any valid page.
                migrate_infos.emplace_back(page_file.fileIdLevel(), 0);
                continue;
            }

            // Create meta reader and update `compact_seq`
            auto meta_reader = const_cast<PageFile &>(page_file).createMetaMergingReader();
            while (meta_reader->hasNext())
            {
                meta_reader->moveNext();
                compact_seq = std::max(compact_seq, meta_reader->writeBatchSequence());
            }

            // Create data reader
            auto data_reader = const_cast<PageFile &>(page_file).createReader();
            data_readers.emplace(page_file.fileIdLevel(), std::move(data_reader));
        }

        // Merge all WriteBatch with valid pages, sorted by WriteBatch::sequence
        std::tie(gc_file_edit, bytes_written)
            = mergeValidPages(std::move(data_readers), files_valid_pages, snapshot, compact_seq, gc_file, migrate_infos);
    }

    logMigrationDetails(migrate_infos, migrate_file_id);

    if (gc_file_edit.empty())
    {
        LOG_INFO(log, storage_name << " No valid pages, deleting PageFile_" << migrate_file_id.first << "_" << migrate_file_id.second);
        gc_file.destroy();
    }
    else
    {
        gc_file.setFormal();
        size_t num_migrate_pages = 0;
        for (const auto & [file_id, num_pages] : migrate_infos)
        {
            (void)file_id;
            num_migrate_pages += num_pages;
        }
        LOG_INFO(log,
                 storage_name << " GC have migrated " << num_migrate_pages //
                              << " Pages to PageFile_" << migrate_file_id.first << "_" << migrate_file_id.second);
    }
    return {std::move(gc_file_edit), bytes_written};
}

template <typename SnapshotPtr>
std::tuple<PageEntriesEdit, size_t>          //
DataCompactor<SnapshotPtr>::mergeValidPages( //
    PageStorage::OpenReadFiles && data_readers,
    const ValidPages &            files_valid_pages,
    const SnapshotPtr &           snapshot,
    const WriteBatch::SequenceID  compact_sequence,
    PageFile &                    gc_file,
    MigrateInfos &                migrate_infos) const
{
    PageEntriesEdit gc_file_edit;
    const auto      gc_file_id = gc_file.fileIdLevel();
    // No need to sync after each write. Do sync before closing is enough.
    auto   gc_file_writer = gc_file.createWriter(/* sync_on_write= */ false, true);
    size_t bytes_written  = 0;

    // When all data of one PageFile is obsoleted, we just remove its data but leave its meta part on disk.
    // And all migrated data will be written as multiple WriteBatches(one WriteBatch for one candidate
    // PageFile) with same sequence. To keep the order of all PageFiles' meta, the sequence of WriteBatch
    // should be maximum of all candidates' WriteBatches. No matter we merge valid page(s) from that
    // WriteBatch or not.
    // Next time we recover pages' meta from disk, recover checkpoint first, then merge all pages' meta
    // according to the tuple <Sequence, PageFileId, PageFileLevel> is OK.

    for (auto iter = files_valid_pages.cbegin(); iter != files_valid_pages.cend(); ++iter)
    {
        const auto & file_id_level                          = iter->first;
        const auto & [_valid_bytes, valid_page_ids_in_file] = iter->second;
        (void)_valid_bytes;

        auto reader_iter = data_readers.find(file_id_level);
        if (reader_iter == data_readers.end())
            continue;

        // One WriteBatch for one candidate.
        auto page_id_and_entries = collectValidEntries(valid_page_ids_in_file, snapshot);
        if (!page_id_and_entries.empty())
        {
            auto &        data_reader = reader_iter->second;
            const PageMap pages       = data_reader->read(page_id_and_entries);
            WriteBatch    wb;
            wb.setSequence(compact_sequence);
            for (const auto & [page_id, entry] : page_id_and_entries)
            {
                // Upsert page to gc_file
                const auto page = pages.find(page_id)->second;
                wb.upsertPage(page_id,
                              entry.tag,
                              gc_file_id,
                              std::make_shared<ReadBufferFromMemory>(page.data.begin(), page.data.size()),
                              page.data.size(),
                              entry.field_offsets);
            }
            bytes_written += gc_file_writer->write(wb, gc_file_edit);
        }

        migrate_infos.emplace_back(file_id_level, page_id_and_entries.size());
    }

    // Free gc_file_writer and sync
    delegator->addPageFileUsedSize(gc_file_id, bytes_written, gc_file.parentPath(), /*need_insert_location*/ true);
    return {std::move(gc_file_edit), bytes_written};
}

template <typename SnapshotPtr>
PageIdAndEntries DataCompactor<SnapshotPtr>::collectValidEntries(const PageIdSet & valid_pages, const SnapshotPtr & snapshot)
{
    PageIdAndEntries page_id_and_entries; // The valid pages that we need to migrate to `gc_file`
    for (const auto page_id : valid_pages)
    {
        const auto page_entry = snapshot->version()->findNormalPageEntry(page_id);
        if (!page_entry)
            continue;
        page_id_and_entries.emplace_back(page_id, *page_entry);
    }
    return page_id_and_entries;
}

template <typename SnapshotPtr>
void DataCompactor<SnapshotPtr>::logMigrationDetails(const MigrateInfos & infos, const PageFileIdAndLevel & migrate_file_id) const
{
    std::stringstream migrate_stream, remove_stream;
    migrate_stream << "[";
    remove_stream << "[";
    for (const auto & [file_id, num_pages] : infos)
    {
        if (num_pages > 0)
            migrate_stream << "((" << file_id.first << "," << file_id.second << ")," //
                           << num_pages << "),";
        else
            remove_stream << "(" << file_id.first << "," << file_id.second << "),";
    }
    migrate_stream << "]";
    remove_stream << "]";
    LOG_DEBUG(log,
              storage_name << " Migrate pages to PageFile_" << migrate_file_id.first << "_" << migrate_file_id.second
                           << ", migrate: " << migrate_stream.str() << ", remove: " << remove_stream.str() << ", Config{ "
                           << config.toDebugString() << " }");
}


template class DataCompactor<PageStorage::SnapshotPtr>;
#ifndef NDEBUG
template class DataCompactor<DB::tests::MockSnapshotPtr>;
#endif

} // namespace DB
