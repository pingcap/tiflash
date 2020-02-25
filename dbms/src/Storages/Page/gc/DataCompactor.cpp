#include <IO/ReadBufferFromMemory.h>
#include <Storages/Page/gc/DataCompactor.h>

namespace DB
{

DataCompactor::DataCompactor(const PageStorage & storage)
    : storage_name(storage.storage_name),
      storage_path(storage.storage_path),
      config(storage.config),
      log(storage.log),
      page_file_log(storage.page_file_log)
{
}

std::tuple<DataCompactor::Result, PageEntriesEdit>
DataCompactor::tryMigrate(const PageFileSet & page_files, SnapshotPtr && snapshot, const std::set<PageFileIdAndLevel> & writing_file_ids)
{
    ValidPages valid_pages = collectValidPagesInPageFile(snapshot);

    // Select gc candidate files
    Result      result;
    PageFileSet candidates;
    std::tie(candidates, result.bytes_migrate, result.num_migrate_pages) = selectCandidateFiles(page_files, valid_pages, writing_file_ids);

    result.candidate_size = candidates.size();
    result.do_compaction  = result.candidate_size >= config.merge_hint_low_used_file_num
        || (candidates.size() >= 2 && result.bytes_migrate >= config.merge_hint_low_used_file_total_size);

    // Scan over all `candidates` and do migrate.
    PageEntriesEdit migrate_entries_edit;
    if (result.do_compaction)
    {
        migrate_entries_edit = migratePages(snapshot, valid_pages, candidates, result.num_migrate_pages);
    }

    return {result, std::move(migrate_entries_edit)};
}

DataCompactor::ValidPages DataCompactor::collectValidPagesInPageFile(const PageStorage::SnapshotPtr & snapshot)
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

std::tuple<PageFileSet, size_t, size_t> DataCompactor::selectCandidateFiles( // keep readable indent
    const PageFileSet &                  page_files,
    const ValidPages &                   file_valid_pages,
    const std::set<PageFileIdAndLevel> & writing_file_ids) const
{
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

        if (auto it = file_valid_pages.find(page_file.fileIdLevel()); it != file_valid_pages.end())
        {
            valid_size       = it->second.first;
            valid_rate       = (float)valid_size / file_size;
            valid_page_count = it->second.second.size();
        }

        // Don't gc writing page file.
        bool is_candidate = (writing_file_ids.count(page_file.fileIdLevel()) > 0)
            && (valid_rate < config.merge_hint_low_used_rate || file_size < config.file_small_size);
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

bool DataCompactor::isMigrateFileExist(const PageFileIdAndLevel & file_id) const
{
    // In case that those files are hold by snapshot and do migratePages to same PageFile again, we need to check if gc_file is already exist.
    PageFile gc_file = PageFile::openPageFileForRead(file_id.first, file_id.second, storage_path, PageFile::Type::Formal, page_file_log);
    return gc_file.isExist();
}

PageEntriesEdit DataCompactor::migratePages(const SnapshotPtr & snapshot,
                                            const ValidPages &  file_valid_pages,
                                            const PageFileSet & candidates,
                                            const size_t        migrate_page_count) const
{
    if (candidates.empty())
        return {};

    // merge `candidates` to PageFile which PageId = max of all `candidates` and level = level + 1
    auto [largest_file_id, level] = candidates.rbegin()->fileIdLevel();
    const PageFileIdAndLevel migrate_file_id{largest_file_id, level + 1};

    // In case that those files are hold by snapshot and do migratePages to same PageFile again, we need to check if gc_file is already exist.
    if (isMigrateFileExist(migrate_file_id))
    {
        LOG_INFO(log,
                 storage_name << " GC migration to PageFile_" //
                              << migrate_file_id.first << "_" << migrate_file_id.second << " is done before.");
        return {};
    }

    // Create a tmp PageFile for migration
    PageFile gc_file
        = PageFile::newPageFile(migrate_file_id.first, migrate_file_id.second, storage_path, PageFile::Type::Temp, page_file_log);
    LOG_INFO(log,
             storage_name << " GC decide to compact " << candidates.size() << " files, containing " << migrate_page_count
                          << " pages to PageFile_" << gc_file.getFileId() << "_" << gc_file.getLevel());

    PageEntriesEdit          gc_file_edit;
    std::vector<MigrateInfo> migrate_infos;
    {
        PageStorage::OpenReadFiles           data_readers;
        PageStorage::MetaCompactMergineQueue merging_queue;
        for (auto & page_file : candidates)
        {
            if (page_file.getType() != PageFile::Type::Formal)
            {
                throw Exception("Try to migrate pages from invalid type PageFile: " + page_file.toString()
                                    + ", path: " + page_file.folderPath(),
                                ErrorCodes::LOGICAL_ERROR);
            }

            if (auto it = file_valid_pages.find(page_file.fileIdLevel()); it == file_valid_pages.end())
            {
                // This file does not contain any valid page.
                migrate_infos.emplace_back(MigrateInfo{.file_id = page_file.fileIdLevel(), .num_pages = 0, .sequence = 0});
                continue;
            }

            {
                PageEntriesEdit        edit;
                WriteBatch::SequenceID seq;
                const_cast<PageFile &>(page_file).readAndSetPageMetas(edit, seq);
                auto data_reader = const_cast<PageFile &>(page_file).createReader();
                data_readers.emplace(page_file.fileIdLevel(), std::move(data_reader));
            }
            auto reader = const_cast<PageFile &>(page_file).createMetaMergingReader();
            // Read one valid WriteBatch
            reader->moveNext();
            merging_queue.push(std::move(reader));
        }

        // Merge all WriteBatch with valid pages, sorted by WriteBatch::sequence
        gc_file_edit
            = mergeValidPages(std::move(merging_queue), std::move(data_readers), file_valid_pages, snapshot, gc_file, migrate_infos);
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
        for (const auto & info : migrate_infos)
            num_migrate_pages += info.num_pages;
        LOG_INFO(log,
                 storage_name << " GC have migrated " << num_migrate_pages //
                              << " Pages to PageFile_" << migrate_file_id.first << "_" << migrate_file_id.second);
    }
    return gc_file_edit;
}

PageEntriesEdit DataCompactor::mergeValidPages(PageStorage::MetaCompactMergineQueue && merging_queue,
                                               PageStorage::OpenReadFiles &&           data_readers,
                                               const ValidPages &                      file_valid_pages,
                                               const SnapshotPtr &                     snapshot,
                                               PageFile &                              gc_file,
                                               std::vector<MigrateInfo> &              migrate_infos) const
{
    PageEntriesEdit gc_file_edit;
    // No need to sync after each write. Do sync before closing is enough.
    auto gc_file_writer = gc_file.createWriter(/* sync_on_write= */ false);

    while (!merging_queue.empty())
    {
        auto reader = merging_queue.top();
        merging_queue.pop();

        auto it = file_valid_pages.find(reader->fileIdLevel());
        if (it == file_valid_pages.end())
        {
            throw Exception();
        }

        auto & valid_pages_in_file = it->second.second;
        auto   page_id_and_entries = collectValidEntries(reader->getEdits(), valid_pages_in_file, snapshot);
        if (!page_id_and_entries.empty())
        {
            // Copy valid pages from `data_reader` to `gc_file`
            auto          data_reader = data_readers.at(reader->fileIdLevel());
            const PageMap pages       = data_reader->read(page_id_and_entries);
            WriteBatch    wb;
            wb.setSequence(reader->writeBatchSequence());
            for (const auto & [page_id, entry] : page_id_and_entries)
            {
                const auto page = pages.find(page_id)->second;
                wb.upsertPage(page_id,
                              entry.tag, //
                              std::make_shared<ReadBufferFromMemory>(page.data.begin(), page.data.size()),
                              page.data.size(),
                              entry.field_offsets);
            }
            gc_file_writer->write(wb, gc_file_edit);
        }
        migrate_infos.emplace_back(MigrateInfo{
            .file_id = reader->fileIdLevel(), .num_pages = page_id_and_entries.size(), .sequence = reader->writeBatchSequence()});

        //
        if (reader->hasNext())
        {
            reader->moveNext();
            merging_queue.push(reader);
        }
    }

    // Free gc_file_writer and sync
    return gc_file_edit;
}

PageIdAndEntries
DataCompactor::collectValidEntries(PageEntriesEdit && edits, const PageIdSet & valid_pages, const SnapshotPtr & snapshot) const
{
    PageIdAndEntries page_id_and_entries; // The valid pages that we need to migrate to `gc_file`
    for (auto & edit : edits.getRecords())
    {
        if (edit.type == WriteBatch::WriteType::PUT || edit.type == WriteBatch::WriteType::UPSERT)
        {
            if (!valid_pages.count(edit.page_id))
                continue;
            const auto page_entry = snapshot->version()->findNormalPageEntry(edit.page_id);
            if (!page_entry)
                continue;
            page_id_and_entries.emplace_back(edit.page_id, *page_entry);
        }
    }
    return page_id_and_entries;
}

void DataCompactor::logMigrationDetails(const std::vector<MigrateInfo> & infos, const PageFileIdAndLevel & migrate_file_id) const
{
    std::stringstream migrate_stream, remove_stream;
    migrate_stream << "[";
    remove_stream << "[";
    for (const auto & info : infos)
    {
        if (info.num_pages > 0)
            migrate_stream << "((" << DB::toString(info.file_id.first) << "," << DB::toString(info.file_id.second) << ")," //
                           << DB::toString(info.num_pages) << "," << DB::toString(info.sequence) << "),";
        else
            remove_stream << "(" << DB::toString(info.file_id.first) << "," << DB::toString(info.file_id.second) << "),";
    }
    migrate_stream << "]";
    remove_stream << "]";
    LOG_DEBUG(log,
              storage_name << " Migrate pages to PageFile_" << migrate_file_id.first << "_" << migrate_file_id.second
                           << ", mirgrate: " << migrate_stream.str() << ", remove: " << remove_stream.str());
}


} // namespace DB
