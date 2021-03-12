#include <Storages/Page/gc/LegacyCompactor.h>
#include <Storages/Page/gc/restoreFromCheckpoints.h>
#include <Storages/PathPool.h>

namespace DB
{
LegacyCompactor::LegacyCompactor(const PageStorage & storage)
    : storage_name(storage.storage_name),
      delegator(storage.delegator),
      file_provider(storage.getFileProvider()),
      config(storage.config),
      log(storage.log),
      page_file_log(storage.page_file_log),
      version_set(storage.storage_name + ".legacy_compactor", config.version_set_config, log)
{
}

std::tuple<PageFileSet, PageFileSet, size_t> //
LegacyCompactor::tryCompact(                 //
    PageFileSet &&                       page_files,
    const std::set<PageFileIdAndLevel> & writing_file_ids)
{
    // Select PageFiles to compact, all compacted WriteBatch will apply to `this->version_set`
    PageFileSet             page_files_to_remove;
    PageFileSet             page_files_to_compact;
    WriteBatch::SequenceID  checkpoint_sequence = 0;
    std::optional<PageFile> old_checkpoint;
    std::tie(page_files_to_remove, page_files_to_compact, checkpoint_sequence, old_checkpoint)
        = collectPageFilesToCompact(page_files, writing_file_ids);

    if (page_files_to_compact.size() < config.gc_min_legacy_num)
    {
        LOG_DEBUG(log,
                  storage_name << " LegacyCompactor::tryCompact exit without compaction, candidates size: "
                               << page_files_to_compact.size() //
                               << ", compact_legacy_min_num: " << config.gc_min_legacy_num);
        // Nothing to compact, remove legacy/checkpoint page files since we
        // don't do gc on them later.
        removePageFilesIf(page_files, [](const PageFile & pf) -> bool {
            return pf.getType() == PageFile::Type::Legacy || pf.getType() == PageFile::Type::Checkpoint;
        });
        return {std::move(page_files), {}, 0};
    }

    // Use the largest id-level in page_files_to_compact as Checkpoint's file
    const PageFileIdAndLevel checkpoint_id = page_files_to_compact.rbegin()->fileIdLevel();

    const String storage_path = delegator->defaultPath();
    if (PageFile::isPageFileExist(checkpoint_id, storage_path, file_provider, PageFile::Type::Checkpoint, page_file_log))
    {
        LOG_WARNING(log,
                    storage_name << " LegacyCompactor::tryCompact to checkpoint PageFile_" //
                                 << checkpoint_id.first << "_" << checkpoint_id.second << " is done before.");
        // Nothing to compact, remove legacy/checkpoint page files since we
        // don't do gc on them later.
        removePageFilesIf(page_files, [](const PageFile & pf) -> bool {
            return pf.getType() == PageFile::Type::Legacy || pf.getType() == PageFile::Type::Checkpoint;
        });
        return {std::move(page_files), {}, 0};
    }

    // Build a version_set with snapshot
    auto snapshot = version_set.getSnapshot();
    auto wb       = prepareCheckpointWriteBatch(snapshot, checkpoint_sequence);

    {
        std::stringstream legacy_ss;
        legacy_ss << "[";
        for (const auto & page_file : page_files_to_compact)
            legacy_ss << "(" << page_file.getFileId() << "," << page_file.getLevel() << "),";
        legacy_ss << "]";
        const String old_checkpoint_str = (old_checkpoint ? old_checkpoint->toString() : "(none)");

        LOG_INFO(log,
                 storage_name << " Compact legacy PageFile " << legacy_ss.str()                                     //
                              << " and old checkpoint: " << old_checkpoint_str                                      //
                              << " into checkpoint PageFile_" << checkpoint_id.first << "_" << checkpoint_id.second //
                              << " with " << info.toString() << " sequence: " << checkpoint_sequence);
    }

    size_t bytes_written = 0;
    if (!info.empty())
    {
        bytes_written = writeToCheckpoint(storage_path, checkpoint_id, std::move(wb), file_provider, page_file_log);
        // Don't need to insert location since Checkpoint PageFile won't be read except using listAllPageFiles in `PageStorage::restore`
        delegator->addPageFileUsedSize(checkpoint_id, bytes_written, storage_path, /*need_insert_location=*/false);
    }

    // Clean up compacted PageFiles from `page_files`
    {
        // We have generate a new checkpoint, old checkpoint can be remove later.
        if (!info.empty() && old_checkpoint)
            page_files_to_remove.emplace(*old_checkpoint);
        // Compacted files can be remove later
        for (const auto & pf : page_files_to_compact)
            page_files_to_remove.emplace(pf);

        removePageFilesIf(page_files, [&page_files_to_remove](const PageFile & pf) -> bool {
            // Remove page files have been compacted
            return page_files_to_remove.count(pf) > 0 //
                // Remove legacy/checkpoint files since we don't do gc on them later
                || pf.getType() == PageFile::Type::Legacy || pf.getType() == PageFile::Type::Checkpoint;
        });
    }

    return {std::move(page_files), std::move(page_files_to_remove), bytes_written};
}

std::tuple<PageFileSet, PageFileSet, WriteBatch::SequenceID, std::optional<PageFile>>
LegacyCompactor::collectPageFilesToCompact(const PageFileSet & page_files, const std::set<PageFileIdAndLevel> & writing_file_ids)
{
    PageStorage::MetaMergingQueue merging_queue;
    for (auto & page_file : page_files)
    {
        auto reader = const_cast<PageFile &>(page_file).createMetaMergingReader();
        // Read one valid WriteBatch
        reader->moveNext();
        merging_queue.push(std::move(reader));
    }

    std::optional<PageFile>               old_checkpoint_file;
    std::optional<WriteBatch::SequenceID> old_checkpoint_sequence;
    PageFileSet                           page_files_to_remove;
    std::tie(old_checkpoint_file, old_checkpoint_sequence, page_files_to_remove) = //
        restoreFromCheckpoints(merging_queue, version_set, info, storage_name, log);

    // The sequence for compacted checkpoint writebatch
    WriteBatch::SequenceID compact_sequence = 0;
    // To see if we stop to collect candidates
    WriteBatch::SequenceID last_sequence = 0;
    if (old_checkpoint_sequence)
    {
        compact_sequence = *old_checkpoint_sequence;
        last_sequence    = *old_checkpoint_sequence;
    }

    PageFileSet page_files_to_compact;
    while (!merging_queue.empty())
    {
        auto reader = merging_queue.top();
        merging_queue.pop();
        // We don't want to do compaction on formal / writing files. If any, just stop collecting `page_files_to_remove`.
        if (reader->belongingPageFile().getType() == PageFile::Type::Formal //
            || writing_file_ids.count(reader->fileIdLevel()) != 0           //
            || (reader->writeBatchSequence() > last_sequence + 1))
        {
            LOG_DEBUG(log,
                      storage_name << " collectPageFilesToCompact stop on " << reader->belongingPageFile().toString() //
                                   << ", sequence: " << reader->writeBatchSequence() << " last sequence: " << DB::toString(last_sequence));
            break;
        }

        // If no checkpoint, we apply all edits.
        // Else restroed from checkpoint, if checkpoint's WriteBatch sequence number is 0, we need to apply
        // all edits after that checkpoint too. If checkpoint's WriteBatch sequence number is not 0, we
        // apply WriteBatch edits only if its WriteBatch sequence is larger than or equeal tocheckpoint.
        if (!old_checkpoint_sequence.has_value() || //
            (old_checkpoint_sequence.has_value()
             && (*old_checkpoint_sequence == 0 || *old_checkpoint_sequence <= reader->writeBatchSequence())))
        {
            // LOG_TRACE(log, storage_name << " collectPageFilesToCompact recovering from " + reader->toString());
            try
            {
                auto edits = reader->getEdits();
                version_set.apply(edits);
                last_sequence    = reader->writeBatchSequence();
                compact_sequence = std::max(compact_sequence, reader->writeBatchSequence());
                info.mergeEdits(edits);
            }
            catch (Exception & e)
            {
                /// Better diagnostics.
                e.addMessage("(PageStorage: " + storage_name + " while applying edit in collectPageFilesToCompact with "
                             + reader->toString() + ")");
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
            LOG_TRACE(log, storage_name << " collectPageFilesToCompact try to compact: " + reader->belongingPageFile().toString());
            page_files_to_compact.emplace(reader->belongingPageFile());
        }
    }
    return {page_files_to_remove, page_files_to_compact, compact_sequence, old_checkpoint_file};
}

WriteBatch LegacyCompactor::prepareCheckpointWriteBatch(const PageStorage::SnapshotPtr snapshot, const WriteBatch::SequenceID wb_sequence)
{
    WriteBatch wb;
    // First Ingest exists pages with normal_id
    auto normal_ids = snapshot->version()->validNormalPageIds();
    for (auto & page_id : normal_ids)
    {
        auto entry = snapshot->version()->findNormalPageEntry(page_id);
        if (unlikely(!entry))
        {
            throw Exception("Normal Page " + DB::toString(page_id) + " not found while prepareCheckpointWriteBatch.",
                            ErrorCodes::LOGICAL_ERROR);
        }
        wb.upsertPage(page_id, //
                      entry->tag,
                      entry->fileIdLevel(),
                      entry->offset,
                      entry->size,
                      entry->checksum,
                      entry->field_offsets);
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

size_t LegacyCompactor::writeToCheckpoint(
    const String & storage_path, const PageFileIdAndLevel & file_id, WriteBatch && wb, FileProviderPtr & file_provider, Poco::Logger * log)
{
    size_t bytes_written   = 0;
    auto   checkpoint_file = PageFile::newPageFile(file_id.first, file_id.second, storage_path, file_provider, PageFile::Type::Temp, log);
    {
        auto checkpoint_writer = checkpoint_file.createWriter(false, true);

        PageEntriesEdit edit;
        bytes_written += checkpoint_writer->write(wb, edit);
    }
    // drop "data" part for checkpoint file.
    bytes_written -= checkpoint_file.setCheckpoint();
    return bytes_written;
}

} // namespace DB
