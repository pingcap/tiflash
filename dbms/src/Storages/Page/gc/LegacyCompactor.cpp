#include <Storages/Page/gc/LegacyCompactor.h>
#include <Storages/Page/gc/restoreFromCheckpoints.h>

namespace DB
{
LegacyCompactor::LegacyCompactor(const PageStorage & storage)
    : storage_name(storage.storage_name),
      storage_path(storage.storage_path),
      config(storage.config),
      log(storage.log),
      page_file_log(storage.page_file_log),
      version_set(config.version_set_config, log)
{
}

std::tuple<PageFileSet, PageFileSet> LegacyCompactor::tryCompact( //
    PageFileSet &&                       page_files,
    const std::set<PageFileIdAndLevel> & writing_file_ids)
{
    // Select PageFiles to compact, all compacted WriteBatch will apply to `this->version_set`
    PageFileSet             page_files_to_compact;
    WriteBatch::SequenceID  checkpoint_sequence = 0;
    std::optional<PageFile> old_checkpoint;
    std::tie(page_files_to_compact, checkpoint_sequence, old_checkpoint) = collectPageFilesToCompact(page_files, writing_file_ids);

    if (page_files_to_compact.size() < config.gc_compact_legacy_min_num)
    {
        LOG_DEBUG(log,
                  storage_name << " LegacyCompactor::tryCompact exit without compaction, candidates size: "
                               << page_files_to_compact.size() //
                               << ", compact_legacy_min_num: " << config.gc_compact_legacy_min_num);
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
        return {std::move(page_files), {}};
    }

    // Build a version_set with snapshot
    auto snapshot = version_set.getSnapshot();
    auto wb       = prepareCheckpointWriteBatch(snapshot, checkpoint_sequence);

    // Use the largest id-level in page_files_to_compact as Checkpoint's file
    const PageFileIdAndLevel largest_id_level = page_files_to_compact.rbegin()->fileIdLevel();
    {
        std::stringstream legacy_ss;
        legacy_ss << "[";
        for (const auto & page_file : page_files_to_compact)
            legacy_ss << "(" << page_file.getFileId() << "," << page_file.getLevel() << "),";
        legacy_ss << "]";
        const String old_checkpoint_str = (old_checkpoint ? old_checkpoint->toString() : "(none)");

        LOG_INFO(log,
                 storage_name << " Compact legacy PageFile " << legacy_ss.str()                                           //
                              << " and old checkpoint: " << old_checkpoint_str                                            //
                              << " into checkpoint PageFile_" << largest_id_level.first << "_" << largest_id_level.second //
                              << " with " << info.toString() << " sequence: " << checkpoint_sequence);
    }

    if (!info.empty())
    {
        writeToCheckpoint(storage_path, largest_id_level, std::move(wb), page_file_log);
    }

    // Clean up compacted PageFiles from `page_files`
    {
        for (auto itr = page_files.begin(); itr != page_files.end();)
        {
            auto & page_file = *itr;
            if (page_files_to_compact.count(page_file) > 0)
            {
                // Remove page files have been compacted
                itr = page_files.erase(itr);
            }
            else if (page_file.getType() == PageFile::Type::Legacy || page_file.getType() == PageFile::Type::Checkpoint)
            {
                // Remove legacy/checkpoint files since we don't do gc on them later
                itr = page_files.erase(itr);
            }
            else
            {
                itr++;
            }
        }

        // We have generate a new checkpoint, old checkpoint can be remove later.
        if (old_checkpoint)
            page_files_to_compact.emplace(*old_checkpoint);
    }

    return {std::move(page_files), std::move(page_files_to_compact)};
}

std::tuple<PageFileSet, WriteBatch::SequenceID, std::optional<PageFile>>
LegacyCompactor::collectPageFilesToCompact(const PageFileSet & page_files, const std::set<PageFileIdAndLevel> & writing_file_ids)
{
    WriteBatch::SequenceID               compact_sequence = 0;
    PageStorage::MetaCompactMergineQueue merging_queue;
    for (auto & page_file : page_files)
    {
        auto reader = const_cast<PageFile &>(page_file).createMetaMergingReader();
        // Read one valid WriteBatch
        reader->moveNext();
        merging_queue.push(std::move(reader));
    }

    std::optional<PageFile>               old_checkpoint_file;
    std::optional<WriteBatch::SequenceID> checkpoint_wb_sequence;
    PageFileSet                           page_files_to_compact;
    std::tie(old_checkpoint_file, checkpoint_wb_sequence, page_files_to_compact) = //
        restoreFromCheckpoints(merging_queue, version_set, info, storage_name, log);

    WriteBatch::SequenceID last_sequence = (checkpoint_wb_sequence.has_value() ? *checkpoint_wb_sequence : 0);
    while (!merging_queue.empty())
    {
        auto reader = merging_queue.top();
        merging_queue.pop();
        // We don't want to do compaction on formal / writing files. If any, just stop collecting `page_files_to_compact`.
        if (reader->belongingPageFile().getType() == PageFile::Type::Formal //
            || writing_file_ids.count(reader->fileIdLevel()) != 0           //
            || (reader->writeBatchSequence() > last_sequence + 1))
        {
            LOG_TRACE(log,
                      storage_name << " collectPageFilesToCompact stop on " << reader->belongingPageFile().toString() //
                                   << ", sequence: " << reader->writeBatchSequence() << " last sequence: " << DB::toString(last_sequence));
            break;
        }

        // If no checkpoint, we apply all edits.
        // Else restroed from checkpoint, if checkpoint's WriteBatch sequence number is 0, we need to apply
        // all edits after that checkpoint too. If checkpoint's WriteBatch sequence number is not 0, we
        // apply WriteBatch edits only if its WriteBatch sequence is larger than checkpoint.
        if (!checkpoint_wb_sequence.has_value() || //
            (checkpoint_wb_sequence.has_value()
             && (*checkpoint_wb_sequence == 0 || *checkpoint_wb_sequence < reader->writeBatchSequence())))
        {
            LOG_TRACE(log, storage_name << " collectPageFilesToCompact recovering from " + reader->toString());
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
                e.addMessage("(PageStorage: " + storage_name + " while applying edit in gcCompactLegacy with " + reader->toString() + ")");
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
    return {page_files_to_compact, compact_sequence, old_checkpoint_file};
}

WriteBatch LegacyCompactor::prepareCheckpointWriteBatch(const PageStorage::SnapshotPtr snapshot, const WriteBatch::SequenceID wb_sequence)
{
    WriteBatch wb;
    // First Ingest exists pages with normal_id
    auto normal_ids = snapshot->version()->validNormalPageIds();
    for (auto & page_id : normal_ids)
    {
        auto entry = snapshot->version()->findNormalPageEntry(page_id);
        wb.upsertPage(page_id, entry->tag, nullptr, entry->size, entry->field_offsets);
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

void LegacyCompactor::writeToCheckpoint(const String &             storage_path,
                                        const PageFileIdAndLevel & file_id,
                                        WriteBatch &&              wb,
                                        Poco::Logger *             log)
{
    auto checkpoint_file = PageFile::newPageFile(file_id.first, file_id.second, storage_path, PageFile::Type::Temp, log);
    {
        auto checkpoint_writer = checkpoint_file.createWriter(false);

        PageEntriesEdit edit;
        checkpoint_writer->write(wb, edit);
    }
    checkpoint_file.setCheckpoint();
}

} // namespace DB
