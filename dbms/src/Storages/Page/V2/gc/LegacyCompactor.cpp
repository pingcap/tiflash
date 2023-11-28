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

#include <Storages/Page/V2/gc/LegacyCompactor.h>
#include <Storages/Page/V2/gc/restoreFromCheckpoints.h>
#include <Storages/PathPool.h>

namespace DB::PS::V2
{
LegacyCompactor::LegacyCompactor(
    const PageStorage & storage,
    const WriteLimiterPtr & write_limiter_,
    const ReadLimiterPtr & read_limiter_)
    : storage_name(storage.storage_name)
    , delegator(storage.delegator)
    , file_provider(storage.getFileProvider())
    , config(storage.config)
    , log(storage.log)
    , page_file_log(storage.page_file_log)
    , version_set(storage.storage_name + ".legacy_compactor", config.version_set_config, log)
    , write_limiter(write_limiter_)
    , read_limiter(read_limiter_)
{}

std::tuple<PageFileSet, PageFileSet, size_t> LegacyCompactor::tryCompact(
    PageFileSet && page_files,
    const WritingFilesSnapshot & writing_files)
{
    // Select PageFiles to compact, all compacted WriteBatch will apply to `this->version_set`
    PageFileSet page_files_to_remove;
    PageFileSet page_files_to_compact;
    WriteBatch::SequenceID checkpoint_sequence = 0;
    std::optional<PageFile> old_checkpoint;
    std::tie(page_files_to_remove, page_files_to_compact, checkpoint_sequence, old_checkpoint)
        = collectPageFilesToCompact(page_files, writing_files);

    PageFileIdAndLevel min_writing_file_id_level = writing_files.minFileIDLevel();

    if (page_files_to_compact.size() < config.gc_min_legacy_num)
    {
        // Nothing to compact
        LOG_DEBUG(
            log,
            "{} LegacyCompactor::tryCompact exit without compaction, candidates size: {}, compact_legacy_min_num: {}",
            storage_name,
            page_files_to_compact.size(),
            config.gc_min_legacy_num);
        removePageFilesIf(page_files, [&min_writing_file_id_level](const PageFile & pf) -> bool {
            return
                // Remove page files that maybe writing to
                pf.fileIdLevel() >= min_writing_file_id_level
                // Remove legacy/checkpoint files since we don't do gc on them later
                || pf.getType() == PageFile::Type::Legacy || pf.getType() == PageFile::Type::Checkpoint;
        });
        return {std::move(page_files), {}, 0};
    }

    // Use the largest id-level in page_files_to_compact as Checkpoint's file
    const PageFileIdAndLevel checkpoint_id = page_files_to_compact.rbegin()->fileIdLevel();

    // We only store the checkpoint file to `defaultPath` for convenience. If we store the checkpoint
    // to multi disk one day, don't forget to check existence for multi disks deployment.
    const String storage_path = delegator->defaultPath();
    if (PageFile::isPageFileExist(
            checkpoint_id,
            storage_path,
            file_provider,
            PageFile::Type::Checkpoint,
            page_file_log))
    {
        LOG_WARNING(
            log,
            "{} LegacyCompactor::tryCompact to checkpoint PageFile_{}_{} is done before.",
            storage_name,
            checkpoint_id.first,
            checkpoint_id.second);
        removePageFilesIf(page_files, [&min_writing_file_id_level](const PageFile & pf) -> bool {
            return
                // Remove page files that maybe writing to
                pf.fileIdLevel() >= min_writing_file_id_level
                // Remove legacy/checkpoint files since we don't do gc on them later
                || pf.getType() == PageFile::Type::Legacy || pf.getType() == PageFile::Type::Checkpoint;
        });
        return {std::move(page_files), {}, 0};
    }

    // Build a version_set with snapshot
    auto snapshot = version_set.getSnapshot(/*tracing_id*/ "", nullptr);
    auto wb = prepareCheckpointWriteBatch(snapshot, checkpoint_sequence);

    {
        std::stringstream legacy_ss;
        legacy_ss << "[";
        for (const auto & page_file : page_files_to_compact)
            legacy_ss << "(" << page_file.getFileId() << "," << page_file.getLevel() << "),";
        legacy_ss << "]";
        const String old_checkpoint_str = (old_checkpoint ? old_checkpoint->toString() : "(none)");

        LOG_INFO(
            log,
            "{} Compact legacy PageFile {} and old checkpoint: {} into checkpoint PageFile_{}_{} with {} sequence: {}",
            storage_name,
            legacy_ss.str(),
            old_checkpoint_str,
            checkpoint_id.first,
            checkpoint_id.second,
            info.toString(),
            checkpoint_sequence);
    }

    size_t bytes_written = 0;
    if (!info.empty())
    {
        bytes_written = writeToCheckpoint(
            storage_path,
            checkpoint_id,
            std::move(wb),
            file_provider,
            page_file_log,
            write_limiter);
        // 1. Don't need to insert location since Checkpoint PageFile won't be read except using listAllPageFiles in `PageStorage::restore`
        // 2. Also, `checkpoint_id` is the same as the largest page file compacted,
        //    so insert the checkpoint file's location here will overwrite the old page file's location and may incur error when deploy on multi disk environment
        // 3. And we always store checkpoint file on `delegator`'s default path, so we can just remove it from the default path when removing it
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

        removePageFilesIf(page_files, [&page_files_to_remove, &min_writing_file_id_level](const PageFile & pf) -> bool {
            return //
                // Remove page files have been compacted
                page_files_to_remove.count(pf) > 0
                // Remove page files that maybe writing to
                || pf.fileIdLevel() >= min_writing_file_id_level
                // Remove legacy/checkpoint files since we don't do gc on them later
                || pf.getType() == PageFile::Type::Legacy || pf.getType() == PageFile::Type::Checkpoint;
        });
    }

    return {std::move(page_files), std::move(page_files_to_remove), bytes_written};
}

std::tuple<PageFileSet, PageFileSet, WriteBatch::SequenceID, std::optional<PageFile>> LegacyCompactor::
    collectPageFilesToCompact(const PageFileSet & page_files, const WritingFilesSnapshot & writing_files)
{
    PageStorage::MetaMergingQueue merging_queue;
    for (const auto & page_file : page_files)
    {
        PageFile::MetaMergingReaderPtr reader;
        if (auto iter = writing_files.find(page_file.fileIdLevel()); iter != writing_files.end())
        {
            // create reader with max meta reading offset
            reader = PageFile::MetaMergingReader::createFrom(
                const_cast<PageFile &>(page_file),
                iter->second.meta_offset,
                read_limiter,
                /*background*/ true);
        }
        else
        {
            reader = PageFile::MetaMergingReader::createFrom(
                const_cast<PageFile &>(page_file),
                read_limiter,
                /*background*/ true);
        }
        if (reader->hasNext())
        {
            // Read one valid WriteBatch
            reader->moveNext();
            merging_queue.push(std::move(reader));
        }
        // else the file doesn't contain any valid meta, just skip it. Or the compaction will be
        // stopped by a writable file that contains no valid meta.
    }

    std::optional<PageFile> old_checkpoint_file;
    std::optional<WriteBatch::SequenceID> old_checkpoint_sequence;
    PageFileSet page_files_to_remove;
    std::tie(old_checkpoint_file, old_checkpoint_sequence, page_files_to_remove) = //
        restoreFromCheckpoints(merging_queue, version_set, info, storage_name, log);

    // The sequence for compacted checkpoint writebatch
    WriteBatch::SequenceID compact_sequence = 0;
    // To see if we stop to collect candidates
    WriteBatch::SequenceID last_sequence = 0;
    if (old_checkpoint_sequence)
    {
        compact_sequence = *old_checkpoint_sequence;
        last_sequence = *old_checkpoint_sequence;
    }

    const auto gc_safe_sequence = writing_files.minPersistedSequence();
    PageFileSet page_files_to_compact;
    while (!merging_queue.empty())
    {
        auto reader = merging_queue.top();
        merging_queue.pop();
        // We don't want to do compaction on formal / writing files, and can not exceed the
        // last persisted sequence, or some write batches may be lost.
        // If any, just stop collecting `page_files_to_remove`.
        const auto reader_wb_seq = reader->writeBatchSequence();
        if (reader->belongingPageFile().getType() == PageFile::Type::Formal //
            || reader_wb_seq >= gc_safe_sequence //
            || writing_files.contains(reader->fileIdLevel()))
        {
            LOG_DEBUG(
                log,
                "{} collectPageFilesToCompact stop on {}, sequence: {} last sequence: {} gc safe squence: {}",
                storage_name,
                reader->belongingPageFile().toString(),
                reader_wb_seq,
                last_sequence,
                gc_safe_sequence);
            break;
        }

        // If no checkpoint, we apply all edits.
        // Else restroed from checkpoint, if checkpoint's WriteBatch sequence number is 0, we need to apply
        // all edits after that checkpoint too. If checkpoint's WriteBatch sequence number is not 0, we
        // apply WriteBatch edits only if its WriteBatch sequence is larger than or equeal tocheckpoint.
        if (!old_checkpoint_sequence.has_value() || //
            (old_checkpoint_sequence.has_value() && //
             (*old_checkpoint_sequence == 0 || *old_checkpoint_sequence <= reader_wb_seq)))
        {
            if (unlikely(reader_wb_seq > last_sequence + 1))
            {
                // There would be a case for lefting hole on the WAL (combined from multiple meta files).
                // Thread 1 tries to write a WriteBatch with seq=999 (called wb1), thread 2 try to write a WriteBatch
                // with seq=1000 (called wb2). However, wb2 is committed to disk first. And the process crashes in the
                // middle of writing wb1 (or even not writing down wb1 at all). After recovering from disk, the wb1 is
                // throw away while wb2 is left.
                // Then there would be a hole in the WAL. We need to automatically recover from crashes in the middle
                // from writing, so just skip the hole and continue the compaction.
                // FIXME: rethink the multi-threads writing support.
                LOG_WARNING(
                    log,
                    "{} collectPageFilesToCompact skip non-continuous sequence from {} to {}, {{{}}}",
                    storage_name,
                    last_sequence,
                    reader_wb_seq,
                    reader->toString());
            }

            try
            {
                auto edits = reader->getEdits();
                version_set.apply(edits);
                last_sequence = reader_wb_seq;
                compact_sequence = std::max(compact_sequence, reader_wb_seq);
                info.mergeEdits(edits);
            }
            catch (Exception & e)
            {
                /// Better diagnostics.
                e.addMessage(
                    "(PageStorage: " + storage_name + " while applying edit in collectPageFilesToCompact with "
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
            LOG_TRACE(
                log,
                "{} collectPageFilesToCompact try to compact: {}",
                storage_name,
                reader->belongingPageFile().toString());
            page_files_to_compact.emplace(reader->belongingPageFile());
        }
    }
    return {page_files_to_remove, page_files_to_compact, compact_sequence, old_checkpoint_file};
}

WriteBatch LegacyCompactor::prepareCheckpointWriteBatch(
    const PageStorage::ConcreteSnapshotPtr & snapshot,
    const WriteBatch::SequenceID wb_sequence)
{
    // namespace in v2 is useless
    WriteBatch wb{MAX_NAMESPACE_ID};
    // First Ingest exists pages with normal_id
    auto normal_ids = snapshot->version()->validNormalPageIds();
    for (const auto & page_id : normal_ids)
    {
        auto entry = snapshot->version()->findNormalPageEntry(page_id);
        if (unlikely(!entry))
        {
            throw Exception(
                "Normal Page " + DB::toString(page_id) + " not found while prepareCheckpointWriteBatch.",
                ErrorCodes::LOGICAL_ERROR);
        }
        wb.upsertPage(
            page_id, //
            entry->tag,
            entry->fileIdLevel(),
            entry->offset,
            entry->size,
            entry->checksum,
            entry->field_offsets);
    }

    // After ingesting normal_pages, we will ref them manually to ensure the ref-count is correct.
    auto ref_ids = snapshot->version()->validPageIds();
    for (const auto & page_id : ref_ids)
    {
        auto ori_id = snapshot->version()->isRefId(page_id).second;
        wb.putRefPage(page_id, ori_id);
    }

    wb.setSequence(wb_sequence);
    return wb;
}

size_t LegacyCompactor::writeToCheckpoint(
    const String & storage_path,
    const PageFileIdAndLevel & file_id,
    WriteBatch && wb,
    FileProviderPtr & file_provider,
    LoggerPtr log,
    const WriteLimiterPtr & write_limiter)
{
    size_t bytes_written = 0;
    auto checkpoint_file
        = PageFile::newPageFile(file_id.first, file_id.second, storage_path, file_provider, PageFile::Type::Temp, log);
    {
        auto checkpoint_writer = checkpoint_file.createWriter(false, true);

        PageEntriesEdit edit;
        bytes_written += checkpoint_writer->write(wb, edit, write_limiter, /*background*/ true);
    }
    // drop "data" part for checkpoint file.
    bytes_written -= checkpoint_file.setCheckpoint();
    return bytes_written;
}

} // namespace DB::PS::V2
