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

#include <Common/FailPoint.h>
#include <IO/Buffer/ReadBufferFromMemory.h>
#include <Storages/Page/V2/gc/DataCompactor.h>
#include <Storages/PathPool.h>

#ifndef NDEBUG
#include <Storages/Page/V2/mock/MockUtils.h>
#endif

namespace DB
{
namespace FailPoints
{
extern const char force_set_page_data_compact_batch[];
} // namespace FailPoints

namespace PS::V2
{
template <typename SnapshotPtr>
DataCompactor<SnapshotPtr>::DataCompactor(
    const PageStorage & storage,
    PageStorageConfig gc_config,
    const WriteLimiterPtr & write_limiter_,
    const ReadLimiterPtr & read_limiter_)
    : storage_name(storage.storage_name)
    , delegator(storage.delegator)
    , file_provider(storage.getFileProvider())
    , config(std::move(gc_config))
    , log(storage.log)
    , page_file_log(storage.page_file_log)
    , write_limiter(write_limiter_)
    , read_limiter(read_limiter_)
{}

// There could be a chance that one big PageFile contains many valid bytes, but we still need to rewrite it
// with another PageFiles to a newer PageFile (so that they have the higher `compact_seq`) to make
// GC move forward.
static constexpr size_t GC_NUM_CANDIDATE_SIZE_LOWER_BOUND = 2;

template <typename SnapshotPtr>
std::tuple<typename DataCompactor<SnapshotPtr>::Result, PageEntriesEdit> //
DataCompactor<SnapshotPtr>::tryMigrate( //
    const PageFileSet & page_files,
    SnapshotPtr && snapshot,
    const WritingFilesSnapshot & writing_files)
{
    ValidPages valid_pages = collectValidPagesInPageFile(snapshot);
    Result result;
    const auto candidates = selectCandidateFiles(page_files, valid_pages, writing_files);

    result.candidate_size = candidates.compact_candidates.size();
    result.bytes_migrate = candidates.total_valid_bytes;
    result.num_migrate_pages = candidates.num_migrate_pages;
    result.do_compaction = result.candidate_size >= config.gc_min_files
        || (result.candidate_size >= GC_NUM_CANDIDATE_SIZE_LOWER_BOUND && result.bytes_migrate >= config.gc_min_bytes);

    // Scan over all `candidates` and do migrate.
    PageEntriesEdit migrate_entries_edit;
    if (result.do_compaction)
    {
        std::tie(migrate_entries_edit, result.bytes_written)
            = migratePages(snapshot, valid_pages, candidates, result.num_migrate_pages);
    }
    else
    {
        LOG_DEBUG(
            log,
            "{} DataCompactor::tryMigrate exit without compaction [candidates size={}] [total byte size={}], "
            "[files without valid page={}] Config{{ {} }}", //
            storage_name,
            result.candidate_size,
            result.bytes_migrate,
            candidates.files_without_valid_pages.size(),
            config.toDebugStringV2());
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
            throw Exception(
                "PageStorage GC: Normal Page " + DB::toString(page_id) + " not found.",
                ErrorCodes::LOGICAL_ERROR);
        }
        auto && [valid_size, valid_page_ids_in_file] = valid_pages[page_entry->fileIdLevel()];
        valid_size += page_entry->size;
        valid_page_ids_in_file.emplace(page_id);
    }
    return valid_pages;
}

template <typename SnapshotPtr>
typename DataCompactor<SnapshotPtr>::CompactCandidates //
DataCompactor<SnapshotPtr>::selectCandidateFiles( // keep readable indent
    const PageFileSet & page_files,
    const ValidPages & files_valid_pages,
    const WritingFilesSnapshot & writing_files) const
{
#ifdef PAGE_STORAGE_UTIL_DEBUGGGING
    LOG_TRACE(log, "{} input size of candidates: {}", storage_name, page_files.size());
#endif

    /**
     * Assume that there is few PageFile with lower <FileID, level> (and lower sequence number) but high valid rate,
     * following by multiple PageFiles with higher <FileID, level> (and higher sequence number) but lower valid rate,
     * we want to compact all them with a higher sequence to move GC forward. Or compacting one by one will greatly
     * increase the write amplification.
     *
     * TODO: upper level (DeltaTree engine) may leave some pages undeleted after recovering from a crash, we should consider
     * * how to clean up those pages after recovering from a crash in the upper level
     * * new design for better handling for this situation
     */

    static constexpr double HIGH_RATE_THRESHOLD = 0.65;

    // Those files with valid pages, we need to migrate those pages into a new PageFile
    PageFileSet candidates;
    // Those files without valid pages, we need to log them down later
    PageFileSet files_without_valid_pages;
    // Those files have high vaild rate with big size, we need update thier sid.
    PageFileSet hardlink_candidates;

    size_t candidate_total_size = 0;
    size_t num_migrate_pages = 0;
    size_t num_candidates_with_high_rate = 0;
    size_t candidate_total_size_with_lower_rate = 0;
    for (const auto & page_file : page_files)
    {
        if (unlikely(page_file.getType() != PageFile::Type::Formal))
        {
            throw Exception(
                "Try to pick " + page_file.toString() + " as gc candidate, path: " + page_file.folderPath(),
                ErrorCodes::LOGICAL_ERROR);
        }

        const auto file_size = page_file.getDataFileSize();
        UInt64 valid_size = 0;
        float valid_rate = 0.0f;
        size_t valid_page_count = 0;

        if (auto it = files_valid_pages.find(page_file.fileIdLevel()); it != files_valid_pages.end())
        {
            valid_size = it->second.first;
            valid_rate = static_cast<float>(valid_size) / file_size;
            valid_page_count = it->second.second.size();
        }

        // Don't gc writing page file.
        bool is_candidate = !writing_files.contains(page_file.fileIdLevel())
            && (valid_rate < config.gc_max_valid_rate //
                || file_size < config.file_small_size //
                || config.gc_max_valid_rate >= 1.0 // all page file will be picked
            );
#ifdef PAGE_STORAGE_UTIL_DEBUGGGING
        LOG_TRACE(
            log,
            "{} {} [valid rate={:.2f}] [file size={}]",
            storage_name,
            page_file.toString(),
            valid_rate,
            file_size);
#endif
        if (!is_candidate)
        {
            continue;
        }

        if (valid_size == 0)
        {
            // Do not collect page_files without valid pages, since it can not set the compact sequence to a bigger one
            // and move GC forward. Instead, it cause higher write amplification while rewriting one PageFile to a
            // new one.
            files_without_valid_pages.emplace(page_file);
            continue;
        }

        if (likely(config.gc_force_hardlink_rate <= 1.0) && valid_rate > config.gc_force_hardlink_rate
            && file_size > config.file_max_size)
        {
            hardlink_candidates.emplace(page_file);
            continue;
        }

        candidates.emplace(page_file);
        num_migrate_pages += valid_page_count;
        candidate_total_size += valid_size;
        if (valid_rate < HIGH_RATE_THRESHOLD)
        {
            candidate_total_size_with_lower_rate += valid_size;
        }
        else
        {
            num_candidates_with_high_rate++;
            candidate_total_size_with_lower_rate += 0;
            LOG_INFO(
                log,
                "{} collect {} with high valid rate as candidates [valid rate={:.2f}] [file size={}]",
                storage_name,
                page_file.toString(),
                valid_rate,
                file_size);
        }

        bool stop = false;
        if (num_candidates_with_high_rate == 0)
        {
            // If all candidates are in lower valid rate, we stop collecting the candidates until we can fully fill
            // a new PageFile.
            stop = candidate_total_size >= config.file_max_size;
        }
        else
        {
            // If there are candidates with high valid rate, we want to compact as many following lower rate as possible
            // to move forward the GC and reduce the write amplification.
            stop = candidate_total_size_with_lower_rate >= config.file_max_size;
        }
        // Note: should always add check for `GC_NUM_CANDIDATE_SIZE_LOWER_BOUND`
        if (stop && candidates.size() >= GC_NUM_CANDIDATE_SIZE_LOWER_BOUND)
        {
            break;
        }
    }

    return CompactCandidates{
        std::move(candidates),
        std::move(files_without_valid_pages),
        std::move(hardlink_candidates),
        candidate_total_size,
        num_migrate_pages};
}

template <typename SnapshotPtr>
bool DataCompactor<SnapshotPtr>::isPageFileExistInAllPath(const PageFileIdAndLevel & file_id_and_level) const
{
    const auto paths = delegator->listPaths();
    for (const auto & pf_parent_path : paths)
    {
        if (PageFile::isPageFileExist(
                file_id_and_level,
                pf_parent_path,
                file_provider,
                PageFile::Type::Formal,
                page_file_log)
            || PageFile::isPageFileExist(
                file_id_and_level,
                pf_parent_path,
                file_provider,
                PageFile::Type::Legacy,
                page_file_log))
        {
            return true;
        }
    }
    return false;
}

template <typename SnapshotPtr>
std::tuple<PageEntriesEdit, size_t> //
DataCompactor<SnapshotPtr>::migratePages( //
    const SnapshotPtr & snapshot,
    const ValidPages & files_valid_pages,
    const CompactCandidates & candidates,
    const size_t migrate_page_count) const
{
    if (candidates.compact_candidates.empty())
        return {PageEntriesEdit{}, 0};

    // merge `compact_candidates` to PageFile which PageId = max of all `compact_candidates` and level = level + 1
    auto [largest_file_id, level] = candidates.compact_candidates.rbegin()->fileIdLevel();
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

    // We need to check existence for multi disks deployment, or we may generate the same file id
    // among different disks, some of them will be ignored by the PageFileSet because of duplicated
    // file id while restoring from disk
    if (isPageFileExistInAllPath(migrate_file_id))
    {
        LOG_INFO(
            log,
            "{} GC migration to PageFile_{}_{} is done before.",
            storage_name,
            migrate_file_id.first,
            migrate_file_id.second);
        return {PageEntriesEdit{}, 0};
    }
    // else the PageFile is not exists in all paths, continue migration.


    // Choose one path for creating a tmp PageFile for migration
    const String pf_parent_path = delegator->choosePath(migrate_file_id);
    PageFile gc_file = PageFile::newPageFile(
        migrate_file_id.first,
        migrate_file_id.second,
        pf_parent_path,
        file_provider,
        PageFile::Type::Temp,
        page_file_log);
    LOG_INFO(
        log,
        "{} GC decide to migrate {} files, containing {} pages to PageFile_{}_{}, path {}",
        storage_name,
        candidates.compact_candidates.size(),
        migrate_page_count,
        gc_file.getFileId(),
        gc_file.getLevel(),
        pf_parent_path);

    PageEntriesEdit gc_file_edit;
    size_t bytes_written = 0;
    MigrateInfos migrate_infos;
    {
        for (auto & page_file : candidates.files_without_valid_pages)
        {
            if (auto it = files_valid_pages.find(page_file.fileIdLevel()); it == files_valid_pages.end())
            {
                // This file does not contain any valid page.
                migrate_infos.emplace_back(page_file.fileIdLevel(), 0);
            }
            else
            {
                throw Exception(
                    "The page file " + page_file.toString()
                    + " in files_without_valid_pages actually contains unexpected pages");
            }
        }
    }
    WriteBatch::SequenceID compact_seq = 0;
    {
        PageStorage::OpenReadFiles data_readers;
        // To keep the order of all PageFiles' meta, `compact_seq` should be maximum of all candidates.
        for (auto & page_file : candidates.compact_candidates)
        {
            if (page_file.getType() != PageFile::Type::Formal)
            {
                throw Exception(
                    "Try to migrate pages from invalid type PageFile: " + page_file.toString()
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
            auto meta_reader = PageFile::MetaMergingReader::createFrom(
                const_cast<PageFile &>(page_file),
                read_limiter,
                /*background*/ true);
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
        std::tie(gc_file_edit, bytes_written) = mergeValidPages(
            std::move(data_readers),
            files_valid_pages,
            snapshot,
            compact_seq,
            gc_file,
            migrate_infos);
    }

    logMigrationDetails(migrate_infos, migrate_file_id);

    for (auto & page_file : candidates.hardlink_candidates)
    {
        const PageFileIdAndLevel hardlink_file_id{page_file.getFileId(), page_file.getLevel() + 1};
        if (isPageFileExistInAllPath(hardlink_file_id))
        {
            LOG_INFO(
                log,
                "{} GC link to PageFile_{}_{} is done before.",
                storage_name,
                hardlink_file_id.first,
                hardlink_file_id.second);
            continue;
        }

        PageFile hard_link_file = PageFile::newPageFile(
            page_file.getFileId(),
            page_file.getLevel() + 1,
            page_file.parentPath(),
            file_provider,
            PageFile::Type::Temp,
            page_file_log);

        LOG_INFO(
            log,
            "{} GC decide to link PageFile_{}_{} to PageFile_{}_{}",
            storage_name,
            hard_link_file.getFileId(),
            hard_link_file.getLevel(),
            page_file.getFileId(),
            page_file.getLevel());

        PageEntriesEdit edit;
        if (!hard_link_file.linkFrom(const_cast<PageFile &>(page_file), compact_seq, edit))
        {
            hard_link_file.destroy();
            continue;
        }

        hard_link_file.setFormal();
        gc_file_edit.concate(edit);
        // After the hard link file is created, the original file will be removed later and subtract its data size from the delegator.
        // So we need to increase the data size for the hard link file for correctness on the disk data usage in a longer time dimension.
        delegator->addPageFileUsedSize(
            hard_link_file.fileIdLevel(),
            hard_link_file.getDataFileSize(),
            hard_link_file.parentPath(),
            /*need_insert_location*/ true);
    }

    if (gc_file_edit.empty())
    {
        LOG_INFO(
            log,
            "{} No valid pages, deleting PageFile_{}_{}",
            storage_name,
            migrate_file_id.first,
            migrate_file_id.second);
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
        LOG_INFO(
            log,
            "{} GC have migrated {} Pages with sequence {} to PageFile_{}_{}",
            storage_name,
            num_migrate_pages,
            compact_seq,
            migrate_file_id.first,
            migrate_file_id.second);
    }
    return {std::move(gc_file_edit), bytes_written};
}

template <typename SnapshotPtr>
std::tuple<PageEntriesEdit, size_t> //
DataCompactor<SnapshotPtr>::mergeValidPages( //
    PageStorage::OpenReadFiles && data_readers,
    const ValidPages & files_valid_pages,
    const SnapshotPtr & snapshot,
    const WriteBatch::SequenceID compact_sequence,
    PageFile & gc_file,
    MigrateInfos & migrate_infos) const
{
    PageEntriesEdit gc_file_edit;
    const auto gc_file_id = gc_file.fileIdLevel();
    // No need to sync after each write. Do sync before closing is enough.
    auto gc_file_writer = gc_file.createWriter(/* sync_on_write= */ false, true);
    size_t bytes_written = 0;

    // When all data of one PageFile is obsoleted, we just remove its data but leave its meta part on disk.
    // And all migrated data will be written as multiple WriteBatches(one WriteBatch for one candidate
    // PageFile) with same sequence. To keep the order of all PageFiles' meta, the sequence of WriteBatch
    // should be maximum of all candidates' WriteBatches. No matter we merge valid page(s) from that
    // WriteBatch or not.
    // Next time we recover pages' meta from disk, recover checkpoint first, then merge all pages' meta
    // according to the tuple <Sequence, PageFileId, PageFileLevel> is OK.

    for (const auto & files_valid_page : files_valid_pages)
    {
        const auto & file_id_level = files_valid_page.first;
        const auto & [_valid_bytes, valid_page_ids_in_file] = files_valid_page.second;
        (void)_valid_bytes;

        auto reader_iter = data_readers.find(file_id_level);
        if (reader_iter == data_readers.end())
            continue;

        auto page_id_and_entries = collectValidEntries(valid_page_ids_in_file, snapshot);
        if (!page_id_and_entries.empty())
        {
            auto & data_reader = reader_iter->second;
            // A helper to read entries from `data_reader` and migrate the pages to `gc_file_writer`.
            // The changes will be recorded by `gc_file_edit` and the bytes written will be return.
            auto migrate_entries = [compact_sequence, &data_reader, &gc_file_id, &gc_file_writer, &gc_file_edit, this](
                                       PageIdAndEntries & entries) -> size_t {
                const PageMap pages = data_reader->read(entries, read_limiter, /*background*/ true);
                // namespace id in v2 is useless
                WriteBatch wb{MAX_NAMESPACE_ID};
                wb.setSequence(compact_sequence);
                for (const auto & [page_id, entry] : entries)
                {
                    // Upsert page to gc_file
                    const auto page = pages.find(page_id)->second;
                    wb.upsertPage(
                        page_id,
                        entry.tag,
                        gc_file_id,
                        std::make_shared<ReadBufferFromMemory>(page.data.begin(), page.data.size()),
                        page.data.size(),
                        entry.field_offsets);
                }

                return gc_file_writer->write(wb, gc_file_edit, write_limiter, true);
            };

#ifndef NDEBUG
            size_t MAX_BATCH_PER_MOVEMENT = 1000; // NOLINT(readability-identifier-naming)
            fiu_do_on(FailPoints::force_set_page_data_compact_batch, { MAX_BATCH_PER_MOVEMENT = 3; });
#else
            constexpr size_t MAX_BATCH_PER_MOVEMENT = 1000; // NOLINT(readability-identifier-naming)
#endif
            if (page_id_and_entries.size() <= MAX_BATCH_PER_MOVEMENT)
            {
                bytes_written += migrate_entries(page_id_and_entries);
            }
            else
            {
                // There could be one candidate contains many pages, split the `page_id_and_entries`
                // and try to control the memory peak.
#ifndef NDEBUG
                size_t entries_migrated = 0;
#endif
                PageIdAndEntries entries_batch;
                entries_batch.reserve(MAX_BATCH_PER_MOVEMENT);
                for (size_t start_idx = 0; start_idx < page_id_and_entries.size(); /**/)
                {
                    size_t end_idx = std::min(start_idx + MAX_BATCH_PER_MOVEMENT, page_id_and_entries.size());
                    entries_batch.clear();
                    entries_batch.assign(
                        page_id_and_entries.begin() + start_idx,
                        page_id_and_entries.begin() + end_idx);
#ifndef NDEBUG
                    entries_migrated += entries_batch.size();
#endif
                    const auto curr_bytes_written = migrate_entries(entries_batch);
                    LOG_DEBUG(
                        log,
                        "{} DataCompactor::mergeValidPages run with a samller batch [start_idx={}] [end_idx={}] "
                        "[curr_bytes_written={}]",
                        storage_name,
                        start_idx,
                        end_idx,
                        curr_bytes_written);
                    bytes_written += curr_bytes_written;

                    start_idx = end_idx;
                }
#ifndef NDEBUG
                if (entries_migrated != page_id_and_entries.size())
                {
                    throw Exception(
                        "Expect migrate " + DB::toString(page_id_and_entries.size()) + " but only migrate "
                        + DB::toString(entries_migrated) + " pages!");
                }
#endif
            }
        }
        migrate_infos.emplace_back(file_id_level, page_id_and_entries.size());
    }

    // Free gc_file_writer and sync
    delegator->addPageFileUsedSize(gc_file_id, bytes_written, gc_file.parentPath(), /*need_insert_location*/ true);
    return {std::move(gc_file_edit), bytes_written};
}

template <typename SnapshotPtr>
PageIdAndEntries DataCompactor<SnapshotPtr>::collectValidEntries(
    const PageIdSet & valid_pages,
    const SnapshotPtr & snapshot)
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
void DataCompactor<SnapshotPtr>::logMigrationDetails(
    const MigrateInfos & infos,
    const PageFileIdAndLevel & migrate_file_id) const
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
    LOG_DEBUG(
        log,
        "{} Migrate pages to PageFile_{}_{}, migrate: {}, remove: {}, Config{{ {} }}", //
        storage_name,
        migrate_file_id.first,
        migrate_file_id.second,
        migrate_stream.str(),
        remove_stream.str(),
        config.toDebugStringV2());
}


template class DataCompactor<PageStorage::ConcreteSnapshotPtr>;
#ifndef NDEBUG
template class DataCompactor<DB::PS::V2::tests::MockSnapshotPtr>;
#endif
} // namespace PS::V2
} // namespace DB
