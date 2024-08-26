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

#include <Common/Exception.h>
#include <Storages/Page/V3/PageDefines.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/Page/V3/WAL/WALReader.h>
#include <Storages/Page/V3/WAL/serialize.h>
#include <Storages/Page/V3/WALStore.h>
#include <common/logger_useful.h>

#include <memory>
#include <optional>

namespace DB
{
namespace ErrorCodes
{
extern const int PS_DIR_APPLY_INVALID_STATUS;
} // namespace ErrorCodes
namespace PS::V3
{
template <typename Trait>
typename PageDirectoryFactory<Trait>::PageDirectoryPtr PageDirectoryFactory<Trait>::create(
    const String & storage_name,
    FileProviderPtr & file_provider,
    PSDiskDelegatorPtr & delegator,
    const WALConfig & config)
{
    auto [wal, reader] = WALStore::create(storage_name, file_provider, delegator, config);
    return createFromReader(storage_name, reader, std::move(wal));
}

template <typename Trait>
typename PageDirectoryFactory<Trait>::PageDirectoryPtr PageDirectoryFactory<Trait>::createFromReader(
    const String & storage_name,
    WALStoreReaderPtr reader,
    WALStorePtr wal)
{
    PageDirectoryPtr dir = std::make_unique<typename Trait::PageDirectory>(storage_name, std::move(wal));
    loadFromDisk(dir, std::move(reader));

    // Reset the `sequence` to the maximum of persisted.
    dir->sequence = max_applied_ver.sequence;

    // After restoring from the disk, we need cleanup all invalid entries in memory, or it will
    // try to run GC again on some entries that are already marked as invalid in BlobStore.
    // It's no need to remove the expired entries in BlobStore, so skip filling removed_entries to improve performance.
    dir->gcInMemEntries({.need_removed_entries = false});
    LOG_INFO(
        DB::Logger::get(storage_name),
        "PageDirectory restored, max_page_id={} max_applied_ver={}",
        dir->getMaxIdAfterRestart(),
        dir->sequence);

    restoreBlobStats(dir);

    return dir;
}

template <typename Trait>
typename PageDirectoryFactory<Trait>::PageDirectoryPtr PageDirectoryFactory<Trait>::dangerouslyCreateFromEditWithoutWAL(
    const String & storage_name,
    PageEntriesEdit & edit)
{
    PageDirectoryPtr dir = std::make_unique<typename Trait::PageDirectory>(std::move(storage_name), nullptr);

    loadEdit(dir, edit, /*force_apply*/ true);
    // Reset the `sequence` to the maximum of persisted.
    dir->sequence = max_applied_ver.sequence;

    // Remove invalid entries
    dir->gcInMemEntries({.need_removed_entries = false});

    return dir;
}

// just for test
template <typename Trait>
typename PageDirectoryFactory<Trait>::PageDirectoryPtr PageDirectoryFactory<Trait>::createFromEditForTest(
    const String & storage_name,
    FileProviderPtr & file_provider,
    PSDiskDelegatorPtr & delegator,
    PageEntriesEdit & edit,
    UInt64 filter_seq)
{
    auto [wal, reader] = WALStore::create(storage_name, file_provider, delegator, WALConfig());
    (void)reader;
    PageDirectoryPtr dir = std::make_unique<typename Trait::PageDirectory>(storage_name, std::move(wal));

    // Allocate mock sequence to run gc
    UInt64 mock_sequence = 0;
    for (auto & r : edit.getMutRecords())
    {
        r.version.sequence = ++mock_sequence;
    }

    loadEdit(dir, edit, /*force_apply*/ false, filter_seq);
    // Reset the `sequence` to the maximum of persisted.
    dir->sequence = max_applied_ver.sequence;
    RUNTIME_CHECK(dir->sequence, mock_sequence);

    // After restoring from the disk, we need cleanup all invalid entries in memory, or it will
    // try to run GC again on some entries that are already marked as invalid in BlobStore.
    // It's no need to remove the expired entries in BlobStore when restore, so no need to fill removed_entries.
    dir->gcInMemEntries({.need_removed_entries = false});
    LOG_INFO(
        DB::Logger::get(storage_name),
        "PageDirectory restored, max_page_id={} max_applied_ver={}",
        dir->getMaxIdAfterRestart(),
        dir->sequence);

    restoreBlobStats(dir);

    return dir;
}

template <typename Trait>
void PageDirectoryFactory<Trait>::restoreBlobStats(const PageDirectoryPtr & dir)
{
    if (!blob_stats)
        return;

    // After all entries restored to `mvcc_table_directory`, only apply
    // the latest entry to `blob_stats`, or we may meet error since
    // some entries may be removed in memory but not get compacted
    // in the log file.
    for (const auto & [page_id, entries] : dir->mvcc_table_directory)
    {
        // We should restore the entry to `blob_stats` even if it is marked as "deleted",
        // or we will mistakenly reuse the space to write other blobs down into that space.
        // So we need to use `getLastEntry` instead of `getEntry(version)` here.
        if (auto entry = entries->getLastEntry(std::nullopt); entry)
        {
            auto [success, details_msg] = blob_stats->restoreByEntry(*entry);
            if (success)
                continue;

            // Restore entry to blob_stats fail, if the entry->size == 0,
            // it is acceptable. Just ingore.
            if (entry->size == 0)
            {
                // log down the page_id for tracing back
                LOG_WARNING(
                    Logger::get(),
                    "Restore position from BlobStat ignore empty page"
                    ", offset=0x{:X} blob_id={} page_id={} entry={}",
                    entry->offset,
                    entry->file_id,
                    page_id,
                    *entry);
            }
            else
            {
                LOG_ERROR(Logger::get(), details_msg);
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Restore position from BlobStat failed, the space/subspace is already being used"
                    ", offset=0x{:X} blob_id={} page_id={} entry={}",
                    entry->offset,
                    entry->file_id,
                    page_id,
                    *entry);
            }
        }
    }

    blob_stats->restore();
}

template <typename Trait>
void PageDirectoryFactory<Trait>::loadEdit(
    const PageDirectoryPtr & dir,
    const PageEntriesEdit & edit,
    bool force_apply,
    UInt64 filter_seq)
{
    // Relax some check at the beginning
    bool strict_check = false;
    for (const auto & r : edit.getRecords())
    {
        // Turn on strict check once we meet a record that is larger than `filter_seq`
        if (r.version.sequence > filter_seq)
            strict_check = true;

        if (likely(!debug.dump_entries))
        {
            // Because REF is not an idempotent operation, when loading edit from disk to restore
            // the PageDirectory, it could re-apply a REF to an non-existing page_id that is already
            // deleted in the dumped snapshot.
            // So we filter the REF record which is less than or equal to the `filter_seq`
            // Is this entry could be duplicated with the dumped snapshot
            bool filter = !force_apply && r.version.sequence <= filter_seq && r.type == EditRecordType::REF;
            if (unlikely(filter))
            {
                LOG_INFO(Logger::get(), "Not idempotent REF record is ignored during restart, record={}", r);
                updateMaxIdByRecord(dir, r);
                continue;
            }

            if (max_applied_ver < r.version)
                max_applied_ver = r.version;

            applyRecord(dir, r, strict_check);
            continue;
        }

        // for debug, we always show all entries
        if (max_applied_ver < r.version)
            max_applied_ver = r.version;
        LOG_INFO(Logger::get(), "{}", r);
        if (debug.apply_entries_to_directory)
            applyRecord(dir, r, strict_check);
    }
}

template <typename Trait>
void PageDirectoryFactory<Trait>::applyRecord(
    const PageDirectoryPtr & dir,
    const typename PageEntriesEdit::EditRecord & r,
    bool strict_check)
{
    auto [iter, created] = dir->mvcc_table_directory.insert(std::make_pair(r.page_id, nullptr));
    if (created)
    {
        if constexpr (std::is_same_v<Trait, u128::FactoryTrait>)
        {
            iter->second = std::make_shared<VersionedPageEntries<u128::PageDirectoryTrait>>();
        }
        else if constexpr (std::is_same_v<Trait, universal::FactoryTrait>)
        {
            iter->second = std::make_shared<VersionedPageEntries<universal::PageDirectoryTrait>>();
        }
    }

    updateMaxIdByRecord(dir, r);

    const auto & version_list = iter->second;
    const auto & restored_version = r.version;
    try
    {
        switch (r.type)
        {
        case EditRecordType::VAR_EXTERNAL:
        case EditRecordType::VAR_REF:
        {
            auto holder = version_list->fromRestored(r);
            if (holder)
            {
                *holder = r.page_id;
                dir->external_ids_by_ns.addExternalIdUnlock(holder);
            }
            break;
        }
        case EditRecordType::VAR_ENTRY:
            version_list->fromRestored(r);
            break;
        case EditRecordType::PUT_EXTERNAL:
        {
            auto holder = version_list->createNewExternal(restored_version, r.entry);
            if (holder)
            {
                *holder = r.page_id;
                dir->external_ids_by_ns.addExternalIdUnlock(holder);
            }
            break;
        }
        case EditRecordType::PUT:
            version_list->createNewEntry(restored_version, r.entry);
            break;
        case EditRecordType::UPDATE_DATA_FROM_REMOTE:
        {
            auto id_to_resolve = r.page_id;
            auto sequence_to_resolve = restored_version.sequence;
            auto version_list_iter = iter;
            while (true)
            {
                const auto & current_version_list = version_list_iter->second;
                // We need to ignore the "deletes" both when resolve page id and update local cache.
                // Check `PageDirectory::getByIDImpl` or the unit test
                // `UniPageStorageRemoteReadTest.WriteReadRefWithRestart` for details.
                const bool ignore_delete = id_to_resolve != r.page_id;
                auto [resolve_state, next_id_to_resolve, next_ver_to_resolve]
                    = current_version_list->resolveToPageId(sequence_to_resolve, ignore_delete, nullptr);
                if (resolve_state == ResolveResult::TO_NORMAL)
                {
                    current_version_list->updateLocalCacheForRemotePage(
                        PageVersion(sequence_to_resolve, 0),
                        r.entry,
                        ignore_delete);
                    break;
                }
                else if (resolve_state == ResolveResult::TO_REF)
                {
                    id_to_resolve = next_id_to_resolve;
                    sequence_to_resolve = next_ver_to_resolve.sequence;
                }
                else
                {
                    RUNTIME_CHECK(false);
                }
                version_list_iter = dir->mvcc_table_directory.lower_bound(id_to_resolve);
                assert(version_list_iter != dir->mvcc_table_directory.end());
            }
            break;
        }
        case EditRecordType::DEL:
        case EditRecordType::VAR_DELETE: // nothing different from `DEL`
            version_list->createDelete(restored_version);
            break;
        case EditRecordType::REF:
            Trait::PageDirectory::applyRefEditRecord(dir->mvcc_table_directory, version_list, r, restored_version);
            break;
        case EditRecordType::UPSERT:
        {
            auto id_to_deref = version_list->createUpsertEntry(restored_version, r.entry, strict_check);
            if (Trait::PageIdTrait::getU64ID(id_to_deref) != INVALID_PAGE_U64_ID)
            {
                // The ref-page is rewritten into a normal page, we need to decrease the ref-count of the original page
                auto deref_iter = dir->mvcc_table_directory.find(id_to_deref);
                RUNTIME_CHECK_MSG(
                    deref_iter != dir->mvcc_table_directory.end(),
                    "Can't find page to deref when applying upsert, page_id={}",
                    id_to_deref);
                auto deref_res
                    = deref_iter->second->derefAndClean(/*lowest_seq*/ 0, id_to_deref, restored_version, 1, nullptr);
                RUNTIME_ASSERT(!deref_res);
            }
            break;
        }
        }
    }
    catch (DB::Exception & e)
    {
        e.addMessage(fmt::format(
            " [type={}] [page_id={}] [ver={}]",
            magic_enum::enum_name(r.type),
            r.page_id,
            restored_version));
        throw e;
    }
}

template <typename Trait>
void PageDirectoryFactory<Trait>::updateMaxIdByRecord(
    const PageDirectoryPtr & dir,
    const typename PageEntriesEdit::EditRecord & r)
{
    if constexpr (std::is_same_v<Trait, universal::FactoryTrait>)
    {
        // We only need page id under specific prefix after restart.
        // If you want to add other prefix here, make sure the page id allocation space is still enough after adding it.
        if (UniversalPageIdFormat::isType(r.page_id, StorageType::Data)
            || UniversalPageIdFormat::isType(r.page_id, StorageType::Log)
            || UniversalPageIdFormat::isType(r.page_id, StorageType::Meta))
        {
            dir->max_page_id = std::max(dir->max_page_id, Trait::PageIdTrait::getU64ID(r.page_id));
        }
    }
    else
    {
        dir->max_page_id = std::max(dir->max_page_id, Trait::PageIdTrait::getU64ID(r.page_id));
    }
}

template <typename Trait>
void PageDirectoryFactory<Trait>::loadFromDisk(const PageDirectoryPtr & dir, WALStoreReaderPtr && reader)
{
    DataFileIdSet data_file_ids;
    auto checkpoint_snap_seq = reader->getSnapSeqForCheckpoint();
    // make sure the max sequence is larger or equal than the checkpoint sequence
    if (max_applied_ver.sequence < checkpoint_snap_seq)
        max_applied_ver = PageVersion(checkpoint_snap_seq, 0);

    while (reader->remained())
    {
        auto [from_checkpoint, record] = reader->next();
        if (!record)
        {
            // TODO: Handle error, some error could be ignored.
            // If the file happened to some error,
            // should truncate it to throw away incomplete data.
            reader->throwIfError();
            // else it just run to the end of file.
            break;
        }

        // The edits in later log files may have some overlap with the first checkpoint file.
        // But we want to just apply each edit exactly once.
        // So we will skip edits in later log files if they are already applied.
        if constexpr (std::is_same_v<Trait, u128::FactoryTrait>)
        {
            auto edit = Trait::Serializer::deserializeFrom(record.value(), nullptr);
            loadEdit(dir, edit, from_checkpoint, checkpoint_snap_seq);
        }
        else if constexpr (std::is_same_v<Trait, universal::FactoryTrait>)
        {
            auto edit = Trait::Serializer::deserializeFrom(record.value(), &data_file_ids);
            loadEdit(dir, edit, from_checkpoint, checkpoint_snap_seq);
        }
        else
        {
            RUNTIME_CHECK(false);
        }
    }
}

template class PageDirectoryFactory<u128::FactoryTrait>;
template class PageDirectoryFactory<universal::FactoryTrait>;
} // namespace PS::V3
} // namespace DB
