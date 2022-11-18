// Copyright 2022 PingCAP, Ltd.
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

#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/WAL/WALReader.h>
#include <Storages/Page/V3/WAL/serialize.h>
#include <Storages/Page/V3/WALStore.h>

#include <memory>
#include <optional>
#include <type_traits>

namespace DB
{
namespace ErrorCodes
{
extern const int PS_DIR_APPLY_INVALID_STATUS;
} // namespace ErrorCodes
namespace PS::V3
{
template <typename Trait>
typename Trait::PageDirectoryPtr
PageDirectoryFactory<Trait>::create(String storage_name, FileProviderPtr & file_provider, PSDiskDelegatorPtr & delegator, WALConfig config)
{
    auto [wal, reader] = WALStore::create(storage_name, file_provider, delegator, config);
    return createFromReader(storage_name, reader, std::move(wal));
}

template <typename Trait>
typename Trait::PageDirectoryPtr
PageDirectoryFactory<Trait>::createFromReader(String storage_name, WALStoreReaderPtr reader, WALStorePtr wal)
{
    typename Trait::PageDirectoryPtr dir = std::make_unique<typename Trait::PageDirectoryType>(storage_name, std::move(wal));
    loadFromDisk(dir, std::move(reader));

    // Reset the `sequence` to the maximum of persisted.
    dir->sequence = max_applied_ver.sequence;

    // After restoring from the disk, we need cleanup all invalid entries in memory, or it will
    // try to run GC again on some entries that are already marked as invalid in BlobStore.
    // It's no need to remove the expired entries in BlobStore, so skip filling removed_entries to improve performance.
    dir->gcInMemEntries(/*return_removed_entries=*/false);
    LOG_INFO(DB::Logger::get(storage_name), "PageDirectory restored [max_page_id={}] [max_applied_ver={}]", dir->getMaxId(), dir->sequence);

    if (blob_stats)
    {
        // After all entries restored to `mvcc_table_directory`, only apply
        // the latest entry to `blob_stats`, or we may meet error since
        // some entries may be removed in memory but not get compacted
        // in the log file.
        for (const auto & [page_id, entries] : dir->mvcc_table_directory)
        {
            (void)page_id;

            // We should restore the entry to `blob_stats` even if it is marked as "deleted",
            // or we will mistakenly reuse the space to write other blobs down into that space.
            // So we need to use `getLastEntry` instead of `getEntry(version)` here.
            if (auto entry = entries->getLastEntry(std::nullopt); entry)
            {
                blob_stats->restoreByEntry(*entry);
            }
        }

        blob_stats->restore();
    }

    // TODO: After restored ends, set the last offset of log file for `wal`
    return dir;
}

// just for test
template <typename Trait>
typename Trait::PageDirectoryPtr
PageDirectoryFactory<Trait>::createFromEdit(String storage_name, FileProviderPtr & file_provider, PSDiskDelegatorPtr & delegator, typename Trait::PageEntriesEdit & edit)
{
    auto [wal, reader] = WALStore::create(storage_name, file_provider, delegator, WALConfig());
    (void)reader;
    typename Trait::PageDirectoryPtr dir = std::make_unique<typename Trait::PageDirectoryType>(std::move(storage_name), std::move(wal));

    // Allocate mock sequence to run gc
    UInt64 mock_sequence = 0;
    for (auto & r : edit.getMutRecords())
    {
        r.version.sequence = ++mock_sequence;
    }
    loadEdit(dir, edit);
    // Reset the `sequence` to the maximum of persisted.
    dir->sequence = max_applied_ver.sequence;
    RUNTIME_CHECK(dir->sequence, mock_sequence);

    // After restoring from the disk, we need cleanup all invalid entries in memory, or it will
    // try to run GC again on some entries that are already marked as invalid in BlobStore.
    // It's no need to remove the expired entries in BlobStore when restore, so no need to fill removed_entries.
    dir->gcInMemEntries(/*return_removed_entries=*/false);

    if (blob_stats)
    {
        // After all entries restored to `mvcc_table_directory`, only apply
        // the latest entry to `blob_stats`, or we may meet error since
        // some entries may be removed in memory but not get compacted
        // in the log file.
        for (const auto & [page_id, entries] : dir->mvcc_table_directory)
        {
            (void)page_id;

            // We should restore the entry to `blob_stats` even if it is marked as "deleted",
            // or we will mistakenly reuse the space to write other blobs down into that space.
            // So we need to use `getLastEntry` instead of `getEntry(version)` here.
            if (auto entry = entries->getLastEntry(std::nullopt); entry)
            {
                blob_stats->restoreByEntry(*entry);
            }
        }

        blob_stats->restore();
    }

    return dir;
}

template <typename Trait>
void PageDirectoryFactory<Trait>::loadEdit(const typename Trait::PageDirectoryPtr & dir, const typename Trait::PageEntriesEdit & edit)
{
    for (const auto & r : edit.getRecords())
    {
        if (max_applied_ver < r.version)
            max_applied_ver = r.version;

        if (dump_entries)
            LOG_INFO(Logger::get(), r.toDebugString());
        applyRecord(dir, r);
    }
}

template <typename Trait>
void PageDirectoryFactory<Trait>::applyRecord(
    const typename Trait::PageDirectoryPtr & dir,
    const typename Trait::PageEntriesEdit::EditRecord & r)
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

    dir->max_page_id = std::max(dir->max_page_id, ExternalIdTrait::getU64ID(r.page_id));

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
            auto holder = version_list->createNewExternal(restored_version);
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
        case EditRecordType::DEL:
        case EditRecordType::VAR_DELETE: // nothing different from `DEL`
            version_list->createDelete(restored_version);
            break;
        case EditRecordType::REF:
            Trait::PageDirectoryType::applyRefEditRecord(
                dir->mvcc_table_directory,
                version_list,
                r,
                restored_version);
            break;
        case EditRecordType::UPSERT:
        {
            auto id_to_deref = version_list->createUpsertEntry(restored_version, r.entry);
            if (id_to_deref != ExternalIdTrait::getInvalidID())
            {
                // The ref-page is rewritten into a normal page, we need to decrease the ref-count of the original page
                auto deref_iter = dir->mvcc_table_directory.find(id_to_deref);
                RUNTIME_CHECK_MSG(deref_iter != dir->mvcc_table_directory.end(), "Can't find [page_id={}] to deref when applying upsert", id_to_deref);
                auto deref_res = deref_iter->second->derefAndClean(/*lowest_seq*/ 0, id_to_deref, restored_version, 1, nullptr);
                RUNTIME_ASSERT(!deref_res);
            }
            break;
        }
        }
    }
    catch (DB::Exception & e)
    {
        e.addMessage(fmt::format(" [type={}] [page_id={}] [ver={}]", magic_enum::enum_name(r.type), r.page_id, restored_version));
        throw e;
    }
}

template <typename Trait>
void PageDirectoryFactory<Trait>::loadFromDisk(const typename Trait::PageDirectoryPtr & dir, WALStoreReaderPtr && reader)
{
    while (reader->remained())
    {
        auto record = reader->next();
        if (!record)
        {
            // TODO: Handle error, some error could be ignored.
            // If the file happened to some error,
            // should truncate it to throw away incomplete data.
            reader->throwIfError();
            // else it just run to the end of file.
            break;
        }

        // apply the edit read
        auto edit = Trait::Serializer::deserializeFrom(record.value());
        loadEdit(dir, edit);
    }
}

template class PageDirectoryFactory<u128::FactoryTrait>;
template class PageDirectoryFactory<universal::FactoryTrait>;
} // namespace PS::V3
} // namespace DB
