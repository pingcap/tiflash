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

#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/WAL/WALReader.h>
#include <Storages/Page/V3/WALStore.h>

#include <memory>

namespace DB::PS::V3
{
PageDirectoryPtr PageDirectoryFactory::create(FileProviderPtr & file_provider, PSDiskDelegatorPtr & delegator)
{
    auto [wal, reader] = WALStore::create(file_provider, delegator);
    PageDirectoryPtr dir = std::make_unique<PageDirectory>(std::move(wal));
    loadFromDisk(dir, std::move(reader));

    // After restoring from the disk, we need cleanup all invalid entries in memory, or it will
    // try to run GC again on some entries that are already marked as invalid in BlobStore.
    dir->gcInMemEntries();

    if (blob_stats)
    {
        // After all entries restored to `mvcc_table_directory`, only apply
        // the latest entry to `blob_stats`, or we may meet error since
        // some entries may be removed in memory but not get compacted
        // in the log file.
        for (const auto & [page_id, entries] : dir->mvcc_table_directory)
        {
            (void)page_id;

            // We can't use getEntry(max_seq) to get the entry.
            // Otherwise, It is likely to cause data loss.
            // for example:
            //
            //   page id 4927
            //   {type:5, create_ver: <0,0>, is_deleted: false, delete_ver: <0,0>, ori_page_id: 0.0, being_ref_count: 1, num_entries: 2}
            //     entry 0
            //       sequence: 1802
            //       epoch: 0
            //       is del: false
            //       blob id: 5
            //       offset: 77661628
            //       size: 2381165
            //       crc: 0x1D1BEF504F12D3A0
            //       field offset size: 0
            //     entry 1
            //       sequence: 2121
            //       epoch: 0
            //       is del: true
            //       blob id: 0
            //       offset: 0
            //       size: 0
            //       crc: 0x0
            //       field offset size: 0
            //   page id 5819
            //   {type:6, create_ver: <2090,0>, is_deleted: false, delete_ver: <0,0>, ori_page_id: 0.4927, being_ref_count: 1, num_entries: 0}
            //
            // After getEntry, page id `4927` won't be restore by BlobStore.
            if (auto entry = entries->getLastEntry(); entry)
            {
                blob_stats->restoreByEntry(*entry);
            }
        }

        blob_stats->restore();
    }

    // TODO: After restored ends, set the last offset of log file for `wal`
    return dir;
}

PageDirectoryPtr PageDirectoryFactory::createFromEdit(FileProviderPtr & file_provider, PSDiskDelegatorPtr & delegator, const PageEntriesEdit & edit)
{
    auto [wal, reader] = WALStore::create(file_provider, delegator);
    (void)reader;
    PageDirectoryPtr dir = std::make_unique<PageDirectory>(std::move(wal));
    loadEdit(dir, edit);
    if (blob_stats)
        blob_stats->restore();

    return dir;
}

void PageDirectoryFactory::loadEdit(const PageDirectoryPtr & dir, const PageEntriesEdit & edit)
{
    PageDirectory::MVCCMapType & mvcc_table_directory = dir->mvcc_table_directory;

    for (const auto & r : edit.getRecords())
    {
        max_applied_page_id = std::max(r.page_id, max_applied_page_id);

        auto [iter, created] = mvcc_table_directory.insert(std::make_pair(r.page_id, nullptr));
        if (created)
        {
            iter->second = std::make_shared<VersionedPageEntries>();
        }

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
                    dir->external_ids.emplace_back(std::weak_ptr<PageIdV3Internal>(holder));
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
                    dir->external_ids.emplace_back(std::weak_ptr<PageIdV3Internal>(holder));
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
                PageDirectory::applyRefEditRecord(mvcc_table_directory, version_list, r, restored_version);
                break;
            case EditRecordType::UPSERT:
                version_list->createNewEntry(restored_version, r.entry);
                break;
            }
        }
        catch (DB::Exception & e)
        {
            e.addMessage(fmt::format(" [type={}] [page_id={}] [ver={}]", r.type, r.page_id, restored_version));
            throw e;
        }
    }
}

void PageDirectoryFactory::loadFromDisk(const PageDirectoryPtr & dir, WALStoreReaderPtr && reader)
{
    while (reader->remained())
    {
        auto [ok, edit] = reader->next();
        if (!ok)
        {
            // TODO: Handle error, some error could be ignored.
            // If the file happened to some error,
            // should truncate it to throw away incomplete data.
            reader->throwIfError();
            // else it just run to the end of file.
            break;
        }

        // apply the edit read
        loadEdit(dir, edit);
    }
}
} // namespace DB::PS::V3
