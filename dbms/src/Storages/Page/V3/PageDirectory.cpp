#include <Common/Exception.h>
#include <Common/LogWithPrefix.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/WriteBatch.h>
#include <common/logger_useful.h>

#include <memory>
#include <mutex>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int PS_ENTRY_NOT_EXISTS;
extern const int PS_ENTRY_NO_VALID_VERSION;
} // namespace ErrorCodes
namespace PS::V3
{
std::optional<PageEntryV3> PageDirectory::VersionedPageEntries::getEntry(UInt64 seq) const
{
    auto page_lock = acquireLock();
    // entries are sorted by <ver, epoch>, find the first one less than <ver+1, 0>
    if (auto iter = MapUtils::findLess(entries, PageVersionType(seq + 1));
        iter != entries.end())
    {
        if (!iter->second.is_delete)
            return iter->second.entry;
    }
    return std::nullopt;
}

PageDirectory::PageDirectory()
    : sequence(0)
    , log(getLogWithPrefix(nullptr, "PageDirectory"))
{
}

void PageDirectory::restore()
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

PageDirectorySnapshotPtr PageDirectory::createSnapshot() const
{
    auto snap = std::make_shared<PageDirectorySnapshot>(sequence.load());
    snapshots.emplace_back(std::weak_ptr<PageDirectorySnapshot>(snap));
    return snap;
}

PageIDAndEntryV3 PageDirectory::get(PageId page_id, const PageDirectorySnapshotPtr & snap) const
{
    MVCCMapType::const_iterator iter;
    {
        std::shared_lock read_lock(table_rw_mutex);
        iter = mvcc_table_directory.find(page_id);
        if (iter == mvcc_table_directory.end())
            throw Exception(fmt::format("Entry [id={}] not exist!", page_id), ErrorCodes::PS_ENTRY_NOT_EXISTS);
    }

    if (auto entry = iter->second->getEntry(snap->sequence); entry)
    {
        return PageIDAndEntryV3(page_id, *entry);
    }

    throw Exception(fmt::format("Entry [id={}] [seq={}] not exist!", page_id, snap->sequence), ErrorCodes::PS_ENTRY_NO_VALID_VERSION);
}

PageIDAndEntriesV3 PageDirectory::get(const PageIds & page_ids, const PageDirectorySnapshotPtr & snap) const
{
    std::vector<MVCCMapType::const_iterator> iters;
    iters.reserve(page_ids.size());
    {
        std::shared_lock read_lock(table_rw_mutex);
        for (size_t idx = 0; idx < page_ids.size(); ++idx)
        {
            if (auto iter = mvcc_table_directory.find(page_ids[idx]);
                iter != mvcc_table_directory.end())
            {
                iters.emplace_back(iter);
            }
            else
            {
                throw Exception(fmt::format("Entry [id={}] at [idx={}] not exist!", page_ids[idx], idx), ErrorCodes::PS_ENTRY_NOT_EXISTS);
            }
        }
    }

    PageIDAndEntriesV3 id_entries;
    for (size_t idx = 0; idx < page_ids.size(); ++idx)
    {
        const auto & iter = iters[idx];
        if (auto entry = iter->second->getEntry(snap->sequence); entry)
        {
            id_entries.emplace_back(page_ids[idx], *entry);
        }
        else
            throw Exception(fmt::format("Entry [id={}] [seq={}] at [idx={}] not exist!", page_ids[idx], snap->sequence, idx), ErrorCodes::PS_ENTRY_NO_VALID_VERSION);
    }

    return id_entries;
}

void PageDirectory::apply(PageEntriesEdit && edit)
{
    std::unique_lock write_lock(table_rw_mutex); // TODO: It is totally serialized, make it a pipeline
    UInt64 last_sequence = sequence.load();

    // stage 1, get the entry to be ref
    auto snap = createSnapshot();
    for (auto & r : edit.getRecords())
    {
        // Set the version of inserted entries
        r.version = PageVersionType(last_sequence + 1);

        if (r.type != WriteBatch::WriteType::REF)
        {
            continue;
        }
        auto iter = mvcc_table_directory.find(r.ori_page_id);
        if (iter == mvcc_table_directory.end())
        {
            throw Exception(fmt::format("Trying to add ref from {} to non-exist {} with sequence={}", r.page_id, r.ori_page_id, last_sequence + 1), ErrorCodes::LOGICAL_ERROR);
        }
        if (auto entry = iter->second->getEntry(last_sequence); entry)
        {
            // copy the entry to be ref
            r.entry = *entry;
        }
        else
        {
            throw Exception(fmt::format("Trying to add ref from {} to non-exist {} with sequence={}", r.page_id, r.ori_page_id, last_sequence + 1), ErrorCodes::LOGICAL_ERROR);
        }
    }

    // stage 2, persisted the changes to WAL
    // wal.apply(edit);

    // stage 3, create entry version list for pageId. nothing need to be rollback
    std::unordered_map<PageId, std::pair<PageLock, int>> updating_locks;
    std::vector<VersionedPageEntriesPtr> updating_pages;
    updating_pages.reserve(edit.size());
    for (const auto & r : edit.getRecords())
    {
        auto [iter, created] = mvcc_table_directory.insert(std::make_pair(r.page_id, nullptr));
        if (created)
        {
            iter->second = std::make_shared<VersionedPageEntries>();
        }

        auto update_it = updating_locks.find(r.page_id);
        if (update_it == updating_locks.end())
        {
            updating_locks.emplace(r.page_id, std::make_pair(iter->second->acquireLock(), 1));
        }
        else
        {
            update_it->second.second += 1;
        }

        updating_pages.emplace_back(iter->second);
    }

    // stage 4, there are no rollback since we already persist `edit` to WAL, just ignore error if any
    const auto & records = edit.getRecords();
    for (size_t idx = 0; idx < records.size(); ++idx)
    {
        const auto & r = records[idx];
        switch (r.type)
        {
        case WriteBatch::WriteType::PUT:
            [[fallthrough]];
        case WriteBatch::WriteType::UPSERT:
            [[fallthrough]];
        case WriteBatch::WriteType::REF:
        {
            // Put/upsert/ref all should append a new version for this page
            updating_pages[idx]->createNewVersion(last_sequence + 1, r.entry);
            break;
        }
        case WriteBatch::WriteType::DEL:
        {
            updating_pages[idx]->createDelete(last_sequence + 1);
            break;
        }
        }

        auto locks_it = updating_locks.find(r.page_id);
        locks_it->second.second -= 1;
        if (locks_it->second.second == 0)
            updating_locks.erase(r.page_id);
    }

    if (!updating_locks.empty())
    {
        throw Exception(fmt::format("`updating_locks` must be cleared. But there are some locks not been erased. [remain sizes={}]", updating_locks.size()),
                        ErrorCodes::LOGICAL_ERROR);
    }

    // The edit committed, incr the sequence number to publish changes for `createSnapshot`
    sequence.fetch_add(1);
}


std::pair<std::list<PageEntryV3>, bool> PageDirectory::VersionedPageEntries::deleteAndGC(UInt64 lowest_seq)
{
    std::list<PageEntryV3> del_entries;

    auto page_lock = acquireLock();

    if (entries.size() == 0)
    {
        return std::make_pair(del_entries, true);
    }

    auto iter = MapUtils::findLessEQ(entries, PageVersionType(lowest_seq));

    if (iter == entries.begin())
    {
        return std::make_pair(del_entries, false);
    }

    // If we can't find any seq lower than `lowest_seq`
    // It means all seq in this entry no need gc.
    if (iter == entries.end())
    {
        return std::make_pair(del_entries, false);
    }

    for (--iter; iter != entries.begin(); iter--)
    {
        // Already deleted, no need put in `del_entries`
        if (iter->second.is_delete)
        {
            continue;
        }

        del_entries.emplace_back(iter->second.entry);
        iter = entries.erase(iter);
    }

    // erase begin
    del_entries.emplace_back(iter->second.entry);
    entries.erase(iter);

    return std::make_pair(del_entries, entries.empty() || (entries.size() == 1 && entries.begin()->second.is_delete));
}

PageSize PageDirectory::VersionedPageEntries::getEntryByBlobId(std::map<BlobFileId, VersionedPageIdAndEntryList> & blob_ids, PageId page_id)
{
    PageSize single_page_size = 0;
    for (auto iter = entries.begin(); iter != entries.end(); iter++)
    {
        if (iter->second.is_delete)
        {
            continue;
        }

        const auto & entry = iter->second.entry;

        if (auto it = blob_ids.find(entry.file_id); it != blob_ids.end())
        {
            single_page_size += entry.size;
            it->second.emplace_back(std::make_tuple(page_id, iter->first, iter->second.entry));
        }
    }

    return single_page_size;
}

void PageDirectory::snapshotsGC()
{
    UInt64 lowest_seq = UINT64_MAX;

    // Cleanup released snapshots
    for (auto iter = snapshots.begin(); iter != snapshots.end(); /* empty */)
    {
        if (iter->expired())
            iter = snapshots.erase(iter);
        else
        {
            if (lowest_seq == UINT64_MAX)
            {
                lowest_seq = iter->lock()->sequence;
            }
            ++iter;
        }
    }

    if (lowest_seq == UINT64_MAX)
    {
        return;
    }

    for (auto iter = mvcc_table_directory.begin(); iter != mvcc_table_directory.end(); iter++)
    {
        const auto & [del_entries, all_deleted] = iter->second->deleteAndGC(lowest_seq);
        if (!del_entries.empty())
        {
            blobstore->remove(del_entries);
        }

        if (all_deleted)
        {
            std::unique_lock write_lock(table_rw_mutex);
            iter = mvcc_table_directory.erase(iter);
        }
    }

    return;
}


void PageDirectory::gcApply(std::map<PageEntryV3, VersionedPageIdAndEntry> & copy_list)
{
    for (auto & [entry, versioned_pageid_entry] : copy_list)
    {
        auto page_id = std::get<0>(versioned_pageid_entry);
        auto version = std::get<1>(versioned_pageid_entry);

        auto iter = mvcc_table_directory.find(page_id);
        if (iter == mvcc_table_directory.end())
        {
            throw Exception(fmt::format("Can't found [pageid={}]", page_id), ErrorCodes::LOGICAL_ERROR);
        }

        auto versioned_page = iter->second;
        iter->second->acquireLock();
        iter->second->createNewVersion(version.sequence, version.epoch + 1, entry);
    }
}

// FIXME : should put into pagestore.
void PageDirectory::blobstoreGC()
{
    std::map<BlobFileId, VersionedPageIdAndEntryList> blob_need_gc;
    blobstore->getGCStats(blob_need_gc);
    PageSize total_page_size = 0;
    {
        std::shared_lock read_lock(table_rw_mutex);

        for (auto iter = mvcc_table_directory.begin();
             iter != mvcc_table_directory.end();
             iter++)
        {
            total_page_size += iter->second->getEntryByBlobId(blob_need_gc, iter->first);
        }
    }

    if (blob_need_gc.empty())
    {
        return;
    }

    std::map<PageEntryV3, VersionedPageIdAndEntry> copy_list;
    copy_list = blobstore->gc(blob_need_gc, total_page_size);

    if (copy_list.empty())
    {
        throw Exception("Something wrong after BlobStore GC.", ErrorCodes::LOGICAL_ERROR);
    }

    gcApply(copy_list);
}

void PageDirectory::gc()
{
    snapshotsGC();
    blobstoreGC();
}

} // namespace PS::V3
} // namespace DB
