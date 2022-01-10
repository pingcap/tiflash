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

    auto snap = createSnapshot();

    // stage 1, persisted the changes to WAL
    // wal.apply(edit);

    // stage 2, create entry version list for pageId. nothing need to be rollback
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

    // stage 3, there are no rollback since we already persist `edit` to WAL, just ignore error if any
    const auto & records = edit.getRecords();
    for (size_t idx = 0; idx < records.size(); ++idx)
    {
        const auto & r = records[idx];
        switch (r.type)
        {
        case WriteBatch::WriteType::PUT:
            [[fallthrough]];
        case WriteBatch::WriteType::UPSERT:
            updating_pages[idx]->createNewVersion(last_sequence + 1, r.entry);
            break;
        case WriteBatch::WriteType::REF:
        {
            // We can't handle `REF` before other writes, because `PUT` and `REF`
            // maybe in the same WriteBatch.
            // Also we can't throw an exception if we can't find the origin page_id,
            // because WAL have already applied the change and there is no
            // mechanism to roll back changes in the WAL.
            auto iter = mvcc_table_directory.find(r.ori_page_id);
            if (iter == mvcc_table_directory.end())
            {
                LOG_FMT_WARNING(log, "Trying to add ref from {} to non-exist {} with sequence={}", r.page_id, r.ori_page_id, last_sequence + 1);
                break;
            }

            if (auto entry = iter->second->getEntry(last_sequence); entry)
            {
                // copy the entry to be ref
                updating_pages[idx]->createNewVersion(last_sequence + 1, *entry);
            }
            else
            {
                LOG_FMT_WARNING(log, "Trying to add ref from {} to non-exist {} with sequence={}", r.page_id, r.ori_page_id, last_sequence + 1);
            }
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

bool PageDirectory::gc()
{
    // Cleanup released snapshots
    for (auto iter = snapshots.begin(); iter != snapshots.end(); /* empty */)
    {
        if (iter->expired())
            iter = snapshots.erase(iter);
        else
            ++iter;
    }
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

} // namespace PS::V3
} // namespace DB
