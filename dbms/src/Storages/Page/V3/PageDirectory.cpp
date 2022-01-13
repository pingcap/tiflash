#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/LogWithPrefix.h>
#include <Common/assert_cast.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/WriteBatch.h>
#include <common/logger_useful.h>

#include <memory>
#include <mutex>
#include <type_traits>

namespace CurrentMetrics
{
extern const Metric PSMVCCSnapshotsList;
} // namespace CurrentMetrics

namespace DB
{
namespace FailPoints
{
extern const char random_slow_page_storage_remove_expired_snapshots[];
} // namespace FailPoints

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int PS_ENTRY_NOT_EXISTS;
extern const int PS_ENTRY_NO_VALID_VERSION;
} // namespace ErrorCodes
namespace PS::V3
{
std::optional<PageEntryV3> VersionedPageEntries::getEntry(UInt64 seq) const
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


std::pair<VersionedEntries, PageSize> VersionedPageEntries::getEntriesByBlobId(BlobFileId blob_id)
{
    PageSize single_page_size = 0;
    VersionedEntries versioned_entries;

    auto page_lock = acquireLock();
    for (const auto & [versioned_type, entry_del] : entries)
    {
        if (entry_del.is_delete)
        {
            continue;
        }

        const auto & entry = entry_del.entry;

        if (entry.file_id == blob_id)
        {
            single_page_size += entry.size;
            versioned_entries.emplace_back(std::make_pair(versioned_type, entry_del.entry));
        }
    }

    return std::make_pair(std::move(versioned_entries), single_page_size);
}

std::pair<PageEntriesV3, bool> VersionedPageEntries::deleteAndGC(UInt64 lowest_seq)
{
    PageEntriesV3 del_entries;

    auto page_lock = acquireLock();

    if (entries.empty())
    {
        return std::make_pair(del_entries, true);
    }

    auto iter = MapUtils::findLessEQ(entries, PageVersionType(lowest_seq, UINT64_MAX));

    // Nothing need be GC
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

    return std::make_pair(std::move(del_entries), entries.empty() || (entries.size() == 1 && entries.begin()->second.is_delete));
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

    CurrentMetrics::add(CurrentMetrics::PSMVCCSnapshotsList);
    return snap;
}


std::tuple<size_t, double, unsigned> PageDirectory::getSnapshotsStat() const
{
    double longest_living_seconds = 0.0;
    unsigned longest_living_from_thread_id = 0;
    DB::Int64 num_snapshots_removed = 0;
    size_t num_valid_snapshots = 0;

    for (auto iter = snapshots.begin(); iter != snapshots.end(); /* empty */)
    {
        if (iter->expired())
        {
            iter = snapshots.erase(iter);
            num_snapshots_removed++;
        }
        else
        {
            fiu_do_on(FailPoints::random_slow_page_storage_remove_expired_snapshots, {
                pcg64 rng(randomSeed());
                std::chrono::milliseconds ms{std::uniform_int_distribution(0, 900)(rng)}; // 0~900 milliseconds
                std::this_thread::sleep_for(ms);
            });

            const auto snapshot_lifetime = iter->lock()->elapsedSeconds();
            if (snapshot_lifetime > longest_living_seconds)
            {
                longest_living_seconds = snapshot_lifetime;
                longest_living_from_thread_id = iter->lock()->getTid();
            }
            num_valid_snapshots++;
            ++iter;
        }
    }

    CurrentMetrics::sub(CurrentMetrics::PSMVCCSnapshotsList, num_snapshots_removed);
    // Return some statistics of the oldest living snapshot.
    return {num_valid_snapshots, longest_living_seconds, longest_living_from_thread_id};
}

static inline PageDirectorySnapshotPtr
toConcreteSnapshot(const DB::PageStorageSnapshotPtr & ptr)
{
    return std::static_pointer_cast<PageDirectorySnapshot>(ptr);
}

PageIDAndEntryV3 PageDirectory::get(PageId page_id, const DB::PageStorageSnapshotPtr & snap) const
{
    return get(page_id, toConcreteSnapshot(snap));
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

PageIDAndEntriesV3 PageDirectory::get(const PageIds & page_ids, const DB::PageStorageSnapshotPtr & snap) const
{
    return get(page_ids, toConcreteSnapshot(snap));
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

PageId PageDirectory::getMaxId() const
{
    std::unique_lock write_lock(table_rw_mutex);
    PageId max_page_id;
    for (auto & [page_id, versioned] : mvcc_table_directory)
    {
        max_page_id = std::max(max_page_id, page_id);
    }
    return max_page_id;
}

std::set<PageId> PageDirectory::getAllPageIds()
{
    std::set<PageId> page_ids;
    for (auto & [page_id, versioned] : mvcc_table_directory)
    {
        (void)versioned;
        page_ids.insert(page_id);
    }
    return page_ids;
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


std::set<PageId> PageDirectory::gcApply(const PageIdAndVersionedEntryList & migrated_entries, bool need_scan_page_ids)
{
    for (const auto & [page_id, version, entry] : migrated_entries)
    {
        auto iter = mvcc_table_directory.find(page_id);
        if (iter == mvcc_table_directory.end())
        {
            throw Exception(fmt::format("Can't found [pageid={}] while doing gcApply", page_id), ErrorCodes::LOGICAL_ERROR);
        }

        auto versioned_page = iter->second;
        iter->second->acquireLock();
        iter->second->createNewVersion(version.sequence, version.epoch + 1, entry);

        // TBD: wal apply
    }

    if (!need_scan_page_ids)
    {
        return {};
    }

    std::set<PageId> page_ids;
    std::unique_lock write_lock(table_rw_mutex);

    for (const auto & [page_id, versioned_entries] : mvcc_table_directory)
    {
        page_ids.insert(page_id);
    }

    return page_ids;
}

std::pair<std::map<BlobFileId, PageIdAndVersionedEntries>, PageSize>
PageDirectory::getEntriesByBlobIds(const std::vector<BlobFileId> & blob_need_gc)
{
    std::map<BlobFileId, PageIdAndVersionedEntries> blob_versioned_entries;

    PageSize total_page_size = 0;
    {
        std::shared_lock read_lock(table_rw_mutex);

        for (const auto & blob_id : blob_need_gc)
        {
            PageIdAndVersionedEntries versioned_pageid_entries;

            for (const auto & [page_id, version_entries] : mvcc_table_directory)
            {
                VersionedEntries versioned_entries;
                PageSize page_size;
                std::tie(versioned_entries, page_size) = version_entries->getEntriesByBlobId(blob_id);
                if (page_size != 0)
                {
                    versioned_pageid_entries.emplace_back(
                        std::make_pair(page_id, std::move(versioned_entries)));
                    total_page_size += page_size;
                }
            }

            if (versioned_pageid_entries.empty())
            {
                throw Exception(fmt::format("Can't get any entries from [BlobFileId={}]", blob_id));
            }

            blob_versioned_entries[blob_id] = std::move(versioned_pageid_entries);
        }
    }
    return std::make_pair(std::move(blob_versioned_entries), total_page_size);
}

std::vector<PageEntriesV3> PageDirectory::gc()
{
    UInt64 lowest_seq = sequence.load();
    std::vector<PageEntriesV3> all_del_entries;

    // Cleanup released snapshots
    for (auto iter = snapshots.begin(); iter != snapshots.end(); /* empty */)
    {
        if (iter->expired())
            iter = snapshots.erase(iter);
        else
        {
            lowest_seq = std::min(lowest_seq, iter->lock()->sequence);
            ++iter;
        }
    }
    {
        std::unique_lock write_lock(table_rw_mutex);
        for (auto iter = mvcc_table_directory.begin(); iter != mvcc_table_directory.end(); /*empty*/)
        {
            const auto & [del_entries, all_deleted] = iter->second->deleteAndGC(lowest_seq);
            if (!del_entries.empty())
            {
                all_del_entries.emplace_back(std::move(del_entries));
            }

            if (all_deleted)
            {
                iter = mvcc_table_directory.erase(iter);
            }
            else
            {
                iter++;
            }
        }
    }

    return all_del_entries;
}

} // namespace PS::V3
} // namespace DB
