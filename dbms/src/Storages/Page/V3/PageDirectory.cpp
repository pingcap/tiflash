#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/LogWithPrefix.h>
#include <Common/assert_cast.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/WAL/serialize.h>
#include <Storages/Page/V3/WALStore.h>
#include <Storages/Page/WriteBatch.h>
#include <common/logger_useful.h>

#include <memory>
#include <mutex>
#include <type_traits>
#include <utility>

#ifdef FIU_ENABLE
#include <Common/randomSeed.h>

#include <pcg_random.hpp>
#include <thread>
#endif

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
/********************************
 * VersionedPageEntries methods *
 ********************************/

std::optional<PageEntryV3> VersionedPageEntries::getEntryNotSafe(UInt64 seq) const
{
    // entries are sorted by <ver, epoch>, find the first one less than <ver+1, 0>
    if (auto iter = MapUtils::findLess(entries, PageVersionType(seq + 1));
        iter != entries.end())
    {
        if (!iter->second.is_delete)
            return iter->second.entry;
    }
    return std::nullopt;
}

std::optional<PageEntryV3> VersionedPageEntries::getEntry(UInt64 seq) const
{
    auto page_lock = acquireLock();
    return getEntryNotSafe(seq);
}

PageSize VersionedPageEntries::getEntriesByBlobIds(
    const std::unordered_set<BlobFileId> & blob_ids,
    PageIdV3Internal page_id,
    std::map<BlobFileId, PageIdAndVersionedEntries> & blob_versioned_entries)
{
    // blob_file_0, [<page_id_0, ver0, entry0>,
    //               <page_id_0, ver1, entry1>,
    //               <page_id_1, ver1, entry1> ]
    // blob_file_1, [...]
    // ...
    // the total entries size taken out
    PageSize total_entries_size = 0;
    auto page_lock = acquireLock();
    for (const auto & [versioned_type, entry_or_del] : entries)
    {
        if (entry_or_del.is_delete)
        {
            continue;
        }

        const auto & entry = entry_or_del.entry;
        if (blob_ids.count(entry.file_id) > 0)
        {
            blob_versioned_entries[entry.file_id].emplace_back(page_id, versioned_type, entry);
            total_entries_size += entry.size;
        }
    }
    return total_entries_size;
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

/**********************************
  * CollapsingPageDirectory methods *
  *********************************/

CollapsingPageDirectory::CollapsingPageDirectory() = default;

void CollapsingPageDirectory::apply(PageEntriesEdit && edit)
{
    for (const auto & record : edit.getRecords())
    {
        if (max_applied_ver < record.version)
        {
            max_applied_ver = record.version;
        }
        max_applied_page_id = std::max(record.page_id, max_applied_page_id);

        switch (record.type)
        {
        case WriteBatch::WriteType::PUT:
            [[fallthrough]];
        case WriteBatch::WriteType::UPSERT:
        {
            // Insert or replace with the latest entry
            if (auto iter = table_directory.find(record.page_id); iter == table_directory.end())
            {
                table_directory[record.page_id] = std::make_pair(record.version, record.entry);
            }
            else
            {
                const auto & [ver, entry] = iter->second;
                (void)entry;
                if (ver < record.version)
                {
                    iter->second = std::make_pair(record.version, record.entry);
                }
                // else just ignore outdate version
            }
            break;
        }
        case WriteBatch::WriteType::DEL:
        {
            // Remove the entry if the version of del is newer
            if (auto iter = table_directory.find(record.page_id); iter != table_directory.end())
            {
                const auto & [ver, entry] = iter->second;
                (void)entry;
                if (ver < record.version)
                {
                    table_directory.erase(iter);
                }
            }
            break;
        }
        default:
        {
            // REF is converted into `PUT` in serialized edit, so it should not run into here.
            throw Exception(fmt::format("Unknown write type: {}", record.type));
        }
        }
    }
}

void CollapsingPageDirectory::dumpTo(std::unique_ptr<LogWriter> & log_writer)
{
    PageEntriesEdit edit;
    for (const auto & [page_id, versioned_entry] : table_directory)
    {
        const auto & version = versioned_entry.first;
        const auto & entry = versioned_entry.second;
        edit.upsertPage(page_id, version, entry);
    }
    const String serialized = ser::serializeTo(edit);
    ReadBufferFromString payload(serialized);
    log_writer->addRecord(payload, serialized.size());
}

/**************************
  * PageDirectory methods *
  *************************/

PageDirectory::PageDirectory()
    : PageDirectory(0, nullptr)
{
}

PageDirectory::PageDirectory(UInt64 init_seq, WALStorePtr && wal_)
    : sequence(init_seq)
    , wal(std::move(wal_))
    , log(getLogWithPrefix(nullptr, "PageDirectory"))
{
}

PageDirectory PageDirectory::create(const CollapsingPageDirectory & collapsing_directory, WALStorePtr && wal)
{
    // Reset the `sequence` to the maximum of persisted.
    PageDirectory dir(
        /*init_seq=*/collapsing_directory.max_applied_ver.sequence,
        std::move(wal));
    for (const auto & [page_id, versioned_entry] : collapsing_directory.table_directory)
    {
        const auto & version = versioned_entry.first;
        const auto & entry = versioned_entry.second;
        auto [iter, created] = dir.mvcc_table_directory.insert(std::make_pair(page_id, nullptr));
        if (created)
            iter->second = std::make_shared<VersionedPageEntries>();
        iter->second->createNewVersion(version.sequence, version.epoch, entry);
    }
    return dir;
}

PageDirectorySnapshotPtr PageDirectory::createSnapshot() const
{
    auto snap = std::make_shared<PageDirectorySnapshot>(sequence.load());
    {
        std::lock_guard snapshots_lock(snapshots_mutex);
        snapshots.emplace_back(std::weak_ptr<PageDirectorySnapshot>(snap));
    }

    CurrentMetrics::add(CurrentMetrics::PSMVCCSnapshotsList);
    return snap;
}


std::tuple<size_t, double, unsigned> PageDirectory::getSnapshotsStat() const
{
    double longest_living_seconds = 0.0;
    unsigned longest_living_from_thread_id = 0;
    DB::Int64 num_snapshots_removed = 0;
    size_t num_valid_snapshots = 0;
    {
        std::lock_guard lock(snapshots_mutex);
        for (auto iter = snapshots.begin(); iter != snapshots.end(); /* empty */)
        {
            auto snapshot_ptr = iter->lock();
            if (!snapshot_ptr)
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

                const auto snapshot_lifetime = snapshot_ptr->elapsedSeconds();
                if (snapshot_lifetime > longest_living_seconds)
                {
                    longest_living_seconds = snapshot_lifetime;
                    longest_living_from_thread_id = snapshot_ptr->getTid();
                }
                num_valid_snapshots++;
                ++iter;
            }
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

PageIDAndEntryV3 PageDirectory::get(PageIdV3Internal page_id, const DB::PageStorageSnapshotPtr & snap) const
{
    return get(page_id, toConcreteSnapshot(snap));
}

PageIDAndEntryV3 PageDirectory::get(PageIdV3Internal page_id, const PageDirectorySnapshotPtr & snap) const
{
    MVCCMapType::const_iterator iter;
    {
        std::shared_lock read_lock(table_rw_mutex);
        iter = mvcc_table_directory.find(page_id);
        if (iter == mvcc_table_directory.end())
            throw Exception(fmt::format("Entry [ns_id={} p_id={}] not exist!", page_id.high, page_id.low), ErrorCodes::PS_ENTRY_NOT_EXISTS);
    }

    if (auto entry = iter->second->getEntry(snap->sequence); entry)
    {
        return PageIDAndEntryV3(page_id, *entry);
    }

    throw Exception(fmt::format("Entry [ns_id={} p_id={}] [seq={}] not exist!", page_id.high, page_id.low, snap->sequence), ErrorCodes::PS_ENTRY_NO_VALID_VERSION);
}

PageIDAndEntriesV3 PageDirectory::get(const PageIdV3Internals & page_ids, const DB::PageStorageSnapshotPtr & snap) const
{
    return get(page_ids, toConcreteSnapshot(snap));
}

PageIDAndEntriesV3 PageDirectory::get(const PageIdV3Internals & page_ids, const PageDirectorySnapshotPtr & snap) const
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
                throw Exception(fmt::format("Entry [ns_id={} p_id={}] at [idx={}] not exist!", page_ids[idx].high, page_ids[idx].low, idx), ErrorCodes::PS_ENTRY_NOT_EXISTS);
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
            throw Exception(fmt::format("Entry [ns_id={} p_id={}] [seq={}] at [idx={}] not exist!", page_ids[idx].high, page_ids[idx].low, snap->sequence, idx), ErrorCodes::PS_ENTRY_NO_VALID_VERSION);
    }

    return id_entries;
}

PageIdV3Internal PageDirectory::getMaxIdWithinUpperBound(PageIdV3Internal upper_bound) const
{
    std::shared_lock read_lock(table_rw_mutex);

    auto iter = mvcc_table_directory.upper_bound(upper_bound);
    if (iter == mvcc_table_directory.begin())
    {
        // The smallest page id is greater than the target page id or mvcc_table_directory is empty,
        // and it means no page id is less than or equal to the target page id, return 0.
        return PageIdV3Internal(0);
    }
    else
    {
        // iter is not at the beginning and mvcc_table_directory is not empty,
        // so iter-- must be a valid iterator, and it's the largest page id which is smaller than the target page id.
        iter--;
        return iter->first;
    }
}

std::set<PageIdV3Internal> PageDirectory::getAllPageIds()
{
    std::set<PageIdV3Internal> page_ids;
    std::shared_lock read_lock(table_rw_mutex);

    for (auto & [page_id, versioned] : mvcc_table_directory)
    {
        (void)versioned;
        page_ids.insert(page_id);
    }
    return page_ids;
}

void PageDirectory::apply(PageEntriesEdit && edit, const WriteLimiterPtr & write_limiter)
{
    // Note that we need to make sure increasing `sequence` in order, so it
    // also needs to be protected by `write_lock` throughout the `apply`
    // TODO: It is totally serialized, make it a pipeline
    std::unique_lock write_lock(table_rw_mutex);
    UInt64 last_sequence = sequence.load();

    // stage 1, persisted the changes to WAL with version [seq=last_seq + 1, epoch=0]
    wal->apply(edit, PageVersionType(last_sequence + 1, 0), write_limiter);

    // stage 2, create entry version list for pageId. nothing need to be rollback
    std::unordered_map<PageIdV3Internal, std::pair<PageLock, int>> updating_locks;
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
        case WriteBatch::WriteType::PUT_EXTERNAL:
        {
            throw Exception("Not implemented");
        }
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
                LOG_FMT_WARNING(log, "Trying to add ref from {}.{} to non-exist {}.{} with sequence={}", r.page_id.high, r.page_id.low, r.ori_page_id.high, r.ori_page_id.low, last_sequence + 1);
                break;
            }

            // If we already hold the lock from `r.ori_page_id`, Then we should not request it again.
            // This can happen when r.ori_page_id have other operating in current writebatch
            if (auto entry = (updating_locks.find(r.ori_page_id) != updating_locks.end()
                                  ? iter->second->getEntryNotSafe(last_sequence + 1)
                                  : iter->second->getEntry(last_sequence + 1));
                entry)
            {
                // copy the entry to be ref
                updating_pages[idx]->createNewVersion(last_sequence + 1, *entry);
            }
            else
            {
                LOG_FMT_WARNING(log, "Trying to add ref from {}.{} to non-exist {}.{} with sequence={}", r.page_id.high, r.page_id.low, r.ori_page_id.high, r.ori_page_id.low, last_sequence + 1);
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

std::set<PageIdV3Internal> PageDirectory::gcApply(PageEntriesEdit && migrated_edit, bool need_scan_page_ids, const WriteLimiterPtr & write_limiter)
{
    for (auto & record : migrated_edit.getMutRecords())
    {
        MVCCMapType::const_iterator iter;
        {
            std::shared_lock read_lock(table_rw_mutex);
            iter = mvcc_table_directory.find(record.page_id);
            if (unlikely(iter == mvcc_table_directory.end()))
            {
                throw Exception(fmt::format("Can't found [ns_id={} page_id={}] while doing gcApply", record.page_id.high, record.page_id.low), ErrorCodes::LOGICAL_ERROR);
            }
        } // release the read lock on `table_rw_mutex`

        // Increase the epoch for migrated record.
        record.version.epoch += 1;

        // Append the gc version to version list
        const auto & versioned_entries = iter->second;
        auto page_lock = versioned_entries->acquireLock();
        versioned_entries->createNewVersion(record.version.sequence, record.version.epoch, record.entry);
    }

    // Apply migrate edit into WAL with the increased epoch version
    wal->apply(migrated_edit, write_limiter);

    if (!need_scan_page_ids)
    {
        return {};
    }

    return getAllPageIds();
}

std::pair<std::map<BlobFileId, PageIdAndVersionedEntries>, PageSize>
PageDirectory::getEntriesByBlobIds(const std::vector<BlobFileId> & blob_ids) const
{
    std::unordered_set<BlobFileId> blob_id_set;
    for (const auto blob_id : blob_ids)
        blob_id_set.insert(blob_id);
    assert(blob_id_set.size() == blob_ids.size());

    std::map<BlobFileId, PageIdAndVersionedEntries> blob_versioned_entries;
    PageSize total_page_size = 0;

    MVCCMapType::const_iterator iter;
    {
        std::shared_lock read_lock(table_rw_mutex);
        iter = mvcc_table_directory.cbegin();
        if (iter == mvcc_table_directory.end())
            return {blob_versioned_entries, total_page_size};
    }

    while (true)
    {
        // `iter` is an iter that won't be invalid cause by `apply`/`gcApply`.
        // do scan on the version list without lock on `mvcc_table_directory`.
        auto page_id = iter->first;
        const auto & version_entries = iter->second;
        total_page_size += version_entries->getEntriesByBlobIds(blob_id_set, page_id, blob_versioned_entries);

        {
            std::shared_lock read_lock(table_rw_mutex);
            iter++;
            if (iter == mvcc_table_directory.end())
                break;
        }
    }
    for (const auto blob_id : blob_ids)
    {
        if (blob_versioned_entries.find(blob_id) == blob_versioned_entries.end())
        {
            throw Exception(fmt::format("Can't get any entries from [blob_id={}]", blob_id));
        }
    }
    return std::make_pair(std::move(blob_versioned_entries), total_page_size);
}


std::vector<PageEntriesV3> PageDirectory::gc(const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter)
{
    [[maybe_unused]] bool done_anything = false;
    UInt64 lowest_seq = sequence.load();

    done_anything |= wal->compactLogs(write_limiter, read_limiter);

    {
        // Cleanup released snapshots
        std::lock_guard lock(snapshots_mutex);
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
    }

    std::vector<PageEntriesV3> all_del_entries;
    MVCCMapType::iterator iter;
    {
        std::shared_lock read_lock(table_rw_mutex);
        iter = mvcc_table_directory.begin();
        if (iter == mvcc_table_directory.end())
            return all_del_entries;
    }

    while (true)
    {
        // `iter` is an iter that won't be invalid cause by `apply`/`gcApply`.
        // do gc on the version list without lock on `mvcc_table_directory`.
        const auto & [del_entries, all_deleted] = iter->second->deleteAndGC(lowest_seq);
        if (!del_entries.empty())
        {
            all_del_entries.emplace_back(std::move(del_entries));
        }

        {
            std::unique_lock write_lock(table_rw_mutex);
            if (all_deleted)
            {
                iter = mvcc_table_directory.erase(iter);
            }
            else
            {
                iter++;
            }

            if (iter == mvcc_table_directory.end())
                break;
        }
    }

    return all_del_entries;
}

} // namespace PS::V3
} // namespace DB
