#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/LogWithPrefix.h>
#include <Common/assert_cast.h>
#include <Storages/Page/V3/MapUtils.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/WAL/serialize.h>
#include <Storages/Page/V3/WALStore.h>
#include <Storages/Page/WriteBatch.h>
#include <common/logger_useful.h>
#include <openssl/base64.h>

#include <memory>
#include <mutex>
#include <optional>
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

void VersionedPageEntries::createNewVersion(UInt64 seq, UInt64 epoch, const PageEntryV3 & entry)
{
    auto page_lock = acquireLock();
    if (entries.empty())
    {
        entries.emplace(PageVersionType(seq, epoch), EntryOrDelete::newNormalEntry(entry));
        return;
    }

    auto last_entry = entries.rbegin();
    if (last_entry->second.isDelete())
    {
        entries.emplace(PageVersionType(seq, epoch), EntryOrDelete::newNormalEntry(entry));
    }
    else if (last_entry->second.isNormal())
    {
        if (last_entry->second.being_ref_count != 1 && seq > last_entry->first.sequence)
        {
            throw Exception(fmt::format(
                "Try to replace normal entry with an newer version [seq={}] [prev_seq={}] [lady_entry={}]",
                seq,
                last_entry->first.sequence,
                last_entry->second.toDebugString()));
        }
        // inherit the `being_ref_count` of the last entry
        entries.emplace(PageVersionType(seq, epoch), EntryOrDelete::newRepalcingEntry(last_entry->second, entry));
    }
    else if (last_entry->second.isRef())
        throw Exception(fmt::format("Try to create new normal entry on an ref page"));
    else if (last_entry->second.isExternal())
        throw Exception(fmt::format("Try to create new normal entry on an ext page"));
}

bool VersionedPageEntries::createNewRefVersion(UInt64 seq, PageId ori_page_id)
{
    auto page_lock = acquireLock();
    if (entries.empty())
    {
        entries.emplace(PageVersionType(seq), EntryOrDelete::newRefEntry(ori_page_id));
        return true;
    }

    // adding ref to the same ori id should be idempotent
    // adding ref after deleted should be ok
    // adding ref to replace put/external is not allowed
    auto last_iter = entries.rbegin();
    if (last_iter->second.isRef())
    {
        // duplicated ref pair, just ignore
        if (last_iter->second.origin_page_id == ori_page_id)
        {
            return false;
        }

        throw Exception(fmt::format(
            "try to create new ref version on a id that already has ref to another id "
            "[seq={}] [ori_page_id={}] [last_ver={}] [last_entry={}]",
            seq,
            ori_page_id,
            last_iter->first,
            last_iter->second.toDebugString()));
    }
    else if (last_iter->second.isDelete())
    {
        // apply ref on a deleted page, ok
        entries.emplace(PageVersionType(seq), EntryOrDelete::newRefEntry(ori_page_id));
        return true;
    }

    throw Exception(fmt::format(
        "try to create new ref version on an id that already has entries "
        "[seq={}] [ori_page_id={}] [last_ver={}] [last_entry={}]",
        seq,
        ori_page_id,
        last_iter->first,
        last_iter->second.toDebugString()));
}

std::tuple<VersionedPageEntries::ResolveResult, PageId, PageVersionType>
VersionedPageEntries::resolveToPageId(UInt64 seq)
{
    auto page_lock = acquireLock();
    // entries are sorted by <ver, epoch>, increase the ref count of the first one less than <ver+1, 0>
    if (auto iter = MapUtils::findMutLess(entries, PageVersionType(seq + 1));
        iter != entries.end())
    {
        if (iter->second.isNormal())
            return {RESOLVE_TO_NORMAL, 0, PageVersionType(0)};
        else if (iter->second.isRef())
            return {RESOLVE_TO_REF, iter->second.origin_page_id, iter->first};
    }
    return {RESOLVE_FAIL, 0, PageVersionType(0)};
}

std::optional<PageEntryV3> VersionedPageEntries::getEntry(UInt64 seq) const
{
    auto page_lock = acquireLock();
    // entries are sorted by <ver, epoch>, find the first one less than <ver+1, 0>
    if (auto iter = MapUtils::findLess(entries, PageVersionType(seq + 1));
        iter != entries.end())
    {
        // NORMAL
        if (iter->second.isNormal())
            return iter->second.entry;
    }
    return std::nullopt;
}

Int64 VersionedPageEntries::incrRefCount(const PageVersionType & ver)
{
    auto page_lock = acquireLock();
    if (auto iter = MapUtils::findMutLess(entries, PageVersionType(ver.sequence + 1));
        iter != entries.end())
    {
        if (iter->second.isNormal())
            return ++iter->second.being_ref_count;
        else
            throw Exception(fmt::format("The entry to be added ref count is not normal entry [entry={}] [ver={}]", iter->second.toDebugString(), ver));
    }
    throw Exception(fmt::format("The entry to be added ref count is not found [ver={}]", ver));
}

PageSize VersionedPageEntries::getEntriesByBlobIds(
    const std::unordered_set<BlobFileId> & blob_ids,
    PageId page_id,
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
        if (!entry_or_del.isNormal())
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

std::pair<PageEntriesV3, bool> VersionedPageEntries::cleanOutdatedEntries(
    UInt64 lowest_seq,
    PageId page_id,
    std::map<PageId, std::pair<PageVersionType, Int64>> * normal_entries_to_deref,
    const PageLock & /*page_lock*/)
{
    PageEntriesV3 outdated_entries;

    if (entries.empty())
    {
        return std::make_pair(outdated_entries, true);
    }

    auto iter = MapUtils::findLess(entries, PageVersionType(lowest_seq + 1));
    // If we can't find any seq lower than `lowest_seq` then
    // all version in this list don't need gc.
    if (iter == entries.begin() || iter == entries.end())
    {
        return std::make_pair(outdated_entries, false);
    }

    bool keep_if_being_ref = !(iter->second.isNormal() || iter->second.isExternal());
    --iter; // keep the first version less than <lowest_seq+1, 0>
    while (true)
    {
        if (iter->second.isDelete())
        {
            // Already deleted, no need put in `del_entries`
            iter = entries.erase(iter);
        }
        else if (iter->second.isRef())
        {
            if (normal_entries_to_deref != nullptr)
            {
                // need to decrease the ref count by <ver=iter->first, ori_page_id=iter->second.origin_page_id;>
                if (auto [deref_counter, new_created] = normal_entries_to_deref->emplace(std::make_pair(iter->second.origin_page_id, std::make_pair(/*ver=*/iter->first, /*count=*/1))); !new_created)
                {
                    if (deref_counter->second.first.sequence != iter->first.epoch)
                    {
                        throw Exception(fmt::format(
                            "There exist two different version of ref, should not happen [page_id={}] [ori_page_id={}] [ver={}] [another_ver={}]",
                            page_id,
                            iter->second.origin_page_id,
                            iter->first,
                            deref_counter->second.first));
                    }
                    deref_counter->second.second += 1;
                }
                iter = entries.erase(iter);
            }
        }
        else if (iter->second.isNormal() || iter->second.isExternal())
        {
            if (keep_if_being_ref)
            {
                if (iter->second.being_ref_count == 1)
                {
                    outdated_entries.emplace_back(iter->second.entry);
                    iter = entries.erase(iter);
                }
                // The `being_ref_count` for this version is valid. While for older versions,
                // theirs `being_ref_count` is invalid, don't need to be kept
                keep_if_being_ref = false;
            }
            else
            {
                outdated_entries.emplace_back(iter->second.entry);
                iter = entries.erase(iter);
            }
        }

        if (iter == entries.begin())
            break;
        --iter;
    }

    return std::make_pair(std::move(outdated_entries), entries.empty() || (entries.size() == 1 && entries.begin()->second.isDelete()));
}

std::pair<PageEntriesV3, bool> VersionedPageEntries::derefAndClean(UInt64 lowest_seq, PageId page_id, const PageVersionType & deref_ver, const Int64 deref_count)
{
    auto page_lock = acquireLock();

    {
        // decrease the ref-counter
        auto iter = MapUtils::findMutLess(entries, PageVersionType(deref_ver.sequence + 1, 0));
        if (iter == entries.end())
        {
            throw Exception(fmt::format("Can not find entry for decreasing ref count [page_id={}] [ver={}] [deref_count={}]", page_id, deref_ver, deref_count));
        }
        if (iter->second.being_ref_count <= deref_count)
        {
            throw Exception(fmt::format("Decreasing ref count error [page_id={}] [ver={}] [deref_count={}] [entry={}]", page_id, deref_ver, deref_count, iter->second.toDebugString()));
        }
        iter->second.being_ref_count -= deref_count;
    }

    return cleanOutdatedEntries(lowest_seq, page_id, nullptr, page_lock);
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
        case EditRecordType::PUT:
            [[fallthrough]];
        case EditRecordType::UPSERT:
        {
            // Insert or replace with latest entry
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
        case EditRecordType::DEL:
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
    PageId max_page_id = 0;
    std::shared_lock read_lock(table_rw_mutex);

    for (const auto & [page_id, versioned] : mvcc_table_directory)
    {
        (void)versioned;
        max_page_id = std::max(max_page_id, page_id);
    }
    return max_page_id;
}

std::set<PageId> PageDirectory::getAllPageIds()
{
    std::set<PageId> page_ids;
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
    for (const auto & r : edit.getRecords())
    {
        auto [iter, created] = mvcc_table_directory.insert(std::make_pair(r.page_id, nullptr));
        if (created)
        {
            iter->second = std::make_shared<VersionedPageEntries>();
        }

        auto & version_list = iter->second;
        if (r.type == EditRecordType::REF)
        {
            // applying ref 3->2, existing ref 2->1, normal entry 1, then we should collapse
            // the ref to be 3->1, increase the refcounting of normale entry 1
            auto [resolve_success, resolved_id, resolved_ver] = [this](PageId id_to_resolve, PageVersionType ver_to_resolve)
                -> std::tuple<bool, PageId, PageVersionType> {
                while (true)
                {
                    auto resolve_ver_iter = mvcc_table_directory.find(id_to_resolve);
                    if (resolve_ver_iter == mvcc_table_directory.end())
                        return {false, 0, PageVersionType(0)};

                    const VersionedPageEntriesPtr & resolve_version_list = resolve_ver_iter->second;
                    // If we already hold the lock from `id_to_resolve`, then we should not request it again.
                    // This can happen when `id_to_resolve` have other operating in current writebatch
                    auto resolve_page_lock = resolve_version_list->acquireLock();
                    auto [need_collapse, next_id_to_resolve, next_ver_to_resolve] = resolve_version_list->resolveToPageId(ver_to_resolve.sequence);
                    switch (need_collapse)
                    {
                    case VersionedPageEntries::RESOLVE_FAIL:
                        return {false, id_to_resolve, ver_to_resolve};
                    case VersionedPageEntries::RESOLVE_TO_NORMAL:
                        return {true, id_to_resolve, ver_to_resolve};
                    case VersionedPageEntries::RESOLVE_TO_REF:
                        if (id_to_resolve == next_id_to_resolve)
                        {
                            return {false, next_id_to_resolve, next_ver_to_resolve};
                        }
                        id_to_resolve = next_id_to_resolve;
                        ver_to_resolve = next_ver_to_resolve;
                        break; // continue the resolving
                    }
                }
            }(r.ori_page_id, PageVersionType(last_sequence + 1));
            if (!resolve_success)
            {
                throw Exception(fmt::format(
                    "Trying to add ref to non-exist page [page_id={}] [ori_id={}] [seq={}] [resolve_id={}] [resolve_ver={}]",
                    r.page_id,
                    r.ori_page_id,
                    last_sequence + 1,
                    resolved_id,
                    resolved_ver));
            }
            // use the resolved_id to collapse ref chain 3->2, 2->1 ==> 3->1
            bool is_ref_created = version_list->createNewRefVersion(last_sequence + 1, resolved_id);

            if (is_ref_created)
            {
                // Add the ref-count of being-ref entry
                if (auto resolved_iter = mvcc_table_directory.find(resolved_id); resolved_iter != mvcc_table_directory.end())
                {
                    resolved_iter->second->incrRefCount(resolved_ver);
                }
                else
                {
                    throw Exception(fmt::format(
                        "The ori page id is not found [page_id={}] [ori_id={}] [seq={}] [resolved_id={}] [resolved_ver={}]",
                        r.page_id,
                        r.ori_page_id,
                        last_sequence + 1,
                        resolved_id,
                        resolved_ver));
                }
            }

            continue; // continue to process next record in `edit`
        }

        switch (r.type)
        {
        case EditRecordType::PUT_EXTERNAL:
        {
            throw Exception("Not implemented");
        }
        case EditRecordType::PUT:
            [[fallthrough]];
        case EditRecordType::UPSERT:
        {
            try
            {
                version_list->createNewVersion(last_sequence + 1, r.entry);
            }
            catch (DB::Exception & e)
            {
                e.addMessage(fmt::format(" [page_id={}] [seq={}]", r.page_id, last_sequence + 1));
                throw e;
            }
            break;
        }
        case EditRecordType::DEL:
        {
            version_list->createDelete(last_sequence + 1);
            break;
        }
        case EditRecordType::REF:
            throw Exception(fmt::format("should handle ref before switch statement"));
        }
    }

    // stage 3, the edit committed, incr the sequence number to publish changes for `createSnapshot`
    sequence.fetch_add(1);
}

std::set<PageId> PageDirectory::gcApply(PageEntriesEdit && migrated_edit, bool need_scan_page_ids, const WriteLimiterPtr & write_limiter)
{
    for (auto & record : migrated_edit.getMutRecords())
    {
        MVCCMapType::const_iterator iter;
        {
            std::shared_lock read_lock(table_rw_mutex);
            iter = mvcc_table_directory.find(record.page_id);
            if (unlikely(iter == mvcc_table_directory.end()))
            {
                throw Exception(fmt::format("Can't found [page_id={}] while doing gcApply", record.page_id), ErrorCodes::LOGICAL_ERROR);
            }
        } // release the read lock on `table_rw_mutex`

        // Increase the epoch for migrated record.
        record.version.epoch += 1;

        // Append the gc version to version list
        const auto & versioned_entries = iter->second;
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
        PageId page_id = iter->first;
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

    std::map<PageId, std::pair<PageVersionType, Int64>> normal_entries_to_deref;
    while (true)
    {
        // `iter` is an iter that won't be invalid cause by `apply`/`gcApply`.
        // do gc on the version list without lock on `mvcc_table_directory`.
        const auto & [del_entries, all_deleted] = iter->second->cleanOutdatedEntries(
            lowest_seq,
            /*page_id=*/iter->first,
            &normal_entries_to_deref,
            iter->second->acquireLock());
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

    for (const auto & [page_id, deref_counter] : normal_entries_to_deref)
    {
        MVCCMapType::iterator iter;
        {
            std::shared_lock read_lock(table_rw_mutex);
            iter = mvcc_table_directory.find(page_id);
            if (iter == mvcc_table_directory.end())
                continue;
        }

        const auto & [del_entries, all_deleted] = iter->second->derefAndClean(lowest_seq, page_id, deref_counter.first, deref_counter.second);
        if (!del_entries.empty())
        {
            all_del_entries.emplace_back(std::move(del_entries));
        }

        if (all_deleted)
        {
            std::unique_lock write_lock(table_rw_mutex);
            mvcc_table_directory.erase(iter);
        }
    }

    return all_del_entries;
}

} // namespace PS::V3
} // namespace DB
