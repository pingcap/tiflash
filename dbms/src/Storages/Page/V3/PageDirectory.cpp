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

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Common/SyncPoint/SyncPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Common/assert_cast.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V3/MapUtils.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/WAL/WALReader.h>
#include <Storages/Page/V3/WAL/serialize.h>
#include <Storages/Page/V3/WALStore.h>
#include <Storages/Page/WriteBatch.h>
#include <common/logger_useful.h>

#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <type_traits>
#include <utility>


#ifdef FIU_ENABLE
#include <Common/randomSeed.h>

#include <pcg_random.hpp>
#include <thread>
#endif // FIU_ENABLE

namespace CurrentMetrics
{
extern const Metric PSMVCCSnapshotsList;
} // namespace CurrentMetrics

namespace DB
{
namespace FailPoints
{
extern const char random_slow_page_storage_remove_expired_snapshots[];
extern const char pause_before_full_gc_prepare[];
} // namespace FailPoints

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int PS_ENTRY_NOT_EXISTS;
extern const int PS_ENTRY_NO_VALID_VERSION;
extern const int PS_DIR_APPLY_INVALID_STATUS;
} // namespace ErrorCodes

namespace PS::V3
{
/********************************
 * VersionedPageEntries methods *
 ********************************/

void VersionedPageEntries::createNewEntry(const PageVersion & ver, const PageEntryV3 & entry)
{
    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_DELETE)
    {
        type = EditRecordType::VAR_ENTRY;
        assert(entries.empty());
        entries.emplace(ver, EntryOrDelete::newNormalEntry(entry));
        return;
    }

    if (type == EditRecordType::VAR_ENTRY)
    {
        auto last_iter = MapUtils::findLess(entries, PageVersion(ver.sequence + 1, 0));
        if (last_iter == entries.end())
        {
            entries.emplace(ver, EntryOrDelete::newNormalEntry(entry));
        }
        else if (last_iter->second.isDelete())
        {
            entries.emplace(ver, EntryOrDelete::newNormalEntry(entry));
        }
        else
        {
            assert(last_iter->second.isEntry());
            // It is ok to replace the entry with same sequence and newer epoch, but not valid
            // to replace the entry with newer sequence.
            if (unlikely(last_iter->second.being_ref_count != 1 && last_iter->first.sequence < ver.sequence))
            {
                throw Exception(
                    fmt::format("Try to replace normal entry with an newer seq [ver={}] [prev_ver={}] [last_entry={}]",
                                ver,
                                last_iter->first,
                                last_iter->second.toDebugString()),
                    ErrorCodes::LOGICAL_ERROR);
            }
            // create a new version that inherit the `being_ref_count` of the last entry
            entries.emplace(ver, EntryOrDelete::newReplacingEntry(last_iter->second, entry));
        }
        return;
    }

    throw Exception(
        fmt::format("try to create entry version with invalid state "
                    "[ver={}] [entry={}] [state={}]",
                    ver,
                    ::DB::PS::V3::toDebugString(entry),
                    toDebugString()),
        ErrorCodes::PS_DIR_APPLY_INVALID_STATUS);
}

PageIdV3Internal VersionedPageEntries::createUpsertEntry(const PageVersion & ver, const PageEntryV3 & entry)
{
    auto page_lock = acquireLock();

    // For applying upsert entry, only `VAR_ENTRY`/`VAR_REF` is valid state.

    if (type == EditRecordType::VAR_ENTRY)
    {
        auto last_iter = MapUtils::findLess(entries, PageVersion(ver.sequence + 1, 0));
        if (last_iter == entries.end())
        {
            entries.emplace(ver, EntryOrDelete::newNormalEntry(entry));
        }
        else if (last_iter->second.isDelete())
        {
            // append after delete
            entries.emplace(ver, EntryOrDelete::newNormalEntry(entry));
        }
        else
        {
            assert(last_iter->second.isEntry());
            // It is ok to replace the entry with same sequence and newer epoch, but not valid
            // to replace the entry with newer sequence.
            if (unlikely(last_iter->second.being_ref_count != 1 && last_iter->first.sequence < ver.sequence))
            {
                throw Exception(
                    fmt::format("Try to replace normal entry with an newer seq [ver={}] [prev_ver={}] [last_entry={}]",
                                ver,
                                last_iter->first,
                                last_iter->second.toDebugString()),
                    ErrorCodes::LOGICAL_ERROR);
            }
            // create a new version that inherit the `being_ref_count` of the last entry
            entries.emplace(ver, EntryOrDelete::newReplacingEntry(last_iter->second, entry));
        }
        return buildV3Id(0, INVALID_PAGE_ID);
    }

    if (type == EditRecordType::VAR_REF)
    {
        // an ref-page is rewritten into a normal page
        if (!is_deleted)
        {
            // Full GC has rewritten new data on disk, we need to update this RefPage
            // to be a normal page with the upsert-entry.
            entries.emplace(ver, EntryOrDelete::newNormalEntry(entry));
            is_deleted = false;
            type = EditRecordType::VAR_ENTRY;
            // Also we need to decrease the ref-count of ori_page_id.
            return ori_page_id;
        }
        else
        {
            // The ref-id is deleted before full gc commit, but the data is
            // rewritten into `entry`. We need to update this RefPage to be a
            // be normal page with upsert-entry and a delete. Then later GC will
            // remove the useless data on `entry`.
            entries.emplace(ver, EntryOrDelete::newNormalEntry(entry));
            entries.emplace(delete_ver, EntryOrDelete::newDelete());
            is_deleted = false;
            type = EditRecordType::VAR_ENTRY;
            // Though the ref-id is marked as deleted, but the ref-count of
            // ori_page_id is not decreased. Return the ori_page_id
            // for decreasing ref-count.
            return ori_page_id;
        }
    }

    throw Exception(
        fmt::format("try to create upsert entry version with invalid state "
                    "[ver={}] [entry={}] [state={}]",
                    ver,
                    ::DB::PS::V3::toDebugString(entry),
                    toDebugString()),
        ErrorCodes::PS_DIR_APPLY_INVALID_STATUS);
}

// Create a new external version with version=`ver`.
// If create success, then return a shared_ptr as a holder for page_id. The holder
// will be release when this external version is totally removed.
std::shared_ptr<PageIdV3Internal> VersionedPageEntries::createNewExternal(const PageVersion & ver)
{
    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_DELETE)
    {
        type = EditRecordType::VAR_EXTERNAL;
        is_deleted = false;
        create_ver = ver;
        delete_ver = PageVersion(0);
        being_ref_count = 1;
        // return the new created holder to caller to set the page_id
        external_holder = std::make_shared<PageIdV3Internal>(0, 0);
        return external_holder;
    }

    if (type == EditRecordType::VAR_EXTERNAL)
    {
        if (is_deleted)
        {
            // adding external after deleted should be ok
            if (delete_ver <= ver)
            {
                is_deleted = false;
                create_ver = ver;
                delete_ver = PageVersion(0);
                being_ref_count = 1;
                // return the new created holder to caller to set the page_id
                external_holder = std::make_shared<PageIdV3Internal>(0, 0);
                return external_holder;
            }
            else
            {
                // apply an external with smaller ver than delete_ver, just ignore
                return nullptr;
            }
        }
        else
        {
            // adding external should be idempotent, just ignore
            // don't care about the `ver`
            return nullptr;
        }
    }

    throw Exception(
        fmt::format("try to create external version with invalid state "
                    "[ver={}] [state={}]",
                    ver,
                    toDebugString()),
        ErrorCodes::PS_DIR_APPLY_INVALID_STATUS);
}

// Create a new delete version with version=`ver`.
void VersionedPageEntries::createDelete(const PageVersion & ver)
{
    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_ENTRY)
    {
        // ignore if the last item is already "delete"
        if (entries.empty() || !entries.rbegin()->second.isDelete())
        {
            entries.emplace(ver, EntryOrDelete::newDelete());
        }
        return;
    }

    if (type == EditRecordType::VAR_EXTERNAL || type == EditRecordType::VAR_REF)
    {
        is_deleted = true;
        delete_ver = ver;
        return;
    }

    if (type == EditRecordType::VAR_DELETE)
    {
        // just ignore
        return;
    }

    throw Exception(fmt::format(
        "try to create delete version with invalid state "
        "[ver={}] [state={}]",
        ver,
        toDebugString()));
}

// Create a new reference version with version=`ver` and `ori_page_id_`.
// If create success, then return true, otherwise return false.
bool VersionedPageEntries::createNewRef(const PageVersion & ver, PageIdV3Internal ori_page_id_)
{
    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_DELETE)
    {
        type = EditRecordType::VAR_REF;
        is_deleted = false;
        ori_page_id = ori_page_id_;
        create_ver = ver;
        return true;
    }

    if (type == EditRecordType::VAR_REF)
    {
        if (is_deleted)
        {
            // adding ref after deleted should be ok
            if (delete_ver <= ver)
            {
                ori_page_id = ori_page_id_;
                create_ver = ver;
                is_deleted = false;
                delete_ver = PageVersion(0);
                return true;
            }
            else if (ori_page_id == ori_page_id_)
            {
                // apply a ref to same ori id with small ver, just ignore
                return false;
            }
            // else adding ref to another ori id is not allow, just fallthrough
        }
        else
        {
            if (ori_page_id == ori_page_id_)
            {
                // adding ref to the same ori id should be idempotent, just ignore
                return false;
            }
            // else adding ref to another ori id is not allow, just fallthrough
        }
    }

    // adding ref to replace put/external is not allowed
    throw Exception(fmt::format(
                        "try to create ref version with invalid state "
                        "[ver={}] [ori_page_id={}] [state={}]",
                        ver,
                        ori_page_id_,
                        toDebugString()),
                    ErrorCodes::PS_DIR_APPLY_INVALID_STATUS);
}

std::shared_ptr<PageIdV3Internal> VersionedPageEntries::fromRestored(const PageEntriesEdit::EditRecord & rec)
{
    auto page_lock = acquireLock();
    switch (rec.type)
    {
    case EditRecordType::VAR_REF:
    {
        type = EditRecordType::VAR_REF;
        is_deleted = false;
        create_ver = rec.version;
        ori_page_id = rec.ori_page_id;
        return nullptr;
    }
    case EditRecordType::VAR_EXTERNAL:
    {
        type = EditRecordType::VAR_EXTERNAL;
        is_deleted = false;
        create_ver = rec.version;
        being_ref_count = rec.being_ref_count;
        external_holder = std::make_shared<PageIdV3Internal>(rec.page_id);
        return external_holder;
    }
    case EditRecordType::VAR_ENTRY:
    {
        type = EditRecordType::VAR_ENTRY;
        entries.emplace(rec.version, EntryOrDelete::newFromRestored(rec.entry, rec.being_ref_count));
        return nullptr;
    }
    default:
    {
        throw Exception(fmt::format("Calling VersionedPageEntries::fromRestored with unknown type: {}", static_cast<Int32>(rec.type)));
    }
    }
}

std::tuple<ResolveResult, PageIdV3Internal, PageVersion>
VersionedPageEntries::resolveToPageId(UInt64 seq, bool ignore_delete, PageEntryV3 * entry)
{
    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_ENTRY)
    {
        // entries are sorted by <ver, epoch>, find the first one less than <ver+1, 0>
        if (auto iter = MapUtils::findLess(entries, PageVersion(seq + 1));
            iter != entries.end())
        {
            if (!ignore_delete && iter->second.isDelete())
            {
                // the page is not visible
                return {ResolveResult::FAIL, buildV3Id(0, 0), PageVersion(0)};
            }

            // If `ignore_delete` is true, we need the page entry even if it is logical deleted.
            // Checkout the details in `PageDirectory::get`.

            // Ignore all "delete"
            while (iter != entries.begin() && iter->second.isDelete())
            {
                --iter;
            }
            // Then `iter` point to an entry or the `entries.begin()`, return if entry found
            if (iter->second.isEntry())
            {
                // copy and return the entry
                if (entry != nullptr)
                    *entry = iter->second.entry;
                return {ResolveResult::TO_NORMAL, buildV3Id(0, 0), PageVersion(0)};
            }
            // else fallthrough to FAIL
        } // else fallthrough to FAIL
    }
    else if (type == EditRecordType::VAR_EXTERNAL)
    {
        // If `ignore_delete` is true, we need the origin page id even if it is logical deleted.
        // Checkout the details in `PageDirectory::getNormalPageId`.
        bool ok = ignore_delete || (!is_deleted || seq < delete_ver.sequence);
        if (create_ver.sequence <= seq && ok)
        {
            return {ResolveResult::TO_NORMAL, buildV3Id(0, 0), PageVersion(0)};
        }
    }
    else if (type == EditRecordType::VAR_REF)
    {
        // Return the origin page id if this ref is visible by `seq`.
        if (create_ver.sequence <= seq && (!is_deleted || seq < delete_ver.sequence))
        {
            return {ResolveResult::TO_REF, ori_page_id, create_ver};
        }
    }
    else
    {
        LOG_WARNING(&Poco::Logger::get("VersionedPageEntries"), "Can't resolve the EditRecordType {}", static_cast<Int32>(type));
    }

    return {ResolveResult::FAIL, buildV3Id(0, 0), PageVersion(0)};
}

std::optional<PageEntryV3> VersionedPageEntries::getEntry(UInt64 seq) const
{
    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_ENTRY)
    {
        // entries are sorted by <ver, epoch>, find the first one less than <ver+1, 0>
        if (auto iter = MapUtils::findLess(entries, PageVersion(seq + 1));
            iter != entries.end())
        {
            // not deleted
            if (iter->second.isEntry())
                return iter->second.entry;
        }
    }
    return std::nullopt;
}

std::optional<PageEntryV3> VersionedPageEntries::getLastEntry(std::optional<UInt64> seq) const
{
    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_ENTRY)
    {
        for (auto it_r = entries.rbegin(); it_r != entries.rend(); it_r++)
        {
            if (seq.has_value() && it_r->first.sequence > seq.value())
                continue;
            if (it_r->second.isEntry())
            {
                return it_r->second.entry;
            }
        }
    }
    return std::nullopt;
}

// Returns true when **this id** is "visible" by `seq`.
// If this page id is marked as deleted or not created, it is "not visible".
// Note that not visible does not means this id can be GC.
bool VersionedPageEntries::isVisible(UInt64 seq) const
{
    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_DELETE)
    {
        return false;
    }
    else if (type == EditRecordType::VAR_ENTRY)
    {
        // entries are sorted by <ver, epoch>, find the first one less than <ver+1, 0>
        if (auto iter = MapUtils::findLess(entries, PageVersion(seq + 1));
            iter != entries.end())
        {
            // not deleted
            return iter->second.isEntry();
        }
        // else there are no valid entry less than seq
        return false;
    }
    else if (type == EditRecordType::VAR_EXTERNAL || type == EditRecordType::VAR_REF)
    {
        // `delete_ver` is only valid when `is_deleted == true`
        return create_ver.sequence <= seq && !(is_deleted && delete_ver.sequence <= seq);
    }

    throw Exception(fmt::format(
                        "calling isDeleted with invalid state "
                        "[seq={}] [state={}]",
                        seq,
                        toDebugString()),
                    ErrorCodes::LOGICAL_ERROR);
}

Int64 VersionedPageEntries::incrRefCount(const PageVersion & ver)
{
    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_ENTRY)
    {
        if (auto iter = MapUtils::findMutLess(entries, PageVersion(ver.sequence + 1));
            iter != entries.end())
        {
            // ignore all "delete"
            bool met_delete = false;
            while (iter != entries.begin() && iter->second.isDelete())
            {
                met_delete = true;
                --iter;
            }
            // Then `iter` point to an entry or the `entries.begin()`, return if entry found
            if (iter->second.isEntry())
            {
                if (unlikely(met_delete && iter->second.being_ref_count == 1))
                {
                    throw Exception(fmt::format("Try to add ref to a completely deleted entry [entry={}] [ver={}]", iter->second.toDebugString(), ver), ErrorCodes::LOGICAL_ERROR);
                }
                return ++iter->second.being_ref_count;
            }
        } // fallthrough to FAIL
    }
    else if (type == EditRecordType::VAR_EXTERNAL)
    {
        if (create_ver <= ver)
        {
            // We may add reference to an external id even if it is logically deleted.
            return ++being_ref_count;
        }
    }
    throw Exception(fmt::format("The entry to be added ref count is not found [ver={}] [state={}]", ver, toDebugString()), ErrorCodes::LOGICAL_ERROR);
}

PageSize VersionedPageEntries::getEntriesByBlobIds(
    const std::unordered_set<BlobFileId> & blob_ids,
    PageIdV3Internal page_id,
    std::map<BlobFileId, PageIdAndVersionedEntries> & blob_versioned_entries,
    std::map<PageIdV3Internal, std::tuple<PageIdV3Internal, PageVersion>> & ref_ids_maybe_rewrite)
{
    // `blob_versioned_entries`:
    // blob_file_0, [<page_id_0, ver0, entry0>,
    //               <page_id_1, ver1, entry1> ]
    // blob_file_1, [...]
    // ...

    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_REF)
    {
        // If the ref-id is not deleted, we will check whether its origin_entry.file_id in blob_ids
        if (!is_deleted)
        {
            ref_ids_maybe_rewrite[page_id] = {ori_page_id, create_ver};
        }
        return 0;
    }

    if (type != EditRecordType::VAR_ENTRY)
        return 0;

    assert(type == EditRecordType::VAR_ENTRY);
    // Empty or already deleted
    if (entries.empty())
        return 0;
    auto iter = entries.rbegin();
    if (iter->second.isDelete())
        return 0;

    // If `entry.file_id in blob_ids` we will rewrite this non-deleted page to a new location
    assert(iter->second.isEntry());
    // The total entries size that will be moved
    PageSize entry_size_full_gc = 0;
    const auto & last_entry = iter->second;
    if (blob_ids.count(last_entry.entry.file_id) > 0)
    {
        blob_versioned_entries[last_entry.entry.file_id].emplace_back(page_id, /* ver */ iter->first, last_entry.entry);
        entry_size_full_gc += last_entry.entry.size;
    }
    return entry_size_full_gc;
}

bool VersionedPageEntries::cleanOutdatedEntries(
    UInt64 lowest_seq,
    std::map<PageIdV3Internal, std::pair<PageVersion, Int64>> * normal_entries_to_deref,
    PageEntriesV3 * entries_removed,
    const PageLock & /*page_lock*/)
{
    if (type == EditRecordType::VAR_EXTERNAL)
    {
        return (being_ref_count == 1 && is_deleted && delete_ver.sequence <= lowest_seq);
    }
    else if (type == EditRecordType::VAR_REF)
    {
        // still visible by `lowest_seq`
        if (!is_deleted || lowest_seq < delete_ver.sequence)
            return false;
        // Else this ref page is safe to be deleted.
        if (normal_entries_to_deref != nullptr)
        {
            // need to decrease the ref count by <id=iter->second.origin_page_id, ver=iter->first, num=1>
            if (auto [deref_counter, new_created] = normal_entries_to_deref->emplace(std::make_pair(ori_page_id, std::make_pair(/*ver=*/create_ver, /*count=*/1))); !new_created)
            {
                // the id is already exist in deref map, increase the num to decrease ref count
                deref_counter->second.second += 1;
            }
        }
        return true;
    }
    else if (type == EditRecordType::VAR_DELETE)
    {
        return true;
    }
    else if (type != EditRecordType::VAR_ENTRY)
    {
        throw Exception(fmt::format("Invalid state {}", toDebugString()), ErrorCodes::LOGICAL_ERROR);
    }

    // type == EditRecordType::VAR_ENTRY
    assert(type == EditRecordType::VAR_ENTRY);
    if (entries.empty())
    {
        return true;
    }

    auto iter = MapUtils::findLess(entries, PageVersion(lowest_seq + 1));
    // If we can't find any seq lower than `lowest_seq` then
    // all version in this list don't need gc.
    if (iter == entries.begin() || iter == entries.end())
    {
        return false;
    }

    // If the first version less than <lowest_seq+1, 0> is entry,
    // then we can remove those entries prev of it.
    // If the first version less than <lowest_seq+1, 0> is delete,
    // we may keep the first valid entry before the delete entry
    // if `being_ref_count` > 1 (this means the entry is ref by other entries)
    bool last_entry_is_delete = !iter->second.isEntry();
    --iter; // keep the first version less than <lowest_seq+1, 0>
    while (true)
    {
        if (iter->second.isDelete())
        {
            // a useless version, simply drop it
            iter = entries.erase(iter);
        }
        else if (iter->second.isEntry())
        {
            if (last_entry_is_delete)
            {
                if (iter->second.being_ref_count == 1)
                {
                    if (entries_removed)
                    {
                        entries_removed->emplace_back(iter->second.entry);
                    }
                    iter = entries.erase(iter);
                }
                // The `being_ref_count` for this version is valid. While for older versions,
                // theirs `being_ref_count` is invalid, don't need to be kept
                last_entry_is_delete = false;
            }
            else
            {
                // else there are newer "entry" in the version list, the outdated entries should be removed
                if (entries_removed)
                {
                    entries_removed->emplace_back(iter->second.entry);
                }
                iter = entries.erase(iter);
            }
        }

        if (iter == entries.begin())
            break;
        --iter;
    }

    return entries.empty() || (entries.size() == 1 && entries.begin()->second.isDelete());
}

bool VersionedPageEntries::derefAndClean(UInt64 lowest_seq, PageIdV3Internal page_id, const PageVersion & deref_ver, const Int64 deref_count, PageEntriesV3 * entries_removed)
{
    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_EXTERNAL)
    {
        if (being_ref_count <= deref_count)
        {
            throw Exception(fmt::format("Decreasing ref count error [page_id={}] [ver={}] [deref_count={}]", page_id, deref_ver, deref_count));
        }
        being_ref_count -= deref_count;
        return (is_deleted && delete_ver.sequence <= lowest_seq && being_ref_count == 1);
    }
    else if (type == EditRecordType::VAR_ENTRY)
    {
        // Decrease the ref-counter. The entry may be moved to a newer entry with same sequence but higher epoch,
        // so we need to find the one less than <seq+1, 0> and decrease the ref-counter of it.
        auto iter = MapUtils::findMutLess(entries, PageVersion(deref_ver.sequence + 1, 0));
        if (iter == entries.end())
        {
            throw Exception(fmt::format("Can not find entry for decreasing ref count [page_id={}] [ver={}] [deref_count={}]", page_id, deref_ver, deref_count));
        }
        // ignore all "delete"
        while (iter != entries.begin() && iter->second.isDelete())
        {
            --iter; // move to the previous entry
        }
        // Then `iter` point to an entry or the `entries.begin()`
        if (iter->second.isDelete())
        {
            // run into the begin of `entries`, but still can not find a valid entry to decrease the ref-count
            throw Exception(fmt::format("Can not find entry for decreasing ref count till the begin [page_id={}] [ver={}] [deref_count={}]", page_id, deref_ver, deref_count));
        }
        assert(iter->second.isEntry());
        if (iter->second.being_ref_count <= deref_count)
        {
            throw Exception(fmt::format("Decreasing ref count error [page_id={}] [ver={}] [deref_count={}] [entry={}]", page_id, deref_ver, deref_count, iter->second.toDebugString()));
        }
        iter->second.being_ref_count -= deref_count;

        if (lowest_seq == 0)
            return false;
        // Clean outdated entries after decreased the ref-counter
        // set `normal_entries_to_deref` to be nullptr to ignore cleaning ref-var-entries
        return cleanOutdatedEntries(lowest_seq, /*normal_entries_to_deref*/ nullptr, entries_removed, page_lock);
    }

    throw Exception(fmt::format("calling derefAndClean with invalid state [state={}]", toDebugString()));
}

void VersionedPageEntries::collapseTo(const UInt64 seq, const PageIdV3Internal page_id, PageEntriesEdit & edit)
{
    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_REF)
    {
        if (create_ver.sequence > seq)
            return;
        // We need to keep the VAR_REF once create_ver > seq,
        // or the being-ref entry/external won't be able to be clean
        // after restore.
        edit.varRef(page_id, create_ver, ori_page_id);
        if (is_deleted && delete_ver.sequence <= seq)
        {
            edit.varDel(page_id, delete_ver);
        }
        return;
    }

    if (type == EditRecordType::VAR_EXTERNAL)
    {
        if (create_ver.sequence > seq)
            return;
        edit.varExternal(page_id, create_ver, being_ref_count);
        if (is_deleted && delete_ver.sequence <= seq)
        {
            edit.varDel(page_id, delete_ver);
        }
        return;
    }

    if (type == EditRecordType::VAR_ENTRY)
    {
        // dump the latest entry if it is not a "delete"
        auto last_iter = MapUtils::findLess(entries, PageVersion(seq + 1));
        if (last_iter == entries.end())
            return;

        if (last_iter->second.isEntry())
        {
            const auto & entry = last_iter->second;
            edit.varEntry(page_id, /*ver*/ last_iter->first, entry.entry, entry.being_ref_count);
            return;
        }
        else if (last_iter->second.isDelete())
        {
            if (last_iter == entries.begin())
            {
                // only delete left, then we don't need to keep this
                return;
            }
            auto last_version = last_iter->first;
            auto prev_iter = --last_iter; // Note that `last_iter` should not be used anymore
            while (true)
            {
                // if there is any entry prev to this delete entry,
                //   1) the entry may be ref by another id.
                //   2) the entry may be upsert into a newer wal file by the gc process.
                // So we need to keep the entry item and its delete entry in the snapshot.
                if (prev_iter->second.isEntry())
                {
                    const auto & entry = prev_iter->second;
                    edit.varEntry(page_id, prev_iter->first, entry.entry, entry.being_ref_count);
                    edit.varDel(page_id, last_version);
                    break;
                }
                if (prev_iter == entries.begin())
                    break;
                prev_iter--;
            }
        }
        return;
    }

    if (type == EditRecordType::VAR_DELETE)
    {
        // just ignore
        return;
    }

    throw Exception(fmt::format("Calling collapseTo with invalid state [state={}]", toDebugString()));
}

/**************************
  * PageDirectory methods *
  *************************/

PageDirectory::PageDirectory(String storage_name, WALStorePtr && wal_, UInt64 max_persisted_log_files_)
    : max_page_id(0)
    , sequence(0)
    , wal(std::move(wal_))
    , max_persisted_log_files(max_persisted_log_files_)
    , log(Logger::get(storage_name))
{
}

PageDirectorySnapshotPtr PageDirectory::createSnapshot(const String & tracing_id) const
{
    auto snap = std::make_shared<PageDirectorySnapshot>(sequence.load(), tracing_id);
    {
        std::lock_guard snapshots_lock(snapshots_mutex);
        snapshots.emplace_back(std::weak_ptr<PageDirectorySnapshot>(snap));
    }

    CurrentMetrics::add(CurrentMetrics::PSMVCCSnapshotsList);
    return snap;
}

SnapshotsStatistics PageDirectory::getSnapshotsStat() const
{
    SnapshotsStatistics stat;
    DB::Int64 num_snapshots_removed = 0;
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
                if (snapshot_lifetime > stat.longest_living_seconds)
                {
                    stat.longest_living_seconds = snapshot_lifetime;
                    stat.longest_living_from_thread_id = snapshot_ptr->create_thread;
                    stat.longest_living_from_tracing_id = snapshot_ptr->tracing_id;
                }
                stat.num_snapshots++;
                ++iter;
            }
        }
    }

    CurrentMetrics::sub(CurrentMetrics::PSMVCCSnapshotsList, num_snapshots_removed);
    // Return some statistics of the oldest living snapshot.
    return stat;
}

PageIDAndEntryV3 PageDirectory::getByIDImpl(PageIdV3Internal page_id, const PageDirectorySnapshotPtr & snap, bool throw_on_not_exist) const
{
    PageEntryV3 entry_got;

    // After two write batches applied: [ver=1]{put 10}, [ver=2]{ref 11->10, del 10}, the `mvcc_table_directory` is:
    // {
    //     "10": [
    //         {
    //             "type": "entry",
    //             "create_ver": 1,
    //             "being_ref_count": 2, // being ref by id 11
    //             "entry": "..some offset to blob file" // mark as "entryX"
    //         },
    //         {
    //             "type": "delete",
    //             "delete_ver": 2,
    //         },
    //     ],
    //     "11": {
    //         "type": "ref",
    //         "ori_page_id": 10,
    //         "create_ver": 2,
    //     },
    // }
    //
    // When accessing by a snapshot with seq=2, we should not get the page 10, but can get the page 11.
    // In order to achieve this behavior, when calling `get` with page_id=10 and snapshot seq=2, first
    // call `resolveToPageId` with `ignore_delete=false` and return invalid.
    // When calling `get` with page_id=11 and snapshot seq=2, first call `resolveToPageId` and need further
    // resolve ref id 11 to 10 with seq=2, and continue to ignore all "delete"s in the version chain in
    // page 10 until we find the "entryX".

    PageIdV3Internal id_to_resolve = page_id;
    PageVersion ver_to_resolve(snap->sequence, 0);
    bool ok = true;
    while (ok)
    {
        MVCCMapType::const_iterator iter;
        {
            std::shared_lock read_lock(table_rw_mutex);
            iter = mvcc_table_directory.find(id_to_resolve);
            if (iter == mvcc_table_directory.end())
            {
                if (throw_on_not_exist)
                {
                    LOG_WARNING(log, "Dump state for invalid page id [page_id={}]", page_id);
                    for (const auto & [dump_id, dump_entry] : mvcc_table_directory)
                    {
                        LOG_WARNING(log, "Dumping state [page_id={}] [entry={}]", dump_id, dump_entry == nullptr ? "<null>" : dump_entry->toDebugString());
                    }
                    throw Exception(fmt::format("Invalid page id, entry not exist [page_id={}] [resolve_id={}]", page_id, id_to_resolve), ErrorCodes::PS_ENTRY_NOT_EXISTS);
                }
                else
                {
                    return PageIDAndEntryV3{page_id, PageEntryV3{.file_id = INVALID_BLOBFILE_ID}};
                }
            }
        }
        auto [resolve_state, next_id_to_resolve, next_ver_to_resolve] = iter->second->resolveToPageId(ver_to_resolve.sequence, /*ignore_delete=*/id_to_resolve != page_id, &entry_got);
        switch (resolve_state)
        {
        case ResolveResult::TO_NORMAL:
            return PageIDAndEntryV3(page_id, entry_got);
        case ResolveResult::FAIL:
            ok = false;
            break;
        case ResolveResult::TO_REF:
            if (id_to_resolve == next_id_to_resolve)
            {
                ok = false;
                break;
            }
            id_to_resolve = next_id_to_resolve;
            ver_to_resolve = next_ver_to_resolve;
            break; // continue the resolving
        }
    }

    // Only mix mode throw_on_not_exist is false.
    // In mix mode, storage will create a snapshot contains V2 and V3.
    // If we find a del entry in V3, we still need find it in V2.
    if (throw_on_not_exist)
    {
        throw Exception(fmt::format("Fail to get entry [page_id={}] [seq={}] [resolve_id={}] [resolve_ver={}]", page_id, snap->sequence, id_to_resolve, ver_to_resolve), ErrorCodes::PS_ENTRY_NO_VALID_VERSION);
    }
    else
    {
        return PageIDAndEntryV3{page_id, PageEntryV3{.file_id = INVALID_BLOBFILE_ID}};
    }
}

std::pair<PageIDAndEntriesV3, PageIds> PageDirectory::getByIDsImpl(const PageIdV3Internals & page_ids, const PageDirectorySnapshotPtr & snap, bool throw_on_not_exist) const
{
    PageEntryV3 entry_got;
    PageIds page_not_found = {};

    const PageVersion init_ver_to_resolve(snap->sequence, 0);
    auto get_one = [&entry_got, init_ver_to_resolve, throw_on_not_exist, this](PageIdV3Internal page_id, PageVersion ver_to_resolve, size_t idx) {
        PageIdV3Internal id_to_resolve = page_id;
        bool ok = true;
        while (ok)
        {
            MVCCMapType::const_iterator iter;
            {
                std::shared_lock read_lock(table_rw_mutex);
                iter = mvcc_table_directory.find(id_to_resolve);
                if (iter == mvcc_table_directory.end())
                {
                    if (throw_on_not_exist)
                    {
                        throw Exception(fmt::format("Invalid page id, entry not exist [page_id={}] [resolve_id={}]", page_id, id_to_resolve), ErrorCodes::PS_ENTRY_NOT_EXISTS);
                    }
                    else
                    {
                        return false;
                    }
                }
            }
            auto [resolve_state, next_id_to_resolve, next_ver_to_resolve] = iter->second->resolveToPageId(ver_to_resolve.sequence, /*ignore_delete=*/id_to_resolve != page_id, &entry_got);
            switch (resolve_state)
            {
            case ResolveResult::TO_NORMAL:
                return true;
            case ResolveResult::FAIL:
                ok = false;
                break;
            case ResolveResult::TO_REF:
                if (id_to_resolve == next_id_to_resolve)
                {
                    ok = false;
                    break;
                }
                id_to_resolve = next_id_to_resolve;
                ver_to_resolve = next_ver_to_resolve;
                break; // continue the resolving
            }
        }

        if (throw_on_not_exist)
        {
            throw Exception(fmt::format("Fail to get entry [page_id={}] [ver={}] [resolve_id={}] [resolve_ver={}] [idx={}]", page_id, init_ver_to_resolve, id_to_resolve, ver_to_resolve, idx), ErrorCodes::PS_ENTRY_NO_VALID_VERSION);
        }
        else
        {
            return false;
        }
    };

    PageIDAndEntriesV3 id_entries;
    for (size_t idx = 0; idx < page_ids.size(); ++idx)
    {
        if (auto ok = get_one(page_ids[idx], init_ver_to_resolve, idx); ok)
        {
            id_entries.emplace_back(page_ids[idx], entry_got);
        }
        else
        {
            page_not_found.emplace_back(page_ids[idx]);
        }
    }

    return std::make_pair(id_entries, page_not_found);
}

PageIdV3Internal PageDirectory::getNormalPageId(PageIdV3Internal page_id, const DB::PageStorageSnapshotPtr & snap_, bool throw_on_not_exist) const
{
    auto snap = toConcreteSnapshot(snap_);
    PageIdV3Internal id_to_resolve = page_id;
    PageVersion ver_to_resolve(snap->sequence, 0);
    bool keep_resolve = true;
    while (keep_resolve)
    {
        MVCCMapType::const_iterator iter;
        {
            std::shared_lock read_lock(table_rw_mutex);
            iter = mvcc_table_directory.find(id_to_resolve);
            if (iter == mvcc_table_directory.end())
            {
                if (throw_on_not_exist)
                {
                    throw Exception(fmt::format("Invalid page id [page_id={}] [resolve_id={}]", page_id, id_to_resolve));
                }
                else
                {
                    return buildV3Id(0, INVALID_PAGE_ID);
                }
            }
        }
        auto [resolve_state, next_id_to_resolve, next_ver_to_resolve] = iter->second->resolveToPageId(ver_to_resolve.sequence, /*ignore_delete=*/id_to_resolve != page_id, nullptr);
        switch (resolve_state)
        {
        case ResolveResult::TO_NORMAL:
            return id_to_resolve;
        case ResolveResult::FAIL:
            // resolve failed
            keep_resolve = false;
            break;
        case ResolveResult::TO_REF:
            if (id_to_resolve == next_id_to_resolve)
            {
                // dead-loop, so break the `while(keep_resolve)`
                keep_resolve = false;
                break;
            }
            id_to_resolve = next_id_to_resolve;
            ver_to_resolve = next_ver_to_resolve;
            break; // continue the resolving
        }
    }

    if (throw_on_not_exist)
    {
        throw Exception(fmt::format(
            "fail to get normal id [page_id={}] [seq={}] [resolve_id={}] [resolve_ver={}]",
            page_id,
            snap->sequence,
            id_to_resolve,
            ver_to_resolve));
    }
    else
    {
        return buildV3Id(0, INVALID_PAGE_ID);
    }
}

PageId PageDirectory::getMaxId() const
{
    std::shared_lock read_lock(table_rw_mutex);
    return max_page_id;
}

std::set<PageIdV3Internal> PageDirectory::getAllPageIds()
{
    std::set<PageIdV3Internal> page_ids;

    std::shared_lock read_lock(table_rw_mutex);
    const auto seq = sequence.load();
    for (auto & [page_id, versioned] : mvcc_table_directory)
    {
        // Only return the page_id that is visible
        if (versioned->isVisible(seq))
            page_ids.insert(page_id);
    }
    return page_ids;
}

void PageDirectory::applyRefEditRecord(
    MVCCMapType & mvcc_table_directory,
    const VersionedPageEntriesPtr & version_list,
    const PageEntriesEdit::EditRecord & rec,
    const PageVersion & version)
{
    // Assume the `mvcc_table_directory` is:
    // {
    //     "10": [
    //         {
    //             "type": "entry",
    //             "create_ver": 1,
    //             "being_ref_count": 2, // being ref by id 11
    //             "entry": "..some offset to blob file" // mark as "entryX"
    //         },
    //         {
    //             "type": "delete",
    //             "delete_ver": 3,
    //         },
    //     ],
    //     "11": {
    //         "type": "ref",
    //         "ori_page_id": 10,
    //         "create_ver": 2,
    //     },
    // }
    //
    // When we need to create a new ref 12->11, first call `resolveToPageId` with `ignore_delete=false`
    // and further resolve ref id 11 to 10. Then we will call `resolveToPageId` with `ignore_delete=true`
    // to ignore the "delete"s.
    // Finally, we will collapse the ref chain to create a "ref 12 -> 10" instead of "ref 12 -> 11 -> 10"
    // in memory and increase the ref-count of "entryX".
    //
    // The reason we choose to collapse the ref chain while applying ref edit is that doing GC on a
    // non-collapse ref chain is much harder and long ref chain make the time of accessing an entry
    // not stable.

    auto [resolve_success, resolved_id, resolved_ver] = [&mvcc_table_directory, ori_page_id = rec.ori_page_id](PageIdV3Internal id_to_resolve, PageVersion ver_to_resolve)
        -> std::tuple<bool, PageIdV3Internal, PageVersion> {
        while (true)
        {
            auto resolve_ver_iter = mvcc_table_directory.find(id_to_resolve);
            if (resolve_ver_iter == mvcc_table_directory.end())
                return {false, buildV3Id(0, 0), PageVersion(0)};

            const VersionedPageEntriesPtr & resolve_version_list = resolve_ver_iter->second;
            auto [resolve_state, next_id_to_resolve, next_ver_to_resolve] = resolve_version_list->resolveToPageId(
                ver_to_resolve.sequence,
                /*ignore_delete=*/id_to_resolve != ori_page_id,
                nullptr);
            switch (resolve_state)
            {
            case ResolveResult::FAIL:
                return {false, id_to_resolve, ver_to_resolve};
            case ResolveResult::TO_NORMAL:
                return {true, id_to_resolve, ver_to_resolve};
            case ResolveResult::TO_REF:
                if (id_to_resolve == next_id_to_resolve)
                {
                    return {false, next_id_to_resolve, next_ver_to_resolve};
                }
                id_to_resolve = next_id_to_resolve;
                ver_to_resolve = next_ver_to_resolve;
                break; // continue the resolving
            }
        }
    }(rec.ori_page_id, version);
    if (!resolve_success)
    {
        throw Exception(fmt::format(
            "Trying to add ref to non-exist page [page_id={}] [ori_id={}] [ver={}] [resolve_id={}] [resolve_ver={}]",
            rec.page_id,
            rec.ori_page_id,
            version,
            resolved_id,
            resolved_ver));
    }

    SYNC_FOR("before_PageDirectory::applyRefEditRecord_create_ref");

    // use the resolved_id to collapse ref chain 3->2, 2->1 ==> 3->1
    bool is_ref_created = version_list->createNewRef(version, resolved_id);
    if (is_ref_created)
    {
        SYNC_FOR("before_PageDirectory::applyRefEditRecord_incr_ref_count");
        // Add the ref-count of being-ref entry
        if (auto resolved_iter = mvcc_table_directory.find(resolved_id); resolved_iter != mvcc_table_directory.end())
        {
            resolved_iter->second->incrRefCount(resolved_ver);
        }
        else
        {
            throw Exception(fmt::format(
                "The ori page id is not found [page_id={}] [ori_id={}] [ver={}] [resolved_id={}] [resolved_ver={}]",
                rec.page_id,
                rec.ori_page_id,
                version,
                resolved_id,
                resolved_ver));
        }
    }
    SYNC_FOR("after_PageDirectory::applyRefEditRecord_incr_ref_count");
}

void PageDirectory::apply(PageEntriesEdit && edit, const WriteLimiterPtr & write_limiter)
{
    // We need to make sure there is only one apply thread to write wal and then increase `sequence`.
    // Note that, as read threads use current `sequence` as read_seq, we cannot increase `sequence`
    // before applying edit to `mvcc_table_directory`.
    //
    // TODO: It is totally serialized by only 1 thread with IO waiting. Make this process a
    // pipeline so that we can batch the incoming edit when doing IO.

    Stopwatch watch;

    std::unique_lock apply_lock(apply_mutex);

    GET_METRIC(tiflash_storage_page_write_duration_seconds, type_latch).Observe(watch.elapsedSeconds());
    watch.restart();

    UInt64 max_sequence = sequence.load();
    const auto edit_size = edit.size();

    // stage 1, persisted the changes to WAL.
    // In order to handle {put X, ref Y->X, del X} inside one WriteBatch (and
    // in later batch pipeline), we increase the sequence for each record.
    for (auto & r : edit.getMutRecords())
    {
        ++max_sequence;
        r.version = PageVersion(max_sequence, 0);
    }

    wal->apply(ser::serializeTo(edit), write_limiter);
    GET_METRIC(tiflash_storage_page_write_duration_seconds, type_wal).Observe(watch.elapsedSeconds());
    watch.restart();
    SCOPE_EXIT({ GET_METRIC(tiflash_storage_page_write_duration_seconds, type_commit).Observe(watch.elapsedSeconds()); });

    {
        std::unique_lock table_lock(table_rw_mutex);

        // stage 2, create entry version list for page_id.
        for (const auto & r : edit.getRecords())
        {
            // Protected in write_lock
            max_page_id = std::max(max_page_id, r.page_id.low);

            auto [iter, created] = mvcc_table_directory.insert(std::make_pair(r.page_id, nullptr));
            if (created)
            {
                iter->second = std::make_shared<VersionedPageEntries>();
            }

            auto & version_list = iter->second;
            try
            {
                switch (r.type)
                {
                case EditRecordType::PUT_EXTERNAL:
                {
                    auto holder = version_list->createNewExternal(r.version);
                    if (holder)
                    {
                        // put the new created holder into `external_ids`
                        *holder = r.page_id;
                        external_ids_by_ns.addExternalId(holder);
                    }
                    break;
                }
                case EditRecordType::PUT:
                    version_list->createNewEntry(r.version, r.entry);
                    break;
                case EditRecordType::DEL:
                    version_list->createDelete(r.version);
                    break;
                case EditRecordType::REF:
                    applyRefEditRecord(mvcc_table_directory, version_list, r, r.version);
                    break;
                case EditRecordType::UPSERT:
                case EditRecordType::VAR_DELETE:
                case EditRecordType::VAR_ENTRY:
                case EditRecordType::VAR_EXTERNAL:
                case EditRecordType::VAR_REF:
                    throw Exception(fmt::format("should not handle edit with invalid type [type={}]", magic_enum::enum_name(r.type)));
                }
            }
            catch (DB::Exception & e)
            {
                e.addMessage(fmt::format(" [type={}] [page_id={}] [ver={}] [edit_size={}]", magic_enum::enum_name(r.type), r.page_id, r.version, edit_size));
                e.rethrow();
            }
        }

        // stage 3, the edit committed, incr the sequence number to publish changes for `createSnapshot`
        sequence.fetch_add(edit_size);
    }
}

void PageDirectory::gcApply(PageEntriesEdit && migrated_edit, const WriteLimiterPtr & write_limiter)
{
    // Increase the epoch for migrated records
    for (auto & record : migrated_edit.getMutRecords())
    {
        record.version.epoch += 1;
    }

    // Apply migrate edit into WAL with the increased epoch version
    wal->apply(ser::serializeTo(migrated_edit), write_limiter);

    // Apply migrate edit to the mvcc map
    for (const auto & record : migrated_edit.getRecords())
    {
        MVCCMapType::const_iterator iter;
        {
            std::shared_lock read_lock(table_rw_mutex);
            iter = mvcc_table_directory.find(record.page_id);
            RUNTIME_CHECK_MSG(iter != mvcc_table_directory.end(), "Can't find [page_id={}] while doing gcApply", record.page_id);
        } // release the read lock on `table_rw_mutex`

        // Append the gc version to version list
        const auto & versioned_entries = iter->second;
        auto id_to_deref = versioned_entries->createUpsertEntry(record.version, record.entry);
        if (id_to_deref.low != INVALID_PAGE_ID)
        {
            // The ref-page is rewritten into a normal page, we need to decrease the ref-count of original page
            MVCCMapType::const_iterator deref_iter;
            {
                std::shared_lock read_lock(table_rw_mutex);
                deref_iter = mvcc_table_directory.find(id_to_deref);
                RUNTIME_CHECK_MSG(deref_iter != mvcc_table_directory.end(), "Can't find [page_id={}] to deref after gcApply", id_to_deref);
            }
            auto deref_res = deref_iter->second->derefAndClean(/*lowest_seq*/ 0, id_to_deref, record.version, 1, nullptr);
            RUNTIME_ASSERT(!deref_res);
        }
    }

    LOG_INFO(log, "GC apply done. [edit size={}]", migrated_edit.size());
}

std::pair<std::map<BlobFileId, PageIdAndVersionedEntries>, PageSize>
PageDirectory::getEntriesByBlobIds(const std::vector<BlobFileId> & blob_ids) const
{
    std::unordered_set<BlobFileId> blob_id_set;
    for (const auto blob_id : blob_ids)
        blob_id_set.insert(blob_id);
    assert(blob_id_set.size() == blob_ids.size());

    // TODO: return the max entry.size to make `BlobStore::gc` more clean
    std::map<BlobFileId, PageIdAndVersionedEntries> blob_versioned_entries;
    PageSize total_page_size = 0;
    UInt64 total_page_nums = 0;
    std::map<PageIdV3Internal, std::tuple<PageIdV3Internal, PageVersion>> ref_ids_maybe_rewrite;

    {
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
            fiu_do_on(FailPoints::pause_before_full_gc_prepare, {
                if (page_id.low == 101)
                    SYNC_FOR("before_PageDirectory::getEntriesByBlobIds_id_101");
            });
            auto single_page_size = version_entries->getEntriesByBlobIds(blob_id_set, page_id, blob_versioned_entries, ref_ids_maybe_rewrite);
            total_page_size += single_page_size;
            if (single_page_size != 0)
            {
                total_page_nums++;
            }

            {
                std::shared_lock read_lock(table_rw_mutex);
                iter++;
                if (iter == mvcc_table_directory.end())
                    break;
            }
        }
    }

    // For the non-deleted ref-ids, we will check whether theirs original entries lay on
    // `blob_id_set`. Rewrite the entries for these ref-ids to be normal pages.
    size_t num_ref_id_rewrite = 0;
    for (const auto & [ref_id, ori_id_ver] : ref_ids_maybe_rewrite)
    {
        const auto ori_id = std::get<0>(ori_id_ver);
        const auto ver = std::get<1>(ori_id_ver);
        MVCCMapType::const_iterator page_iter;
        {
            std::shared_lock read_lock(table_rw_mutex);
            page_iter = mvcc_table_directory.find(ori_id);
            RUNTIME_CHECK(page_iter != mvcc_table_directory.end(), ref_id, ori_id, ver);
        }
        const auto & version_entries = page_iter->second;
        // the latest entry with version.seq <= ref_id.create_ver.seq
        auto entry = version_entries->getLastEntry(ver.sequence);
        RUNTIME_CHECK(entry.has_value(), ref_id, ori_id, ver);
        // If the being-ref entry lays on the full gc candidate blobfiles, then we
        // need to rewrite the ref-id to a normal page.
        if (blob_id_set.count(entry->file_id) > 0)
        {
            blob_versioned_entries[entry->file_id].emplace_back(ref_id, ver, *entry);
            total_page_size += entry->size;
            total_page_nums += 1;
            num_ref_id_rewrite += 1;
        }
    }

    LOG_INFO(log, "Get entries by blob ids done [rewrite_ref_page_num={}] [total_page_size={}] [total_page_nums={}]", //
             num_ref_id_rewrite,
             total_page_size, //
             total_page_nums);
    return std::make_pair(std::move(blob_versioned_entries), total_page_size);
}

bool PageDirectory::tryDumpSnapshot(const ReadLimiterPtr & read_limiter, const WriteLimiterPtr & write_limiter, bool force)
{
    // Only apply compact logs when files snapshot is valid
    auto files_snap = wal->tryGetFilesSnapshot(max_persisted_log_files, force);
    if (!files_snap.isValid())
        return false;

    // To prevent writes from affecting dumping snapshot (and vice versa), old log files
    // are read from disk and a temporary PageDirectory is generated for dumping snapshot.
    // The main reason write affect dumping snapshot is that we can not get a read-only
    // `being_ref_count` by the function `createSnapshot()`.
    assert(!files_snap.persisted_log_files.empty()); // should not be empty
    auto log_num = files_snap.persisted_log_files.rbegin()->log_num;
    auto identifier = fmt::format("{}.dump_{}", wal->name(), log_num);
    auto snapshot_reader = wal->createReaderForFiles(identifier, files_snap.persisted_log_files, read_limiter);
    PageDirectoryFactory factory;
    // we just use the `collapsed_dir` to dump edit of the snapshot, should never call functions like `apply` that
    // persist new logs into disk. So we pass `nullptr` as `wal` to the factory.
    PageDirectoryPtr collapsed_dir = factory.createFromReader(
        identifier,
        std::move(snapshot_reader),
        /* wal */ nullptr);
    // The records persisted in `files_snap` is older than or equal to all records in `edit`
    auto edit_from_disk = collapsed_dir->dumpSnapshotToEdit();
    bool done_any_io = wal->saveSnapshot(std::move(files_snap), ser::serializeTo(edit_from_disk), edit_from_disk.size(), write_limiter);
    return done_any_io;
}

PageEntriesV3 PageDirectory::gcInMemEntries(bool return_removed_entries)
{
    UInt64 lowest_seq = sequence.load();

    UInt64 invalid_snapshot_nums = 0;
    UInt64 valid_snapshot_nums = 0;
    UInt64 longest_alive_snapshot_time = 0;
    UInt64 longest_alive_snapshot_seq = 0;
    UInt64 stale_snapshot_nums = 0;
    {
        // Cleanup released snapshots
        std::lock_guard lock(snapshots_mutex);
        for (auto iter = snapshots.begin(); iter != snapshots.end(); /* empty */)
        {
            if (auto snap = iter->lock(); snap == nullptr)
            {
                iter = snapshots.erase(iter);
                invalid_snapshot_nums++;
            }
            else
            {
                lowest_seq = std::min(lowest_seq, snap->sequence);
                ++iter;
                valid_snapshot_nums++;
                const auto alive_time_seconds = snap->elapsedSeconds();

                if (alive_time_seconds > 10 * 60) // TODO: Make `10 * 60` as a configuration
                {
                    LOG_WARNING(log, "Meet a stale snapshot [thread id={}] [tracing id={}] [seq={}] [alive time(s)={}]", snap->create_thread, snap->tracing_id, snap->sequence, alive_time_seconds);
                    stale_snapshot_nums++;
                }

                if (longest_alive_snapshot_time < alive_time_seconds)
                {
                    longest_alive_snapshot_time = alive_time_seconds;
                    longest_alive_snapshot_seq = snap->sequence;
                }
            }
        }
    }

    PageEntriesV3 all_del_entries;
    MVCCMapType::iterator iter;
    {
        std::shared_lock read_lock(table_rw_mutex);
        iter = mvcc_table_directory.begin();
        if (iter == mvcc_table_directory.end())
            return all_del_entries;
    }

    UInt64 invalid_page_nums = 0;
    UInt64 valid_page_nums = 0;

    // The page_id that we need to decrease ref count
    // { id_0: <version, num to decrease>, id_1: <...>, ... }
    std::map<PageIdV3Internal, std::pair<PageVersion, Int64>> normal_entries_to_deref;
    // Iterate all page_id and try to clean up useless var entries
    while (true)
    {
        // `iter` is an iter that won't be invalid cause by `apply`/`gcApply`.
        // do gc on the version list without lock on `mvcc_table_directory`.
        const bool all_deleted = iter->second->cleanOutdatedEntries(
            lowest_seq,
            &normal_entries_to_deref,
            return_removed_entries ? &all_del_entries : nullptr,
            iter->second->acquireLock());

        {
            std::unique_lock write_lock(table_rw_mutex);
            if (all_deleted)
            {
                iter = mvcc_table_directory.erase(iter);
                invalid_page_nums++;
            }
            else
            {
                valid_page_nums++;
                iter++;
            }

            if (iter == mvcc_table_directory.end())
                break;
        }
    }

    UInt64 total_deref_counter = 0;

    // Iterate all page_id that need to decrease ref count of specified version.
    for (const auto & [page_id, deref_counter] : normal_entries_to_deref)
    {
        MVCCMapType::iterator iter;
        {
            std::shared_lock read_lock(table_rw_mutex);
            iter = mvcc_table_directory.find(page_id);
            if (iter == mvcc_table_directory.end())
                continue;
        }

        const bool all_deleted = iter->second->derefAndClean(
            lowest_seq,
            page_id,
            /*deref_ver=*/deref_counter.first,
            /*deref_count=*/deref_counter.second,
            return_removed_entries ? &all_del_entries : nullptr);

        if (all_deleted)
        {
            std::unique_lock write_lock(table_rw_mutex);
            mvcc_table_directory.erase(iter);
            invalid_page_nums++;
            valid_page_nums--;
        }
    }

    LOG_INFO(log, "After MVCC gc in memory [lowest_seq={}] "
                  "clean [invalid_snapshot_nums={}] [invalid_page_nums={}] "
                  "[total_deref_counter={}] [all_del_entries={}]. "
                  "Still exist [snapshot_nums={}], [page_nums={}]. "
                  "Longest alive snapshot: [longest_alive_snapshot_time={}] "
                  "[longest_alive_snapshot_seq={}] [stale_snapshot_nums={}]",
             lowest_seq,
             invalid_snapshot_nums,
             invalid_page_nums,
             total_deref_counter,
             all_del_entries.size(),
             valid_snapshot_nums,
             valid_page_nums,
             longest_alive_snapshot_time,
             longest_alive_snapshot_seq,
             stale_snapshot_nums);

    return all_del_entries;
}

PageEntriesEdit PageDirectory::dumpSnapshotToEdit(PageDirectorySnapshotPtr snap)
{
    if (!snap)
    {
        snap = createSnapshot(/*tracing_id*/ "");
    }

    PageEntriesEdit edit;
    MVCCMapType::iterator iter;
    {
        std::shared_lock read_lock(table_rw_mutex);
        iter = mvcc_table_directory.begin();
        if (iter == mvcc_table_directory.end())
            return edit;
    }
    while (true)
    {
        iter->second->collapseTo(snap->sequence, iter->first, edit);

        {
            std::shared_lock read_lock(table_rw_mutex);
            ++iter;
            if (iter == mvcc_table_directory.end())
                break;
        }
    }

    LOG_INFO(log, "Dumped snapshot to edits.[sequence={}]", snap->sequence);
    return edit;
}

} // namespace PS::V3
} // namespace DB
