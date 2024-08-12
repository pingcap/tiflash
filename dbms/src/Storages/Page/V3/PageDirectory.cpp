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
#include <Common/FailPoint.h>
#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Common/SyncPoint/SyncPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Common/assert_cast.h>
#include <Storages/Page/V3/MapUtils.h>
#include <Storages/Page/V3/PageDefines.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/WAL/WALReader.h>
#include <Storages/Page/V3/WAL/serialize.h>
#include <Storages/Page/V3/WALStore.h>
#include <Storages/Page/WriteBatchImpl.h>
#include <common/logger_useful.h>

#include <magic_enum.hpp>
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

#pragma GCC diagnostic push
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
// include to suppress warnings on NO_THREAD_SAFETY_ANALYSIS. clang can't work without this include, don't know why
#include <grpcpp/security/credentials.h>
#pragma GCC diagnostic pop

namespace CurrentMetrics
{
extern const Metric PSMVCCSnapshotsList;
extern const Metric PSPendingWriterNum;
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

template <typename Trait>
PageLock VersionedPageEntries<Trait>::acquireLock() const NO_THREAD_SAFETY_ANALYSIS
{
    return std::lock_guard(m);
}

template <typename Trait>
size_t VersionedPageEntries<Trait>::size() const NO_THREAD_SAFETY_ANALYSIS
{
    auto lock = acquireLock();
    return entries.size();
}

template <typename Trait>
void VersionedPageEntries<Trait>::createNewEntry(const PageVersion & ver, const PageEntryV3 & entry)
    NO_THREAD_SAFETY_ANALYSIS
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
            RUNTIME_CHECK_MSG(
                last_iter->second.being_ref_count.getLatestRefCount() == 1 || last_iter->first.sequence >= ver.sequence,
                "Try to replace normal entry with an newer seq [ver={}] [prev_ver={}] [last_entry={}]",
                ver,
                last_iter->first,
                last_iter->second);
            // create a new version that inherit the `being_ref_count` of the last entry
            entries.emplace(ver, EntryOrDelete::newReplacingEntry(last_iter->second, entry));
        }
        return;
    }

    throw Exception(
        ErrorCodes::PS_DIR_APPLY_INVALID_STATUS,
        "try to create entry version with invalid state "
        "[ver={}] [entry={}] [state={}]",
        ver,
        entry,
        toDebugString());
}

template <typename Trait>
typename VersionedPageEntries<Trait>::PageId VersionedPageEntries<Trait>::createUpsertEntry(
    const PageVersion & ver,
    const PageEntryV3 & entry,
    bool strict_check) NO_THREAD_SAFETY_ANALYSIS
{
    auto page_lock = acquireLock();

    // For applying upsert entry, only `VAR_ENTRY`/`VAR_REF` is valid state.
    // But when `strict_check == false`, we will create a new entry when it is
    // in `VAR_DELETE` state.

    if (!strict_check && type == EditRecordType::VAR_DELETE)
    {
        type = EditRecordType::VAR_ENTRY;
        assert(entries.empty());
        entries.emplace(ver, EntryOrDelete::newNormalEntry(entry));
        return Trait::PageIdTrait::getInvalidID();
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
            // append after delete
            entries.emplace(ver, EntryOrDelete::newNormalEntry(entry));
        }
        else
        {
            assert(last_iter->second.isEntry());
            // It is ok to replace the entry with same sequence and newer epoch, but not valid
            // to replace the entry with newer sequence.
            RUNTIME_CHECK_MSG(
                last_iter->second.being_ref_count.getLatestRefCount() == 1 || last_iter->first.sequence >= ver.sequence,
                "Try to replace normal entry with an newer seq [ver={}] [prev_ver={}] [last_entry={}]",
                ver,
                last_iter->first,
                last_iter->second);
            // create a new version that inherit the `being_ref_count` of the last entry
            entries.emplace(ver, EntryOrDelete::newReplacingEntry(last_iter->second, entry));
        }
        return Trait::PageIdTrait::getInvalidID();
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
        ErrorCodes::PS_DIR_APPLY_INVALID_STATUS,
        "try to create upsert entry version with invalid state "
        "[ver={}] [entry={}] [state={}]",
        ver,
        entry,
        toDebugString());
}

// Create a new external version with version=`ver`.
// If create success, then return a shared_ptr as a holder for page_id. The holder
// will be release when this external version is totally removed.
template <typename Trait>
std::shared_ptr<typename VersionedPageEntries<Trait>::PageId> VersionedPageEntries<Trait>::createNewExternal(
    const PageVersion & ver,
    const PageEntryV3 & entry) NO_THREAD_SAFETY_ANALYSIS
{
    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_DELETE)
    {
        type = EditRecordType::VAR_EXTERNAL;
        is_deleted = false;
        create_ver = ver;
        delete_ver = PageVersion(0);
        RUNTIME_CHECK(entries.empty());
        entries.emplace(create_ver, EntryOrDelete::newNormalEntry(entry));
        // return the new created holder to caller to set the page_id
        external_holder = std::make_shared<typename Trait::PageId>();
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
                entries.emplace(create_ver, EntryOrDelete::newNormalEntry(entry));
                // return the new created holder to caller to set the page_id
                external_holder = std::make_shared<typename Trait::PageId>();
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
        ErrorCodes::PS_DIR_APPLY_INVALID_STATUS,
        "try to create external version with invalid state "
        "[ver={}] [state={}]",
        ver,
        toDebugString());
}

// Create a new delete version with version=`ver`.
template <typename Trait>
void VersionedPageEntries<Trait>::createDelete(const PageVersion & ver) NO_THREAD_SAFETY_ANALYSIS
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

    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "try to create delete version with invalid state "
        "[ver={}] [state={}]",
        ver,
        toDebugString());
}

template <typename Trait>
bool VersionedPageEntries<Trait>::updateLocalCacheForRemotePage(const PageVersion & ver, const PageEntryV3 & entry)
    NO_THREAD_SAFETY_ANALYSIS
{
    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_ENTRY)
    {
        auto last_iter = MapUtils::findMutLess(entries, PageVersion(ver.sequence + 1, 0));
        RUNTIME_CHECK_MSG(last_iter != entries.end() && last_iter->second.isEntry(), "{}", toDebugString());
        RUNTIME_CHECK_MSG(last_iter->second.getEntry().checkpoint_info.has_value(), "{}", toDebugString());
        if (!last_iter->second.getEntry().checkpoint_info.is_local_data_reclaimed)
        {
            return false;
        }
        last_iter->second.accessEntry([&](PageEntryV3 & ori_entry){
            ori_entry.file_id = entry.file_id;
            ori_entry.size = entry.size;
            ori_entry.offset = entry.offset;
            ori_entry.checksum = entry.checksum;
            ori_entry.checkpoint_info.is_local_data_reclaimed = false;
        });
        return true;
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "try to update remote page with invalid state "
        "[ver={}] [state={}]",
        ver,
        toDebugString());
}

// Create a new reference version with version=`ver` and `ori_page_id_`.
// If create success, then return true, otherwise return false.
template <typename Trait>
bool VersionedPageEntries<Trait>::createNewRef(const PageVersion & ver, const PageId & ori_page_id_)
    NO_THREAD_SAFETY_ANALYSIS
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
    throw Exception(
        ErrorCodes::PS_DIR_APPLY_INVALID_STATUS,
        "try to create ref version with invalid state "
        "[ver={}] [ori_page_id={}] [state={}]",
        ver,
        ori_page_id_,
        toDebugString());
}

template <typename Trait>
std::shared_ptr<typename VersionedPageEntries<Trait>::PageId> VersionedPageEntries<Trait>::fromRestored(
    const typename PageEntriesEdit::EditRecord & rec) NO_THREAD_SAFETY_ANALYSIS
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
        being_ref_count.restoreFrom(rec.version, rec.being_ref_count);
        entries.emplace(rec.version, EntryOrDelete::newFromRestored(rec.entry, rec.version, 1 /* meaningless */));
        external_holder = std::make_shared<typename Trait::PageId>(rec.page_id);
        return external_holder;
    }
    case EditRecordType::VAR_ENTRY:
    {
        type = EditRecordType::VAR_ENTRY;
        entries.emplace(rec.version, EntryOrDelete::newFromRestored(rec.entry, rec.version, rec.being_ref_count));
        return nullptr;
    }
    default:
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Calling VersionedPageEntries::fromRestored with unknown type: {}",
            static_cast<Int32>(rec.type));
    }
    }
}

template <typename Trait>
std::tuple<ResolveResult, typename VersionedPageEntries<Trait>::PageId, PageVersion> VersionedPageEntries<
    Trait>::resolveToPageId(UInt64 seq, bool ignore_delete, PageEntryV3 * entry) NO_THREAD_SAFETY_ANALYSIS
{
    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_ENTRY)
    {
        // entries are sorted by <ver, epoch>, find the first one less than <ver+1, 0>
        if (auto iter = MapUtils::findLess(entries, PageVersion(seq + 1)); iter != entries.end())
        {
            if (!ignore_delete && iter->second.isDelete())
            {
                // the page is not visible
                return {ResolveResult::FAIL, Trait::PageIdTrait::getInvalidID(), PageVersion(0)};
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
                    *entry = iter->second.getEntry();
                return {ResolveResult::TO_NORMAL, Trait::PageIdTrait::getInvalidID(), PageVersion(0)};
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
            auto iter = entries.find(create_ver);
            RUNTIME_CHECK(iter != entries.end());
            if (entry != nullptr)
                *entry = iter->second.getEntry();
            return {ResolveResult::TO_NORMAL, Trait::PageIdTrait::getInvalidID(), PageVersion(0)};
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
        LOG_WARNING(
            Logger::get(),
            "Can't resolve the EditRecordType, type={} type_int={}",
            magic_enum::enum_name(type),
            static_cast<Int32>(type));
    }

    return {ResolveResult::FAIL, Trait::PageIdTrait::getInvalidID(), PageVersion(0)};
}

template <typename Trait>
std::optional<PageEntryV3> VersionedPageEntries<Trait>::getEntry(UInt64 seq) const NO_THREAD_SAFETY_ANALYSIS
{
    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_ENTRY)
    {
        // entries are sorted by <ver, epoch>, find the first one less than <ver+1, 0>
        if (auto iter = MapUtils::findLess(entries, PageVersion(seq + 1)); iter != entries.end())
        {
            // not deleted
            if (iter->second.isEntry())
                return iter->second.getInner();
        }
    }
    return std::nullopt;
}

template <typename Trait>
std::optional<PageEntryV3> VersionedPageEntries<Trait>::getLastEntry(std::optional<UInt64> seq) const
    NO_THREAD_SAFETY_ANALYSIS
{
    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_ENTRY)
    {
        for (auto it_r = entries.rbegin(); it_r != entries.rend(); it_r++) // NOLINT(modernize-loop-convert)
        {
            if (seq.has_value() && it_r->first.sequence > seq.value())
                continue;
            if (it_r->second.isEntry())
            {
                return it_r->second.getInner();
            }
        }
    }
    return std::nullopt;
}

template <typename Trait>
void VersionedPageEntries<Trait>::copyCheckpointInfoFromEdit(const typename PageEntriesEdit::EditRecord & edit)
    NO_THREAD_SAFETY_ANALYSIS
{
    // We have a running PageStorage instance, and did a checkpoint dump. The checkpoint dump is encoded using
    // PageEntriesEdit. During the checkpoint dump, this function is invoked so that we can write back where
    // (the checkpoint info) each page's data was dumped.
    // In this case, there is a living snapshot protecting the data.

    RUNTIME_CHECK(edit.type == EditRecordType::VAR_ENTRY);
    // The checkpoint_info from `edit` could be empty when we upload the manifest without any page data
    if (!edit.entry.checkpoint_info.has_value())
        return;

    auto page_lock = acquireLock();

    // This entry must be valid because it must be visible for the snap which is used to dump checkpoint, so it cannot be gced
    RUNTIME_CHECK(type == EditRecordType::VAR_ENTRY);

    // Due to GC movement, (sequence, epoch) may be changed to (sequence, epoch+x), so
    // we search within [  (sequence, 0),  (sequence+1, 0)  ), and assign checkpoint info for all of it.
    auto iter = MapUtils::findMutLess(entries, PageVersion(edit.version.sequence + 1));
    if (iter == entries.end())
    {
        // The referenced version may be GCed so that we cannot find any matching entry.
        return;
    }

    // TODO: Not sure if there is a full GC this may be false? Let's keep it here for now.
    RUNTIME_CHECK(iter->first.sequence == edit.version.sequence, iter->first.sequence, edit.version.sequence);

    // Discard epoch, and only check sequence.
    while (iter->first.sequence == edit.version.sequence)
    {
        // We will never meet the same Version mapping to one entry and one delete, so let's verify it is an entry.
        RUNTIME_CHECK(iter->second.isEntry());

        bool is_local_data_reclaimed = false;
        iter->second.accessEntry([&](PageEntryV3 & entry){
            if (entry.checkpoint_info.has_value())
                is_local_data_reclaimed = entry.checkpoint_info.is_local_data_reclaimed;
            // else it does not have checkpoint_info, local data must be not reclaimed

            entry.checkpoint_info = edit.entry.checkpoint_info;
            entry.checkpoint_info.is_local_data_reclaimed = is_local_data_reclaimed; // keep this field value
        });

        if (iter == entries.begin())
            break;
        --iter;
    }
}

// Returns true when **this id** is "visible" by `seq`.
// If this page id is marked as deleted or not created, it is "not visible".
// Note that not visible does not means this id can be GC.
template <typename Trait>
bool VersionedPageEntries<Trait>::isVisible(UInt64 seq) const NO_THREAD_SAFETY_ANALYSIS
{
    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_DELETE)
    {
        return false;
    }
    else if (type == EditRecordType::VAR_ENTRY)
    {
        // entries are sorted by <ver, epoch>, find the first one less than <ver+1, 0>
        if (auto iter = MapUtils::findLess(entries, PageVersion(seq + 1)); iter != entries.end())
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
        return create_ver.sequence <= seq && (!is_deleted || delete_ver.sequence > seq);
    }

    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "calling isDeleted with invalid state "
        "[seq={}] [state={}]",
        seq,
        toDebugString());
}

template <typename Trait>
Int64 VersionedPageEntries<Trait>::incrRefCount(const PageVersion & target_ver, const PageVersion & ref_ver)
    NO_THREAD_SAFETY_ANALYSIS
{
    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_ENTRY)
    {
        if (auto iter = MapUtils::findMutLess(entries, PageVersion(target_ver.sequence + 1)); iter != entries.end())
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
                auto ref_count_value = iter->second.being_ref_count.getLatestRefCount();
                if (unlikely(met_delete && ref_count_value == 1))
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Try to add ref to a completely deleted entry [entry={}] [ver={}]",
                        iter->second,
                        target_ver);
                }
                iter->second.being_ref_count.incrRefCount(ref_ver, 1);
                return ref_count_value + 1;
            }
        } // fallthrough to FAIL
    }
    else if (type == EditRecordType::VAR_EXTERNAL)
    {
        if (create_ver <= target_ver)
        {
            // We may add reference to an external id even if it is logically deleted.
            auto ref_count_value = being_ref_count.getLatestRefCount();
            being_ref_count.incrRefCount(ref_ver, 1);
            return ref_count_value + 1;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "The entry to be added ref count is not found [ver={}] [state={}]",
        target_ver,
        toDebugString());
}

template <typename Trait>
PageSize VersionedPageEntries<Trait>::getEntriesByBlobIds(
    const std::unordered_set<BlobFileId> & blob_ids,
    const PageId & page_id,
    GcEntriesMap & blob_versioned_entries,
    std::map<PageId, std::tuple<PageId, PageVersion>> & ref_ids_maybe_rewrite) NO_THREAD_SAFETY_ANALYSIS
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
    if (const auto & entry = last_entry.getEntry(); blob_ids.count(entry.file_id) > 0)
    {
        blob_versioned_entries[entry.file_id].emplace_back(page_id, /* ver */ iter->first, entry);
        entry_size_full_gc += entry.size;
    }
    return entry_size_full_gc;
}

template <typename Trait>
bool VersionedPageEntries<Trait>::cleanOutdatedEntries(
    UInt64 lowest_seq,
    std::map<PageId, std::pair<PageVersion, Int64>> * normal_entries_to_deref,
    PageEntriesV3 * entries_removed,
    RemoteFileValidSizes * remote_file_sizes,
    const PageLock & /*page_lock*/)
{
    if (type == EditRecordType::VAR_EXTERNAL)
    {
        return (being_ref_count.getLatestRefCount() == 1 && is_deleted && delete_ver.sequence <= lowest_seq);
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
            if (auto [deref_counter, new_created] = normal_entries_to_deref->emplace(
                    std::make_pair(ori_page_id, std::make_pair(/*ver=*/create_ver, /*count=*/1)));
                !new_created)
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid state {}", toDebugString());
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
    if (iter == entries.begin() || iter == entries.end()) // NOLINT(misc-redundant-expression)
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

    if (remote_file_sizes)
    {
        // the entries after `iter` are valid, collect the remote info size for remote GC
        for (auto valid_iter = iter; valid_iter != entries.end(); ++valid_iter)
        {
            if (!valid_iter->second.isEntry())
                continue;
            const auto & entry = valid_iter->second.getEntry();
            if (!entry.checkpoint_info.has_value())
                continue;
            const auto & file_id = *entry.checkpoint_info.data_location.data_file_id;
            auto file_size_iter = remote_file_sizes->try_emplace(file_id, 0);
            file_size_iter.first->second += entry.checkpoint_info.data_location.size_in_file;
        }
    }

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
                if (iter->second.being_ref_count.getLatestRefCount() == 1)
                {
                    if (entries_removed)
                    {
                        entries_removed->emplace_back(iter->second.getEntry());
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
                    entries_removed->emplace_back(iter->second.getEntry());
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

template <typename Trait>
bool VersionedPageEntries<Trait>::derefAndClean(
    UInt64 lowest_seq,
    const typename Trait::PageId & page_id,
    const PageVersion & deref_ver,
    const Int64 deref_count,
    PageEntriesV3 * entries_removed) NO_THREAD_SAFETY_ANALYSIS
{
    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_EXTERNAL)
    {
        being_ref_count.decrRefCountInSnap(lowest_seq, deref_count);
        return (is_deleted && delete_ver.sequence <= lowest_seq && being_ref_count.getLatestRefCount() == 1);
    }
    else if (type == EditRecordType::VAR_ENTRY)
    {
        // Decrease the ref-counter. The entry may be moved to a newer entry with same sequence but higher epoch,
        // so we need to find the one less than <seq+1, 0> and decrease the ref-counter of it.
        auto iter = MapUtils::findMutLess(entries, PageVersion(deref_ver.sequence + 1, 0));
        if (iter == entries.end())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Can not find entry for decreasing ref count [page_id={}] [ver={}] [deref_count={}]",
                page_id,
                deref_ver,
                deref_count);
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
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Can not find entry for decreasing ref count till the begin [page_id={}] [ver={}] [deref_count={}]",
                page_id,
                deref_ver,
                deref_count);
        }
        assert(iter->second.isEntry());
        iter->second.being_ref_count.decrRefCountInSnap(lowest_seq, deref_count);

        if (lowest_seq == 0)
            return false;
        // Clean outdated entries after decreased the ref-counter
        // set `normal_entries_to_deref` to be nullptr to ignore cleaning ref-var-entries
        return cleanOutdatedEntries(
            lowest_seq,
            /*normal_entries_to_deref*/ nullptr,
            entries_removed,
            /*remote_file_sizes*/ nullptr,
            page_lock);
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "calling derefAndClean with invalid state, state={}", toDebugString());
}

template <typename Trait>
void VersionedPageEntries<Trait>::collapseTo(const UInt64 seq, const PageId & page_id, PageEntriesEdit & edit)
    NO_THREAD_SAFETY_ANALYSIS
{
    auto page_lock = acquireLock();
    if (type == EditRecordType::VAR_REF)
    {
        if (create_ver.sequence > seq)
            return;
        // - If create_ver > seq && ! is_deleted, then
        //   we need to keep a record for {page_id, VAR_REF}
        // - If create_ver > seq && is_deleted, then
        //   we need to keep a record for {page_id, VAR_REF} and {page_id, VAR_DEL}
        //   so that the page_id and being-ref page_id can be cleanup after restore
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
        auto iter = entries.find(create_ver);
        RUNTIME_CHECK(iter != entries.end());
        // - If create_ver > seq && ! is_deleted, then
        //   we need to keep a record for {page_id, VAR_EXT}
        // - If create_ver > seq && is_deleted, then
        //   we need to keep a record for {page_id, VAR_EXT} and {page_id, VAR_DEL}
        //   so that the page_id can be ref by another page_id after restore
        edit.varExternal(page_id, create_ver, iter->second.getEntry(), being_ref_count.getRefCountInSnap(seq));
        if (is_deleted && delete_ver.sequence <= seq)
        {
            edit.varDel(page_id, delete_ver);
        }
        return;
    }

    if (type == EditRecordType::VAR_ENTRY)
    {
        auto last_iter = MapUtils::findLess(entries, PageVersion(seq + 1));
        if (last_iter == entries.end())
            return;

        if (last_iter->second.isEntry())
        {
            // The latest entry is not a "delete", then "collapse" to {page_id, the latest version entry}
            const auto & entry = last_iter->second;
            edit.varEntry(
                page_id,
                /*ver*/ last_iter->first,
                entry.getEntry(),
                entry.being_ref_count.getRefCountInSnap(seq));
            return;
        }
        else if (last_iter->second.isDelete())
        {
            if (last_iter == entries.begin())
            {
                // only delete left, then we don't need to keep record for this page_id
                return;
            }

            // The latest entry is "delete",
            // - If the last non-delete entry is not being ref, then don't need to keep record for this page_id
            // - If the last non-delete entry is still being ref, then we keep a varEntry and varDel for this page_id
            //   so that it can be ref by another page_id after restore
            auto last_version = last_iter->first;
            auto prev_iter = --last_iter; // Note that `last_iter` should not be used anymore
            if (prev_iter->second.isEntry())
            {
                auto ref_count_value = prev_iter->second.being_ref_count.getRefCountInSnap(seq);
                if (ref_count_value == 1)
                    return;
                // It is being ref by another id, should persist the item and delete
                const auto & entry = prev_iter->second;
                edit.varEntry(page_id, prev_iter->first, entry.getEntry(), ref_count_value);
                edit.varDel(page_id, last_version);
            }
        }
        return;
    }

    if (type == EditRecordType::VAR_DELETE)
    {
        // just ignore
        return;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Calling collapseTo with invalid state, state={}", toDebugString());
}

/**************************
  * PageDirectory methods *
  *************************/

template <typename Trait>
PageDirectory<Trait>::PageDirectory(String storage_name, WALStorePtr && wal_, UInt64 max_persisted_log_files_)
    : max_page_id(0)
    , sequence(0)
    , wal(std::move(wal_))
    , max_persisted_log_files(max_persisted_log_files_)
    , log(Logger::get(storage_name))
{}

template <typename Trait>
PageDirectorySnapshotPtr PageDirectory<Trait>::createSnapshot(const String & tracing_id) const
{
    GET_METRIC(tiflash_storage_page_command_count, type_snapshot).Increment();
    auto snap = std::make_shared<PageDirectorySnapshot>(sequence.load(), tracing_id);
    {
        std::lock_guard snapshots_lock(snapshots_mutex);
        snapshots.emplace_back(std::weak_ptr<PageDirectorySnapshot>(snap));
    }

    CurrentMetrics::add(CurrentMetrics::PSMVCCSnapshotsList);
    return snap;
}

template <typename Trait>
SnapshotsStatistics PageDirectory<Trait>::getSnapshotsStat() const
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

template <typename Trait>
typename PageDirectory<Trait>::PageIdAndEntry PageDirectory<Trait>::getByIDImpl(
    const PageId & page_id,
    const PageDirectorySnapshotPtr & snap,
    bool throw_on_not_exist) const
{
    GET_METRIC(tiflash_storage_page_command_count, type_read_page_dir).Increment();
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

    PageId id_to_resolve = page_id;
    PageVersion ver_to_resolve(snap->sequence, 0);
    bool ok = true;
    while (ok)
    {
        VersionedPageEntriesPtr iter_v;
        {
            std::shared_lock read_lock(table_rw_mutex);
            auto iter = mvcc_table_directory.find(id_to_resolve);
            if (iter == mvcc_table_directory.end())
            {
                if (throw_on_not_exist)
                {
                    LOG_WARNING(log, "Dump state for invalid page id [page_id={}]", page_id);
                    for (const auto & [dump_id, dump_entry] : mvcc_table_directory)
                    {
                        LOG_WARNING(
                            log,
                            "Dumping state [page_id={}] [entry={}]",
                            dump_id,
                            dump_entry == nullptr ? "<null>" : dump_entry->toDebugString());
                    }
                    throw Exception(
                        ErrorCodes::PS_ENTRY_NOT_EXISTS,
                        "Invalid page id, entry not exist [page_id={}] [resolve_id={}]",
                        page_id,
                        id_to_resolve);
                }
                else
                {
                    return PageIdAndEntry{page_id, PageEntryV3{.file_id = INVALID_BLOBFILE_ID}};
                }
            }
            iter_v = iter->second;
        }
        auto [resolve_state, next_id_to_resolve, next_ver_to_resolve]
            = iter_v->resolveToPageId(ver_to_resolve.sequence, /*ignore_delete=*/id_to_resolve != page_id, &entry_got);
        switch (resolve_state)
        {
        case ResolveResult::TO_NORMAL:
            return PageIdAndEntry(page_id, entry_got);
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
        throw Exception(
            ErrorCodes::PS_ENTRY_NO_VALID_VERSION,
            "Fail to get entry [page_id={}] [seq={}] [resolve_id={}] [resolve_ver={}]",
            page_id,
            snap->sequence,
            id_to_resolve,
            ver_to_resolve);
    }
    else
    {
        return PageIdAndEntry{page_id, PageEntryV3{.file_id = INVALID_BLOBFILE_ID}};
    }
}

template <typename Trait>
std::pair<typename PageDirectory<Trait>::PageIdAndEntries, typename PageDirectory<Trait>::PageIds> PageDirectory<
    Trait>::
    getByIDsImpl(
        const typename PageDirectory<Trait>::PageIds & page_ids,
        const PageDirectorySnapshotPtr & snap,
        bool throw_on_not_exist) const
{
    GET_METRIC(tiflash_storage_page_command_count, type_read_page_dir).Increment();
    PageEntryV3 entry_got;
    PageIds page_not_found = {};

    const PageVersion init_ver_to_resolve(snap->sequence, 0);
    auto get_one = [&entry_got,
                    init_ver_to_resolve,
                    throw_on_not_exist,
                    this](PageId page_id, PageVersion ver_to_resolve, size_t idx) {
        PageId id_to_resolve = page_id;
        bool ok = true;
        while (ok)
        {
            VersionedPageEntriesPtr iter_v;
            {
                std::shared_lock read_lock(table_rw_mutex);
                auto iter = mvcc_table_directory.find(id_to_resolve);
                if (iter == mvcc_table_directory.end())
                {
                    if (throw_on_not_exist)
                    {
                        throw Exception(
                            ErrorCodes::PS_ENTRY_NOT_EXISTS,
                            "Invalid page id, entry not exist [page_id={}] [resolve_id={}]",
                            page_id,
                            id_to_resolve);
                    }
                    else
                    {
                        return false;
                    }
                }
                iter_v = iter->second;
            }
            auto [resolve_state, next_id_to_resolve, next_ver_to_resolve] = iter_v->resolveToPageId(
                ver_to_resolve.sequence,
                /*ignore_delete=*/id_to_resolve != page_id,
                &entry_got);
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
            throw Exception(
                ErrorCodes::PS_ENTRY_NO_VALID_VERSION,
                "Fail to get entry [page_id={}] [ver={}] [resolve_id={}] [resolve_ver={}] [idx={}]",
                page_id,
                init_ver_to_resolve,
                id_to_resolve,
                ver_to_resolve,
                idx);
        }
        else
        {
            return false;
        }
    };

    PageIdAndEntries id_entries;
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

template <typename Trait>
typename PageDirectory<Trait>::PageId PageDirectory<Trait>::getNormalPageId(
    const typename PageDirectory<Trait>::PageId & page_id,
    const DB::PageStorageSnapshotPtr & snap_,
    bool throw_on_not_exist) const
{
    auto snap = toConcreteSnapshot(snap_);
    PageId id_to_resolve = page_id;
    PageVersion ver_to_resolve(snap->sequence, 0);
    bool keep_resolve = true;
    while (keep_resolve)
    {
        VersionedPageEntriesPtr iter_v;
        {
            std::shared_lock read_lock(table_rw_mutex);
            auto iter = mvcc_table_directory.find(id_to_resolve);
            if (iter == mvcc_table_directory.end())
            {
                if (throw_on_not_exist)
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Invalid page id [page_id={}] [resolve_id={}]",
                        page_id,
                        id_to_resolve);
                }
                else
                {
                    return Trait::PageIdTrait::getInvalidID();
                }
            }
            iter_v = iter->second;
        }
        auto [resolve_state, next_id_to_resolve, next_ver_to_resolve]
            = iter_v->resolveToPageId(ver_to_resolve.sequence, /*ignore_delete=*/id_to_resolve != page_id, nullptr);
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
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "fail to get normal id [page_id={}] [seq={}] [resolve_id={}] [resolve_ver={}]",
            page_id,
            snap->sequence,
            id_to_resolve,
            ver_to_resolve);
    }
    else
    {
        return Trait::PageIdTrait::getInvalidID();
    }
}

template <typename Trait>
UInt64 PageDirectory<Trait>::getMaxIdAfterRestart() const
{
    std::shared_lock read_lock(table_rw_mutex);
    return max_page_id;
}

template <typename Trait>
typename PageDirectory<Trait>::PageIdSet PageDirectory<Trait>::getAllPageIds()
{
    GET_METRIC(tiflash_storage_page_command_count, type_scan).Increment();
    std::set<PageId> page_ids;

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

template <typename Trait>
typename PageDirectory<Trait>::PageIdSet PageDirectory<Trait>::getAllPageIdsWithPrefix(
    const String & prefix,
    const DB::PageStorageSnapshotPtr & snap_)
{
    GET_METRIC(tiflash_storage_page_command_count, type_scan).Increment();
    if constexpr (std::is_same_v<Trait, universal::PageDirectoryTrait>)
    {
        PageIdSet page_ids;
        auto seq = toConcreteSnapshot(snap_)->sequence;
        std::shared_lock read_lock(table_rw_mutex);
        for (auto iter = mvcc_table_directory.lower_bound(prefix); iter != mvcc_table_directory.end(); ++iter)
        {
            if (!iter->first.hasPrefix(prefix))
                break;
            // Only return the page_id that is visible
            if (iter->second->isVisible(seq))
                page_ids.insert(iter->first);
        }
        return page_ids;
    }
    else
    {
        throw Exception("", ErrorCodes::NOT_IMPLEMENTED);
    }
}

template <typename Trait>
typename PageDirectory<Trait>::PageIdSet PageDirectory<Trait>::getAllPageIdsInRange(
    const PageId & start,
    const PageId & end,
    const DB::PageStorageSnapshotPtr & snap_)
{
    GET_METRIC(tiflash_storage_page_command_count, type_scan).Increment();
    if constexpr (std::is_same_v<Trait, universal::PageDirectoryTrait>)
    {
        PageIdSet page_ids;
        auto seq = toConcreteSnapshot(snap_)->sequence;
        std::shared_lock read_lock(table_rw_mutex);
        for (auto iter = mvcc_table_directory.lower_bound(start); iter != mvcc_table_directory.end(); ++iter)
        {
            if (!end.empty() && iter->first >= end)
                break;
            // Only return the page_id that is visible
            if (iter->second->isVisible(seq))
                page_ids.insert(iter->first);
        }
        return page_ids;
    }
    else
    {
        throw Exception("", ErrorCodes::NOT_IMPLEMENTED);
    }
}

template <typename Trait>
std::optional<typename PageDirectory<Trait>::PageId> PageDirectory<Trait>::getLowerBound(
    const typename Trait::PageId & start,
    const DB::PageStorageSnapshotPtr & snap_)
{
    if constexpr (std::is_same_v<Trait, universal::PageDirectoryTrait>)
    {
        auto seq = toConcreteSnapshot(snap_)->sequence;
        std::shared_lock read_lock(table_rw_mutex);
        for (auto iter = mvcc_table_directory.lower_bound(start); iter != mvcc_table_directory.end(); ++iter)
        {
            // Only return the page_id that is visible
            if (iter->second->isVisible(seq))
                return iter->first;
        }
        return std::nullopt;
    }
    else
    {
        throw Exception("", ErrorCodes::NOT_IMPLEMENTED);
    }
}

template <typename Trait>
void PageDirectory<Trait>::applyRefEditRecord(
    MVCCMapType & mvcc_table_directory,
    const VersionedPageEntriesPtr & version_list,
    const typename PageEntriesEdit::EditRecord & rec,
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

    auto [resolve_success, resolved_id, resolved_ver]
        = [&mvcc_table_directory, ori_page_id = rec.ori_page_id](
              PageId id_to_resolve,
              PageVersion ver_to_resolve) -> std::tuple<bool, PageId, PageVersion> {
        while (true)
        {
            auto resolve_ver_iter = mvcc_table_directory.find(id_to_resolve);
            if (resolve_ver_iter == mvcc_table_directory.end())
                return {false, Trait::PageIdTrait::getInvalidID(), PageVersion(0)};

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
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Trying to add ref to non-exist page [page_id={}] [ori_id={}] [ver={}] [resolve_id={}] [resolve_ver={}]",
            rec.page_id,
            rec.ori_page_id,
            version,
            resolved_id,
            resolved_ver);
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
            resolved_iter->second->incrRefCount(resolved_ver, version);
        }
        else
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The ori page id is not found [page_id={}] [ori_id={}] [ver={}] [resolved_id={}] [resolved_ver={}]",
                rec.page_id,
                rec.ori_page_id,
                version,
                resolved_id,
                resolved_ver);
        }
    }
    SYNC_FOR("after_PageDirectory::applyRefEditRecord_incr_ref_count");
}

template <typename Trait>
typename PageDirectory<Trait>::Writer * PageDirectory<Trait>::buildWriteGroup(
    Writer * first,
    std::unique_lock<std::mutex> & /*lock*/)
{
    RUNTIME_CHECK(!writers.empty());
    RUNTIME_CHECK(first == writers.front());
    auto * last_writer = first;
    auto iter = writers.begin();
    iter++;
    for (; iter != writers.end(); iter++)
    {
        auto * w = *iter;
        first->edit->merge(std::move(*(w->edit)));
        last_writer = w;
        w->edit->clear(); // free the memory after `moved`
    }
    return last_writer;
}

template <typename Trait>
std::unordered_set<String> PageDirectory<Trait>::apply(PageEntriesEdit && edit, const WriteLimiterPtr & write_limiter)
{
    // We need to make sure there is only one apply thread to write wal and then increase `sequence`.
    // Note that, as read threads use current `sequence` as read_seq, we cannot increase `sequence`
    // before applying edit to `mvcc_table_directory`.
    GET_METRIC(tiflash_storage_page_command_count, type_write).Increment();
    CurrentMetrics::Increment pending_writer_size{CurrentMetrics::PSPendingWriterNum};
    Writer w;
    w.edit = &edit;

    Stopwatch watch;
    std::unique_lock apply_lock(apply_mutex);

    GET_METRIC(tiflash_storage_page_write_duration_seconds, type_latch).Observe(watch.elapsedSeconds());
    watch.restart();

    writers.push_back(&w);
    SYNC_FOR("after_PageDirectory::enter_write_group");
    w.cv.wait(apply_lock, [&] { return w.done || &w == writers.front(); });
    GET_METRIC(tiflash_storage_page_write_duration_seconds, type_wait_in_group).Observe(watch.elapsedSeconds());
    watch.restart();
    if (w.done)
    {
        if (unlikely(!w.success))
        {
            if (w.exception)
            {
                w.exception->rethrow();
            }
            else
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown exception");
            }
        }
        // the `applied_data_files` will be returned by the write
        // group owner, others just return an empty set.
        return {};
    }
    auto * last_writer = buildWriteGroup(&w, apply_lock);
    apply_lock.unlock();
    SYNC_FOR("before_PageDirectory::leader_apply");

    // `true` means the write process has completed without exception
    bool success = false;
    std::unique_ptr<DB::Exception> exception = nullptr;

    SCOPE_EXIT({
        apply_lock.lock();
        while (true)
        {
            auto * ready = writers.front();
            writers.pop_front();
            if (ready != &w)
            {
                ready->done = true;
                ready->success = success;
                if (exception != nullptr)
                {
                    ready->exception = std::move(std::unique_ptr<DB::Exception>(exception->clone()));
                }
                ready->cv.notify_one();
            }
            if (ready == last_writer)
                break;
        }
        if (!writers.empty())
        {
            writers.front()->cv.notify_one();
        }
    });

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

    wal->apply(Trait::Serializer::serializeTo(edit), write_limiter);
    GET_METRIC(tiflash_storage_page_write_duration_seconds, type_wal).Observe(watch.elapsedSeconds());
    watch.restart();
    SCOPE_EXIT({ //
        GET_METRIC(tiflash_storage_page_write_duration_seconds, type_commit).Observe(watch.elapsedSeconds());
    });

    SYNC_FOR("before_PageDirectory::apply_to_memory");
    std::unordered_set<String> applied_data_files;
    {
        std::unique_lock table_lock(table_rw_mutex);

        // stage 2, create entry version list for page_id.
        for (const auto & r : edit.getRecords())
        {
            // Protected in write_lock
            auto [iter, created] = mvcc_table_directory.insert(std::make_pair(r.page_id, nullptr));
            if (created)
            {
                iter->second = std::make_shared<VersionedPageEntries<Trait>>();
            }

            auto & version_list = iter->second;
            try
            {
                switch (r.type)
                {
                case EditRecordType::PUT_EXTERNAL:
                {
                    auto holder = version_list->createNewExternal(r.version, r.entry);
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
                case EditRecordType::UPDATE_DATA_FROM_REMOTE:
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "should not handle edit with invalid type [type={}]",
                        magic_enum::enum_name(r.type));
                }

                // collect the applied remote data_file_ids
                if (r.entry.checkpoint_info.has_value())
                {
                    applied_data_files.emplace(*r.entry.checkpoint_info.data_location.data_file_id);
                }
            }
            catch (DB::Exception & e)
            {
                e.addMessage(fmt::format(
                    " [type={}] [page_id={}] [ver={}] [edit_size={}]",
                    magic_enum::enum_name(r.type),
                    r.page_id,
                    r.version,
                    edit_size));
                exception.reset(e.clone());
                e.rethrow();
            }
        }

        // stage 3, the edit committed, incr the sequence number to publish changes for `createSnapshot`
        sequence.fetch_add(edit_size);
    }

    success = true;
    return applied_data_files;
}

template <typename Trait>
typename PageDirectory<Trait>::PageEntries PageDirectory<Trait>::updateLocalCacheForRemotePages(
    PageEntriesEdit && edit,
    const DB::PageStorageSnapshotPtr & snap_,
    const WriteLimiterPtr & write_limiter)
{
    std::unique_lock apply_lock(apply_mutex);
    auto seq = toConcreteSnapshot(snap_)->sequence;
    for (auto & r : edit.getMutRecords())
    {
        r.version = PageVersion(seq, 0);
    }
    wal->apply(Trait::Serializer::serializeTo(edit), write_limiter);
    typename PageDirectory<Trait>::PageEntries ignored_entries;
    {
        std::unique_lock table_lock(table_rw_mutex);

        for (const auto & r : edit.getRecords())
        {
            auto id_to_resolve = r.page_id;
            auto sequence_to_resolve = seq;
            while (true)
            {
                auto iter = mvcc_table_directory.lower_bound(id_to_resolve);
                assert(iter != mvcc_table_directory.end());
                auto & version_list = iter->second;
                auto [resolve_state, next_id_to_resolve, next_ver_to_resolve] = version_list->resolveToPageId(
                    sequence_to_resolve,
                    /*ignore_delete=*/id_to_resolve != r.page_id,
                    nullptr);
                if (resolve_state == ResolveResult::TO_NORMAL)
                {
                    if (!version_list->updateLocalCacheForRemotePage(PageVersion(sequence_to_resolve, 0), r.entry))
                    {
                        ignored_entries.push_back(r.entry);
                    }
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
            }
        }
    }
    return ignored_entries;
}

template <typename Trait>
void PageDirectory<Trait>::gcApply(PageEntriesEdit && migrated_edit, const WriteLimiterPtr & write_limiter)
{
    // Increase the epoch for migrated records
    for (auto & record : migrated_edit.getMutRecords())
    {
        record.version.epoch += 1;
    }

    // Apply migrate edit into WAL with the increased epoch version
    wal->apply(Trait::Serializer::serializeTo(migrated_edit), write_limiter);

    // Apply migrate edit to the mvcc map
    for (const auto & record : migrated_edit.getRecords())
    {
        typename MVCCMapType::const_iterator iter;
        {
            std::shared_lock read_lock(table_rw_mutex);
            iter = mvcc_table_directory.find(record.page_id);
            RUNTIME_CHECK_MSG(
                iter != mvcc_table_directory.end(),
                "Can't find page while doing gcApply, page_id={}",
                record.page_id);
        } // release the read lock on `table_rw_mutex`

        // Append the gc version to version list
        const auto & versioned_entries = iter->second;
        auto id_to_deref = versioned_entries->createUpsertEntry(record.version, record.entry, /*strict_check*/ true);
        if (id_to_deref != Trait::PageIdTrait::getInvalidID())
        {
            // The ref-page is rewritten into a normal page, we need to decrease the ref-count of original page
            typename MVCCMapType::const_iterator deref_iter;
            {
                std::shared_lock read_lock(table_rw_mutex);
                deref_iter = mvcc_table_directory.find(id_to_deref);
                RUNTIME_CHECK_MSG(
                    deref_iter != mvcc_table_directory.end(),
                    "Can't find page to deref after gcApply, page_id={}",
                    id_to_deref);
            }
            auto deref_res
                = deref_iter->second->derefAndClean(/*lowest_seq*/ 0, id_to_deref, record.version, 1, nullptr);
            RUNTIME_ASSERT(!deref_res);
        }
    }

    LOG_INFO(log, "GC apply done, edit_size={}", migrated_edit.size());
}

template <typename Trait>
std::pair<typename PageDirectory<Trait>::GcEntriesMap, PageSize> PageDirectory<Trait>::getEntriesByBlobIds(
    const std::vector<BlobFileId> & blob_ids) const
{
    std::unordered_set<BlobFileId> blob_id_set;
    for (const auto blob_id : blob_ids)
        blob_id_set.insert(blob_id);
    assert(blob_id_set.size() == blob_ids.size());

    // TODO: return the max entry.size to make `BlobStore::gc` more clean
    typename PageDirectory<Trait>::GcEntriesMap blob_versioned_entries;
    PageSize total_page_size = 0;
    UInt64 total_page_nums = 0;
    std::map<PageId, std::tuple<PageId, PageVersion>> ref_ids_maybe_rewrite;

    {
        PageId page_id;
        VersionedPageEntriesPtr version_entries;

        {
            std::shared_lock read_lock(table_rw_mutex);
            auto iter = mvcc_table_directory.cbegin();
            if (iter == mvcc_table_directory.end())
                return {blob_versioned_entries, total_page_size};
            page_id = iter->first;
            version_entries = iter->second;
        }

        while (true)
        {
            fiu_do_on(FailPoints::pause_before_full_gc_prepare, {
                if constexpr (std::is_same_v<Trait, u128::PageDirectoryTrait>)
                {
                    if (page_id.low == 101)
                        SYNC_FOR("before_PageDirectory::getEntriesByBlobIds_id_101");
                }
            });
            auto single_page_size = version_entries->getEntriesByBlobIds(
                blob_id_set,
                page_id,
                blob_versioned_entries,
                ref_ids_maybe_rewrite);
            total_page_size += single_page_size;
            if (single_page_size != 0)
            {
                total_page_nums++;
            }

            {
                std::shared_lock read_lock(table_rw_mutex);
                auto iter = mvcc_table_directory.upper_bound(page_id);
                if (iter == mvcc_table_directory.end())
                    break;
                page_id = iter->first;
                version_entries = iter->second;
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

        VersionedPageEntriesPtr version_entries;
        {
            std::shared_lock read_lock(table_rw_mutex);
            auto page_iter = mvcc_table_directory.find(ori_id);
            RUNTIME_CHECK(page_iter != mvcc_table_directory.end(), ref_id, ori_id, ver);
            version_entries = page_iter->second;
        }
        // After storing all data in one PageStorage instance, we will run full gc
        // with external pages. Skip rewriting if it is an external pages.
        if (version_entries->isExternalPage())
            continue;
        // the latest entry with version.seq <= ref_id.create_ver.seq
        auto entry = version_entries->getLastEntry(ver.sequence);
        RUNTIME_CHECK_MSG(
            entry.has_value(),
            "ref_id={} ori_id={} ver={} entries={}",
            ref_id,
            ori_id,
            ver,
            version_entries->toDebugString());
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

    LOG_INFO(
        log,
        "Get entries by blob ids done, rewrite_ref_page_num={} total_page_size={} total_page_nums={}", //
        num_ref_id_rewrite,
        total_page_size, //
        total_page_nums);
    return std::make_pair(std::move(blob_versioned_entries), total_page_size);
}

template <typename Trait>
typename PageDirectory<Trait>::PageTypeAndGcInfo PageDirectory<Trait>::getEntriesByBlobIdsForDifferentPageTypes(
    const typename PageDirectory<Trait>::PageTypeAndBlobIds & page_type_and_blob_ids) const
{
    PageDirectory<Trait>::PageTypeAndGcInfo page_type_and_gc_info;
    // Because raft related data should do full gc less frequently, so we get the gc info for different page types separately.
    // TODO: get entries in a single traverse of PageDirectory
    for (const auto & [page_type, blob_ids] : page_type_and_blob_ids)
    {
        auto [blob_versioned_entries, total_page_size] = getEntriesByBlobIds(blob_ids);
        page_type_and_gc_info.emplace_back(page_type, std::move(blob_versioned_entries), total_page_size);
    }

    return page_type_and_gc_info;
}

template <typename Trait>
bool PageDirectory<Trait>::tryDumpSnapshot(const WriteLimiterPtr & write_limiter, bool force)
{
    auto identifier = fmt::format("{}.dump", wal->name());
    auto snap = createSnapshot(identifier);
    SYNC_FOR("after_PageDirectory::create_snap_for_dump");

    // Only apply compact logs when files snapshot is valid
    auto files_snap = wal->tryGetFilesSnapshot(
        max_persisted_log_files,
        snap->sequence,
        details::getMaxSequenceForRecord<Trait>,
        force);
    if (!files_snap.isValid())
        return false;

    assert(!files_snap.persisted_log_files.empty()); // should not be empty

    Stopwatch watch;
    auto edit = dumpSnapshotToEdit(snap);
    files_snap.num_records = edit.size();
    files_snap.dump_elapsed_ms = watch.elapsedMilliseconds();
    if constexpr (std::is_same_v<Trait, u128::PageDirectoryTrait>)
    {
        bool done_any_io = wal->saveSnapshot(
            std::move(files_snap),
            Trait::Serializer::serializeTo(edit),
            snap->sequence,
            write_limiter);
        return done_any_io;
    }
    else if constexpr (std::is_same_v<Trait, universal::PageDirectoryTrait>)
    {
        bool done_any_io = wal->saveSnapshot(
            std::move(files_snap),
            Trait::Serializer::serializeInCompressedFormTo(edit),
            snap->sequence,
            write_limiter);
        return done_any_io;
    }
}

template <typename Trait>
size_t PageDirectory<Trait>::copyCheckpointInfoFromEdit(const PageEntriesEdit & edit)
{
    size_t num_copied = 0;
    const auto & records = edit.getRecords();
    if (records.empty())
        return num_copied;

    for (const auto & rec : records)
    {
        // Only VAR_ENTRY need update checkpoint info.
        if (rec.type != EditRecordType::VAR_ENTRY)
            continue;

        // TODO: Improve from O(nlogn) to O(n).

        VersionedPageEntriesPtr entries;
        {
            std::shared_lock read_lock(table_rw_mutex);
            auto iter = mvcc_table_directory.find(rec.page_id);
            if (iter == mvcc_table_directory.end())
                // There may be obsolete entries deleted.
                // For example, if there is a `Put 1` with sequence 10, `Del 1` with sequence 11,
                // and the snapshot sequence is 12, Page with id 1 may be deleted by the gc process.
                continue;
            entries = iter->second;
        }

        entries->copyCheckpointInfoFromEdit(rec);
        num_copied += 1;
    }
    return num_copied;
}

template <typename Trait>
typename PageDirectory<Trait>::PageEntries PageDirectory<Trait>::gcInMemEntries(const InMemGCOption & options)
    NO_THREAD_SAFETY_ANALYSIS
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
        std::unordered_set<String> tracing_id_set;
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
                    if (!tracing_id_set.contains(snap->tracing_id))
                    {
                        LOG_WARNING(
                            log,
                            "Meet a stale snapshot, create_thread={} tracing_id={} seq={} alive_time={:.3f}",
                            snap->create_thread,
                            snap->tracing_id,
                            snap->sequence,
                            alive_time_seconds);
                        tracing_id_set.emplace(snap->tracing_id);
                    }
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

    SYNC_FOR("after_PageDirectory::doGC_getLowestSeq");

    PageEntriesV3 all_del_entries;
    typename MVCCMapType::iterator iter;
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
    std::map<PageId, std::pair<PageVersion, Int64>> normal_entries_to_deref;
    // Iterate all page_id and try to clean up useless var entries
    while (true)
    {
        // `iter` is an iter that won't be invalid cause by `apply`/`gcApply`.
        // do gc on the version list without lock on `mvcc_table_directory`.
        const bool all_deleted = iter->second->cleanOutdatedEntries(
            lowest_seq,
            &normal_entries_to_deref,
            options.need_removed_entries ? &all_del_entries : nullptr,
            options.remote_valid_sizes,
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
        typename MVCCMapType::iterator iter;
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
            options.need_removed_entries ? &all_del_entries : nullptr);

        if (all_deleted)
        {
            std::unique_lock write_lock(table_rw_mutex);
            mvcc_table_directory.erase(iter);
            invalid_page_nums++;
            valid_page_nums--;
        }
    }

    auto log_level = stale_snapshot_nums > 0 ? Poco::Message::PRIO_INFORMATION : Poco::Message::PRIO_DEBUG;
    LOG_IMPL(
        log,
        log_level,
        "After MVCC gc in memory [lowest_seq={}] "
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

template <typename Trait>
typename PageDirectory<Trait>::PageEntriesEdit PageDirectory<Trait>::dumpSnapshotToEdit(PageDirectorySnapshotPtr snap)
{
    if (!snap)
    {
        snap = createSnapshot(/*tracing_id*/ "");
    }

    PageEntriesEdit edit;

    PageId iter_k;
    VersionedPageEntriesPtr iter_v;
    {
        std::shared_lock read_lock(table_rw_mutex);
        auto iter = mvcc_table_directory.begin();
        if (iter == mvcc_table_directory.end())
            return edit;
        iter_k = iter->first;
        iter_v = iter->second;
    }
    while (true)
    {
        iter_v->collapseTo(snap->sequence, iter_k, edit);

        {
            std::shared_lock read_lock(table_rw_mutex);
            auto iter = mvcc_table_directory.upper_bound(iter_k);
            if (iter == mvcc_table_directory.end())
                break;
            iter_k = iter->first;
            iter_v = iter->second;
        }
    }

    LOG_INFO(log, "Dumped snapshot to edits, sequence={} edit_size={}", snap->sequence, edit.size());
    return edit;
}

template <typename Trait>
size_t PageDirectory<Trait>::numPagesWithPrefix(const String & prefix) const
{
    if constexpr (std::is_same_v<Trait, universal::PageDirectoryTrait>)
    {
        std::shared_lock read_lock(table_rw_mutex);
        size_t num = 0;
        for (auto iter = mvcc_table_directory.lower_bound(prefix); iter != mvcc_table_directory.end(); ++iter)
        {
            if (!iter->first.hasPrefix(prefix))
                break;
            num++;
        }
        return num;
    }
    else
    {
        throw Exception("", ErrorCodes::NOT_IMPLEMENTED);
    }
}

template class VersionedPageEntries<u128::PageDirectoryTrait>;
template class VersionedPageEntries<universal::PageDirectoryTrait>;

template class PageDirectory<u128::PageDirectoryTrait>;
template class PageDirectory<universal::PageDirectoryTrait>;

} // namespace PS::V3
} // namespace DB
