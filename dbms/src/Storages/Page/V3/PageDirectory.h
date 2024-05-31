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

#pragma once

#include <Common/CurrentMetrics.h>
#include <Common/Logger.h>
#include <Common/nocopyable.h>
#include <Poco/Ext/ThreadNumber.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/CheckpointFile/CPDataFileStat.h>
#include <Storages/Page/V3/MapUtils.h>
#include <Storages/Page/V3/PageDefines.h>
#include <Storages/Page/V3/PageDirectory/ExternalIdsByNamespace.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/WAL/serialize.h>
#include <Storages/Page/V3/WALStore.h>
#include <common/defines.h>
#include <common/types.h>

#include <magic_enum.hpp>
#include <memory>
#include <mutex>
#include <shared_mutex>

namespace CurrentMetrics
{
extern const Metric PSMVCCNumSnapshots;
} // namespace CurrentMetrics

namespace DB::PS::V3
{

class PageDirectorySnapshot : public DB::PageStorageSnapshot
{
public:
    using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;

    explicit PageDirectorySnapshot(UInt64 seq, const String & tracing_id_)
        : sequence(seq)
        , create_thread(Poco::ThreadNumber::get())
        , tracing_id(tracing_id_)
        , create_time(std::chrono::steady_clock::now())
    {
        CurrentMetrics::add(CurrentMetrics::PSMVCCNumSnapshots);
    }

    ~PageDirectorySnapshot() override { CurrentMetrics::sub(CurrentMetrics::PSMVCCNumSnapshots); }

    double elapsedSeconds() const
    {
        auto end = std::chrono::steady_clock::now();
        std::chrono::duration<double> diff = end - create_time;
        return diff.count();
    }

public:
    const UInt64 sequence;
    const unsigned create_thread;
    const String tracing_id;

private:
    const TimePoint create_time;
};
using PageDirectorySnapshotPtr = std::shared_ptr<PageDirectorySnapshot>;

// MultiVersionRefCount store the ref count for each version of a page.
// This is not a thread safe class, it must be used with outer synchronization.
class MultiVersionRefCount
{
public:
    MultiVersionRefCount() = default;

    MultiVersionRefCount(const MultiVersionRefCount & other)
    {
        if (other.versioned_ref_counts)
        {
            versioned_ref_counts = std::make_unique<std::vector<std::pair<PageVersion, Int64>>>();
            for (const auto & [ver, ref_count] : *(other.versioned_ref_counts))
            {
                versioned_ref_counts->emplace_back(ver, ref_count);
            }
        }
        else
        {
            versioned_ref_counts = nullptr;
        }
    }

    void restoreFrom(const PageVersion & ver, Int64 ref_count)
    {
        // versioned_ref_counts being nullptr always means ref count 1
        RUNTIME_CHECK(ref_count > 0);
        RUNTIME_CHECK(!versioned_ref_counts);
        if (ref_count == 1)
        {
            return;
        }
        versioned_ref_counts = std::make_unique<std::vector<std::pair<PageVersion, Int64>>>();
        // empty `versioned_ref_counts` means ref count 1, so the ref count delta here is ref_count - 1
        versioned_ref_counts->emplace_back(ver, ref_count - 1);
    }

    void incrRefCount(const PageVersion & ver, Int64 ref_count_delta)
    {
        if (!versioned_ref_counts)
        {
            versioned_ref_counts = std::make_unique<std::vector<std::pair<PageVersion, Int64>>>();
        }
        versioned_ref_counts->emplace_back(ver, ref_count_delta);
    }

    void decrRefCountInSnap(UInt64 snap_seq, Int64 deref_count_delta)
    {
        if (snap_seq == 0)
        {
            // When `snap_seq` is 0, it means the deref operation is caused by rewritting a ref page to a normal page.
            // Actually we can collapse versioned_ref_counts here too because there should be no other gc happend concurrently.
            // But collpase it when `snap_seq` is not zero should be enough. So we just do an append here.
            versioned_ref_counts->emplace_back(PageVersion(0, 0), -deref_count_delta);
        }
        else
        {
            auto new_versioned_ref_counts = std::make_unique<std::vector<std::pair<PageVersion, Int64>>>();
            Int64 ref_count_delta_in_snap = -deref_count_delta;
            for (const auto & [ver, ref_count_delta] : *versioned_ref_counts)
            {
                if (ver.sequence <= snap_seq)
                {
                    ref_count_delta_in_snap += ref_count_delta;
                }
                else
                {
                    new_versioned_ref_counts->emplace_back(ver, ref_count_delta);
                }
            }
            if (ref_count_delta_in_snap == 0)
            {
                if (new_versioned_ref_counts->empty())
                {
                    versioned_ref_counts = nullptr;
                }
                else
                {
                    // There could be some new ref count created after `snap_seq`, we need to
                    // keep the newly added ref counts
                    versioned_ref_counts.swap(new_versioned_ref_counts);
                }
                return;
            }
            if unlikely (ref_count_delta_in_snap <= 0)
            {
                FmtBuffer buf;
                for (const auto & [ver, ref_count_delta] : *versioned_ref_counts)
                {
                    buf.fmtAppend("{}|{},", ver.sequence, ref_count_delta);
                }
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Check ref_count_delta_in_snap > 0 failed, deref_count_delta={}, ref_count_delta_in_snap={}, "
                    "snap_seq={}, versions={}",
                    deref_count_delta,
                    ref_count_delta_in_snap,
                    snap_seq,
                    buf.toString());
            }
            new_versioned_ref_counts->emplace_back(PageVersion(snap_seq, 0), ref_count_delta_in_snap);
            versioned_ref_counts.swap(new_versioned_ref_counts);
        }
    }

    Int64 getRefCountInSnap(UInt64 snap_seq) const
    {
        if (!versioned_ref_counts)
        {
            return 1;
        }
        return std::accumulate(
            versioned_ref_counts->begin(),
            versioned_ref_counts->end(),
            1,
            [&](Int64 acc, const auto & ver_ref_count) {
                return acc + (ver_ref_count.first.sequence <= snap_seq ? ver_ref_count.second : 0);
            });
    }

    Int64 getLatestRefCount() const
    {
        if (!versioned_ref_counts)
        {
            return 1;
        }
        return std::accumulate(
            versioned_ref_counts->begin(),
            versioned_ref_counts->end(),
            1,
            [](Int64 acc, const auto & ver_ref_count) { return acc + ver_ref_count.second; });
    }

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    // store the ref count delta for each version, empty means ref count 1
    std::unique_ptr<std::vector<std::pair<PageVersion, Int64>>> versioned_ref_counts;
};

struct EntryOrDelete
{
    MultiVersionRefCount being_ref_count;
    std::optional<PageEntryV3> entry;

    static EntryOrDelete newDelete()
    {
        return EntryOrDelete{
            .entry = std::nullopt,
        };
    };
    static EntryOrDelete newNormalEntry(const PageEntryV3 & entry)
    {
        return EntryOrDelete{
            .entry = entry,
        };
    }
    static EntryOrDelete newReplacingEntry(const EntryOrDelete & ori_entry, const PageEntryV3 & entry)
    {
        return EntryOrDelete{
            .being_ref_count = ori_entry.being_ref_count,
            .entry = entry,
        };
    }

    static EntryOrDelete newFromRestored(PageEntryV3 entry, const PageVersion & ver, Int64 being_ref_count)
    {
        auto result = EntryOrDelete{
            .entry = entry,
        };
        result.being_ref_count.restoreFrom(ver, being_ref_count);
        return result;
    }

    bool isDelete() const { return !entry.has_value(); }
    bool isEntry() const { return entry.has_value(); }
};

using PageLock = std::lock_guard<std::mutex>;

enum class ResolveResult
{
    FAIL,
    TO_REF,
    TO_NORMAL,
};

// VersionedPageEntries store multi-versions page entries for the same page id.
template <typename Trait>
class VersionedPageEntries
{
public:
    using PageId = typename Trait::PageId;
    using PageEntriesEdit = DB::PS::V3::PageEntriesEdit<PageId>;

    using GcEntries = std::vector<std::tuple<PageId, PageVersion, PageEntryV3>>;
    using GcEntriesMap = std::map<BlobFileId, GcEntries>;

public:
    VersionedPageEntries()
        : type(EditRecordType::VAR_DELETE)
        , is_deleted(false)
        , create_ver(0)
        , delete_ver(0)
        , ori_page_id{}
    {}

    bool isExternalPage() const { return type == EditRecordType::VAR_EXTERNAL; }

    [[nodiscard]] PageLock acquireLock() const;

    void createNewEntry(const PageVersion & ver, const PageEntryV3 & entry);

    // Commit the upsert entry after full gc.
    // Return a PageId, if the page id is valid, it means it rewrite a RefPage into
    // a normal Page. Caller must call `derefAndClean` to decrease the ref-count of
    // the returing page id.
    [[nodiscard]] PageId createUpsertEntry(const PageVersion & ver, const PageEntryV3 & entry, bool strict_check);

    bool createNewRef(const PageVersion & ver, const PageId & ori_page_id);

    std::shared_ptr<PageId> createNewExternal(const PageVersion & ver, const PageEntryV3 & entry);

    void createDelete(const PageVersion & ver);

    // Update the local cache info for remote page,
    // Must a hold snap to prevent the page being deleted.
    bool updateLocalCacheForRemotePage(const PageVersion & ver, const PageEntryV3 & entry);

    std::shared_ptr<PageId> fromRestored(const typename PageEntriesEdit::EditRecord & rec);

    std::tuple<ResolveResult, PageId, PageVersion> resolveToPageId(UInt64 seq, bool ignore_delete, PageEntryV3 * entry);

    Int64 incrRefCount(const PageVersion & target_ver, const PageVersion & ref_ver);

    std::optional<PageEntryV3> getEntry(UInt64 seq) const;

    std::optional<PageEntryV3> getLastEntry(std::optional<UInt64> seq) const;

    void copyCheckpointInfoFromEdit(const typename PageEntriesEdit::EditRecord & edit);

    bool isVisible(UInt64 seq) const;

    /**
     * If there are entries point to file in `blob_ids`, take out the <page_id, ver, entry> and
     * store them into `blob_versioned_entries`.
     * Return the total size of entries in this version list.
     */
    PageSize getEntriesByBlobIds(
        const std::unordered_set<BlobFileId> & blob_ids,
        const PageId & page_id,
        GcEntriesMap & blob_versioned_entries,
        std::map<PageId, std::tuple<PageId, PageVersion>> & ref_ids_maybe_rewrite);

    /**
     * Given a `lowest_seq`, this will clean all outdated entries before `lowest_seq`.
     * It takes good care of the entries being ref by another page id.
     *
     * `normal_entries_to_deref`: Return the informations that the entries need
     *   to be decreased the ref count by `derefAndClean`.
     *   The elem is <page_id, <version, num to decrease ref count>> 
     * `entries_removed`: Return the entries removed from the version list
     *
     * Return `true` iff this page can be totally removed from the whole `PageDirectory`.
     */
    [[nodiscard]] bool cleanOutdatedEntries(
        UInt64 lowest_seq,
        std::map<PageId, std::pair<PageVersion, Int64>> * normal_entries_to_deref,
        PageEntriesV3 * entries_removed,
        RemoteFileValidSizes * remote_file_sizes,
        const PageLock & page_lock);
    /**
     * Decrease the ref-count of entry with given `deref_ver`.
     * If `lowest_seq` != 0, then it will run `cleanOutdatedEntries` after decreasing
     * the ref-count.
     *
     * Return `true` iff this page can be totally removed from the whole `PageDirectory`.
     */
    [[nodiscard]] bool derefAndClean(
        UInt64 lowest_seq,
        const PageId & page_id,
        const PageVersion & deref_ver,
        Int64 deref_count,
        PageEntriesV3 * entries_removed);

    void collapseTo(UInt64 seq, const PageId & page_id, PageEntriesEdit & edit);

    size_t size() const;

    String toDebugString() const
    {
        return fmt::format(
            "{{"
            "type:{}, create_ver: {}, is_deleted: {}, delete_ver: {}, "
            "ori_page_id: {}, being_ref_count: {}, num_entries: {}"
            "}}",
            magic_enum::enum_name(type),
            create_ver,
            is_deleted,
            delete_ver,
            ori_page_id,
            being_ref_count.getLatestRefCount(),
            entries.size());
    }
    template <typename T>
    friend class PageStorageControlV3;

private:
    mutable std::mutex m;

    // Valid value of `type` is one of
    // - VAR_DELETE
    // - VAR_ENTRY
    // - VAR_REF
    // - VAR_EXTERNAL
    EditRecordType type;

    // Has been deleted, valid when type == VAR_REF/VAR_EXTERNAL
    bool is_deleted;
    // Entries sorted by version, valid when type == VAR_ENTRY
    std::multimap<PageVersion, EntryOrDelete> entries;
    // The created version, valid when type == VAR_REF/VAR_EXTERNAL
    PageVersion create_ver;
    // The deleted version, valid when type == VAR_REF/VAR_EXTERNAL && is_deleted = true
    PageVersion delete_ver;
    // Original page id, valid when type == VAR_REF
    PageId ori_page_id;
    // Being ref counter, valid when type == VAR_EXTERNAL
    MultiVersionRefCount being_ref_count;
    // A shared ptr to a holder, valid when type == VAR_EXTERNAL
    std::shared_ptr<PageId> external_holder;
};

// `PageDirectory` store VersionedPageEntries for all pages.
// User can acquire a snapshot from it and get a consist result by the snapshot.
// All its functions are consider concurrent safe.
// User should call `gc` periodic to remove outdated version
// of entries in order to keep the memory consumption as well
// as the restoring time in a reasonable level.
template <typename Trait>
class PageDirectory
{
public:
    using PageId = typename Trait::PageId;
    using PageEntriesEdit = DB::PS::V3::PageEntriesEdit<PageId>;

    using GcEntries = std::vector<std::tuple<PageId, PageVersion, PageEntryV3>>;
    using GcEntriesMap = std::map<BlobFileId, GcEntries>;

    using PageIdSet = std::set<PageId>;
    using PageIds = std::vector<PageId>;
    using PageEntries = std::vector<PageEntryV3>;
    using PageIdAndEntry = std::pair<PageId, PageEntryV3>;
    using PageIdAndEntries = std::vector<PageIdAndEntry>;

public:
    explicit PageDirectory(
        String storage_name,
        WALStorePtr && wal,
        UInt64 max_persisted_log_files_ = MAX_PERSISTED_LOG_FILES);

    PageDirectorySnapshotPtr createSnapshot(const String & tracing_id = "") const;

    SnapshotsStatistics getSnapshotsStat() const;

    PageIdAndEntry getByID(const PageId & page_id, const DB::PageStorageSnapshotPtr & snap) const
    {
        return getByIDImpl(page_id, toConcreteSnapshot(snap), /*throw_on_not_exist=*/true);
    }
    PageIdAndEntry getByIDOrNull(const PageId & page_id, const DB::PageStorageSnapshotPtr & snap) const
    {
        return getByIDImpl(page_id, toConcreteSnapshot(snap), /*throw_on_not_exist=*/false);
    }

    PageIdAndEntries getByIDs(const PageIds & page_ids, const DB::PageStorageSnapshotPtr & snap) const
    {
        return std::get<0>(getByIDsImpl(page_ids, toConcreteSnapshot(snap), /*throw_on_not_exist=*/true));
    }
    std::pair<PageIdAndEntries, PageIds> getByIDsOrNull(
        const PageIds & page_ids,
        const DB::PageStorageSnapshotPtr & snap) const
    {
        return getByIDsImpl(page_ids, toConcreteSnapshot(snap), /*throw_on_not_exist=*/false);
    }

    PageId getNormalPageId(const PageId & page_id, const DB::PageStorageSnapshotPtr & snap_, bool throw_on_not_exist)
        const;

    UInt64 getMaxIdAfterRestart() const;

    PageIdSet getAllPageIds();

    PageIdSet getAllPageIdsWithPrefix(const String & prefix, const DB::PageStorageSnapshotPtr & snap_);

    // end is infinite if empty
    PageIdSet getAllPageIdsInRange(const PageId & start, const PageId & end, const DB::PageStorageSnapshotPtr & snap_);

    std::optional<PageId> getLowerBound(const PageId & start, const DB::PageStorageSnapshotPtr & snap_);

    // Apply the edit into PageDirectory.
    // If there are CheckpointInfo along with the applied edit, this function will
    // returns the applied data file ids.
    std::unordered_set<String> apply(PageEntriesEdit && edit, const WriteLimiterPtr & write_limiter = nullptr);

    // return ignored entries, and the corresponding space in BlobFile should be reclaimed
    PageEntries updateLocalCacheForRemotePages(
        PageEntriesEdit && edit,
        const DB::PageStorageSnapshotPtr & snap_,
        const WriteLimiterPtr & write_limiter = nullptr);

    std::pair<GcEntriesMap, PageSize> getEntriesByBlobIds(const std::vector<BlobFileId> & blob_ids) const;

    using PageTypeAndBlobIds = std::map<PageType, std::vector<BlobFileId>>;
    using PageTypeAndGcInfo = std::vector<std::tuple<PageType, GcEntriesMap, PageSize>>;
    PageTypeAndGcInfo getEntriesByBlobIdsForDifferentPageTypes(const PageTypeAndBlobIds & page_type_and_blob_ids) const;

    void gcApply(PageEntriesEdit && migrated_edit, const WriteLimiterPtr & write_limiter = nullptr);

    bool tryDumpSnapshot(const WriteLimiterPtr & write_limiter = nullptr, bool force = false);

    size_t copyCheckpointInfoFromEdit(const PageEntriesEdit & edit);

    struct InMemGCOption
    {
        // If true, gcInMemEntries will return the removed entries.
        // If false, just return an empty set to reduce the memory
        // and CPU overhead.
        bool need_removed_entries = true;
        // collect the valid size of remote ids if not nullptr
        RemoteFileValidSizes * remote_valid_sizes = nullptr;
    };
    // Perform a GC for in-memory entries
    PageEntries gcInMemEntries(const InMemGCOption & options);

    // Get the external id that is not deleted or being ref by another id by
    // `ns_id`.
    std::optional<std::set<PageIdU64>> getAliveExternalIds(const typename Trait::PageIdTrait::Prefix & ns_id) const
    {
        return external_ids_by_ns.getAliveIds(ns_id);
    }

    // After table dropped, the `getAliveIds` with specified
    // `ns_id` will not be cleaned. We need this method to
    // cleanup all external id ptrs.
    void unregisterNamespace(const typename Trait::PageIdTrait::Prefix & ns_id)
    {
        external_ids_by_ns.unregisterNamespace(ns_id);
    }

    PageEntriesEdit dumpSnapshotToEdit(PageDirectorySnapshotPtr snap = nullptr);

    // Approximate number of pages in memory
    size_t numPages() const
    {
        std::shared_lock read_lock(table_rw_mutex);
        return mvcc_table_directory.size();
    }
    // Only used in test
    size_t numPagesWithPrefix(const String & prefix) const;

    FileUsageStatistics getFileUsageStatistics() const
    {
        auto u = wal->getFileUsageStatistics();
        u.num_pages = numPages();
        return u;
    }

    // `writers` should be used under the protection of apply_mutex
    // So don't use this function in production code
    size_t getWritersQueueSizeForTest() { return writers.size(); }

    // No copying and no moving
    DISALLOW_COPY_AND_MOVE(PageDirectory);

    template <typename>
    friend class PageDirectoryFactory;
    template <typename>
    friend class PageStorageControlV3;

private:
    PageIdAndEntry getByIDImpl(const PageId & page_id, const PageDirectorySnapshotPtr & snap, bool throw_on_not_exist)
        const;
    std::pair<PageIdAndEntries, PageIds> getByIDsImpl(
        const PageIds & page_ids,
        const PageDirectorySnapshotPtr & snap,
        bool throw_on_not_exist) const;

private:
    // Only `std::map` is allow for `MVCCMap`. Cause `std::map::insert` ensure that
    // "No iterators or references are invalidated"
    // https://en.cppreference.com/w/cpp/container/map/insert
    using VersionedPageEntriesPtr = std::shared_ptr<VersionedPageEntries<Trait>>;
    using MVCCMapType = std::map<PageId, VersionedPageEntriesPtr>;

    static void applyRefEditRecord(
        MVCCMapType & mvcc_table_directory,
        const VersionedPageEntriesPtr & version_list,
        const typename PageEntriesEdit::EditRecord & rec,
        const PageVersion & version);

    static inline PageDirectorySnapshotPtr toConcreteSnapshot(const DB::PageStorageSnapshotPtr & ptr)
    {
        return std::static_pointer_cast<PageDirectorySnapshot>(ptr);
    }

    struct Writer
    {
        PageEntriesEdit * edit;
        bool done = false; // The work has been performed by other thread
        bool success = false; // The work complete successfully
        std::unique_ptr<DB::Exception> exception;
        std::condition_variable cv;
    };

    // Return the last writer in the group
    // All the edit in the write group will be merged into `first->edit`.
    Writer * buildWriteGroup(Writer * first, std::unique_lock<std::mutex> & /*lock*/);

private:
    // max page id after restart(just used for table storage).
    // it may be for the whole instance or just for some specific prefix which is depending on the Trait passed.
    // Keeping it up to date is costly but useless, so it is not updated after restarting. Do NOT rely on it
    // except for specific situations
    UInt64 max_page_id;
    std::atomic<UInt64> sequence;

    // Used for avoid concurrently apply edits to wal and mvcc_table_directory.
    mutable std::mutex apply_mutex;
    // This is a queue of Writers to PageDirectory and is protected by apply_mutex.
    // Every writer enqueue itself to this queue before writing.
    // And the head writer of the queue will become the leader and is responsible to write and sync the WAL.
    // The write process of the leader:
    //   1. scan the queue to find all available writers and merge their edits to the leader's edit;
    //   2. unlock the apply_mutex;
    //   3. write the edits to the WAL and sync it;
    //   4. apply the edit to mvcc_table_directory;
    //   5. lock the apply_mutex;
    //   6. dequeue the writers found in step 1 and notify them that their write work has completed;
    //   7. if the writer queue is not empty, notify the head writer to become the leader of next write;
    // Other writers in the queue just wait the leader to wake them up and one of the two conditions must be true:
    //   1. its work has been finished by the leader, and they can just return;
    //   2. it becomes the head of the queue, so it continue to finish the write process of the leader;
    std::deque<Writer *> writers;

    // Used to protect mvcc_table_directory between apply threads and read threads
    mutable std::shared_mutex table_rw_mutex;
    MVCCMapType mvcc_table_directory;

    mutable std::mutex snapshots_mutex;
    mutable std::list<std::weak_ptr<PageDirectorySnapshot>> snapshots;

    mutable ExternalIdsByNamespace<typename Trait::PageIdTrait> external_ids_by_ns;

    WALStorePtr wal;
    const UInt64 max_persisted_log_files;
    LoggerPtr log;
};


namespace details
{
template <typename Trait>
UInt64 getMaxSequenceForRecord(const String & record)
{
    auto edit = Trait::Serializer::deserializeFrom(record, nullptr);
    const auto & records = edit.getRecords();
    RUNTIME_CHECK(!records.empty(), record.size());
    return records.back().version.sequence;
}
} // namespace details

namespace u128
{
struct PageDirectoryTrait
{
    using PageId = PageIdV3Internal;
    using PageIdTrait = PageIdTrait;
    using Serializer = Serializer;
};
using PageDirectoryType = PageDirectory<DB::PS::V3::u128::PageDirectoryTrait>;
using PageDirectoryPtr = std::unique_ptr<PageDirectoryType>;
using VersionedPageEntries = DB::PS::V3::VersionedPageEntries<PageDirectoryTrait>;
using VersionedPageEntriesPtr = std::shared_ptr<VersionedPageEntries>;
} // namespace u128
namespace universal
{
struct PageDirectoryTrait
{
    using PageId = UniversalPageId;
    using PageIdTrait = PageIdTrait;
    using Serializer = Serializer;
};
using PageDirectoryType = PageDirectory<DB::PS::V3::universal::PageDirectoryTrait>;
using PageDirectoryPtr = std::unique_ptr<PageDirectoryType>;
using VersionedPageEntries = DB::PS::V3::VersionedPageEntries<PageDirectoryTrait>;
using VersionedPageEntriesPtr = std::shared_ptr<VersionedPageEntries>;
} // namespace universal
} // namespace DB::PS::V3


template <>
struct fmt::formatter<DB::PS::V3::EntryOrDelete>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::PS::V3::EntryOrDelete & entry, FormatContext & ctx) const
    {
        return fmt::format_to(
            ctx.out(),
            "{{is_delete:{}, entry:{}, being_ref_count:{}}}",
            entry.isDelete(),
            entry.entry,
            entry.being_ref_count.getLatestRefCount());
    }
};
