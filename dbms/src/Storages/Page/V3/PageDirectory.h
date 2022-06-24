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

#pragma once

#include <Common/CurrentMetrics.h>
#include <Common/Logger.h>
#include <Common/nocopyable.h>
#include <Encryption/FileProvider.h>
#include <Poco/Ext/ThreadNumber.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/MapUtils.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/WALStore.h>
#include <common/types.h>

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

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

    ~PageDirectorySnapshot()
    {
        CurrentMetrics::sub(CurrentMetrics::PSMVCCNumSnapshots);
    }

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

struct EntryOrDelete
{
    bool is_delete;
    Int64 being_ref_count = 1;
    PageEntryV3 entry;

    static EntryOrDelete newDelete()
    {
        return EntryOrDelete{
            .is_delete = true,
            .being_ref_count = 1, // meaningless
            .entry = {}, // meaningless
        };
    }
    static EntryOrDelete newNormalEntry(const PageEntryV3 & entry)
    {
        return EntryOrDelete{
            .is_delete = false,
            .being_ref_count = 1,
            .entry = entry,
        };
    }
    static EntryOrDelete newRepalcingEntry(const EntryOrDelete & ori_entry, const PageEntryV3 & entry)
    {
        return EntryOrDelete{
            .is_delete = false,
            .being_ref_count = ori_entry.being_ref_count,
            .entry = entry,
        };
    }

    static EntryOrDelete newFromRestored(PageEntryV3 entry, Int64 being_ref_count)
    {
        return EntryOrDelete{
            .is_delete = false,
            .being_ref_count = being_ref_count,
            .entry = entry,
        };
    }

    bool isDelete() const { return is_delete; }
    bool isEntry() const { return !is_delete; }

    String toDebugString() const
    {
        return fmt::format(
            "{{is_delete:{}, entry:{}, being_ref_count:{}}}",
            is_delete,
            ::DB::PS::V3::toDebugString(entry),
            being_ref_count);
    }
};

class VersionedPageEntries;
using VersionedPageEntriesPtr = std::shared_ptr<VersionedPageEntries>;
using PageLock = std::lock_guard<std::mutex>;
class VersionedPageEntries
{
public:
    VersionedPageEntries()
        : type(EditRecordType::VAR_DELETE)
        , is_deleted(false)
        , create_ver(0)
        , delete_ver(0)
        , ori_page_id(0)
        , being_ref_count(1)
    {}

    [[nodiscard]] PageLock acquireLock() const
    {
        return std::lock_guard(m);
    }

    void createNewEntry(const PageVersion & ver, const PageEntryV3 & entry);

    bool createNewRef(const PageVersion & ver, PageIdV3Internal ori_page_id);

    std::shared_ptr<PageIdV3Internal> createNewExternal(const PageVersion & ver);

    void createDelete(const PageVersion & ver);

    std::shared_ptr<PageIdV3Internal> fromRestored(const PageEntriesEdit::EditRecord & rec);

    enum ResolveResult
    {
        RESOLVE_FAIL,
        RESOLVE_TO_REF,
        RESOLVE_TO_NORMAL,
    };
    std::tuple<ResolveResult, PageIdV3Internal, PageVersion>
    resolveToPageId(UInt64 seq, bool check_prev, PageEntryV3 * entry);

    Int64 incrRefCount(const PageVersion & ver);

    std::optional<PageEntryV3> getEntry(UInt64 seq) const;

    std::optional<PageEntryV3> getLastEntry() const;

    bool isVisible(UInt64 seq) const;

    /**
     * If there are entries point to file in `blob_ids`, take out the <page_id, ver, entry> and
     * store them into `blob_versioned_entries`.
     * Return the total size of entries in this version list.
     */
    PageSize getEntriesByBlobIds(
        const std::unordered_set<BlobFileId> & blob_ids,
        PageIdV3Internal page_id,
        std::map<BlobFileId, PageIdAndVersionedEntries> & blob_versioned_entries);

    /**
     * GC will give a `lowest_seq`.
     * We will find the second entry which `LE` than `lowest_seq`.
     * And reclaim all entries before that one.
     * If we can't found any entry less than `lowest_seq`.
     * Then all entries will be remained.
     * 
     * Ex1. 
     *    entry 1 : seq 2 epoch 0
     *    entry 2 : seq 2 epoch 1
     *    entry 3 : seq 3 epoch 0
     *    entry 4 : seq 4 epoch 0
     * 
     *    lowest_seq : 3
     *    Then (entry 1, entry 2) will be delete.
     * 
     * Ex2. 
     *    entry 1 : seq 2 epoch 0
     *    entry 2 : seq 2 epoch 1
     *    entry 3 : seq 4 epoch 0
     *    entry 4 : seq 4 epoch 1
     * 
     *    lowest_seq : 3
     *    Then (entry 1) will be delete
     * 
     * Ex3. 
     *    entry 1 : seq 2 epoch 0
     *    entry 2 : seq 2 epoch 1
     *    entry 3 : seq 4 epoch 0
     *    entry 4 : seq 4 epoch 1
     * 
     *    lowest_seq : 1
     *    Then no entry should be delete.
     */
    bool cleanOutdatedEntries(
        UInt64 lowest_seq,
        std::map<PageIdV3Internal, std::pair<PageVersion, Int64>> * normal_entries_to_deref,
        PageEntriesV3 * entries_removed,
        const PageLock & page_lock);
    bool derefAndClean(
        UInt64 lowest_seq,
        PageIdV3Internal page_id,
        const PageVersion & deref_ver,
        Int64 deref_count,
        PageEntriesV3 * entries_removed);

    void collapseTo(UInt64 seq, PageIdV3Internal page_id, PageEntriesEdit & edit);

    size_t size() const
    {
        auto lock = acquireLock();
        return entries.size();
    }

    String toDebugString() const
    {
        return fmt::format(
            "{{"
            "type:{}, create_ver: {}, is_deleted: {}, delete_ver: {}, "
            "ori_page_id: {}, being_ref_count: {}, num_entries: {}"
            "}}",
            type,
            create_ver,
            is_deleted,
            delete_ver,
            ori_page_id,
            being_ref_count,
            entries.size());
    }
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
    PageIdV3Internal ori_page_id;
    // Being ref counter, valid when type == VAR_EXTERNAL
    Int64 being_ref_count;
    // A shared ptr to a holder, valid when type == VAR_EXTERNAL
    std::shared_ptr<PageIdV3Internal> external_holder;
};

// `PageDirectory` store multi-versions entries for the same
// page id. User can acquire a snapshot from it and get a
// consist result by the snapshot.
// All its functions are consider concurrent safe.
// User should call `gc` periodic to remove outdated version
// of entries in order to keep the memory consumption as well
// as the restoring time in a reasonable level.
class PageDirectory;
using PageDirectoryPtr = std::unique_ptr<PageDirectory>;
class PageDirectory
{
public:
    explicit PageDirectory(String storage_name, WALStorePtr && wal, UInt64 max_persisted_log_files_ = MAX_PERSISTED_LOG_FILES);

    PageDirectorySnapshotPtr createSnapshot(const String & tracing_id = "") const;

    SnapshotsStatistics getSnapshotsStat() const;

    PageIDAndEntryV3 get(PageIdV3Internal page_id, const PageDirectorySnapshotPtr & snap, bool throw_on_not_exist = true) const;
    PageIDAndEntryV3 get(PageIdV3Internal page_id, const DB::PageStorageSnapshotPtr & snap) const
    {
        return get(page_id, toConcreteSnapshot(snap), /*throw_on_not_exist=*/true);
    }
    PageIDAndEntryV3 getOrNull(PageIdV3Internal page_id, const DB::PageStorageSnapshotPtr & snap) const
    {
        return get(page_id, toConcreteSnapshot(snap), /*throw_on_not_exist=*/false);
    }

    std::pair<PageIDAndEntriesV3, PageIds> get(const PageIdV3Internals & page_ids, const PageDirectorySnapshotPtr & snap, bool throw_on_not_exist = true) const;
    PageIDAndEntriesV3 get(const PageIdV3Internals & page_ids, const DB::PageStorageSnapshotPtr & snap) const
    {
        return std::get<0>(get(page_ids, toConcreteSnapshot(snap), /*throw_on_not_exist=*/true));
    }
    std::pair<PageIDAndEntriesV3, PageIds> getOrNull(PageIdV3Internals page_ids, const DB::PageStorageSnapshotPtr & snap) const
    {
        return get(page_ids, toConcreteSnapshot(snap), /*throw_on_not_exist=*/false);
    }

    PageIdV3Internal getNormalPageId(PageIdV3Internal page_id, const PageDirectorySnapshotPtr & snap, bool throw_on_not_exist) const;
    PageIdV3Internal getNormalPageId(PageIdV3Internal page_id, const DB::PageStorageSnapshotPtr & snap, bool throw_on_not_exist) const
    {
        return getNormalPageId(page_id, toConcreteSnapshot(snap), throw_on_not_exist);
    }

#ifndef NDEBUG
    // Just for tests, refactor them out later
    PageIDAndEntryV3 get(PageId page_id, const PageDirectorySnapshotPtr & snap) const
    {
        return get(buildV3Id(TEST_NAMESPACE_ID, page_id), snap);
    }
    PageIDAndEntryV3 get(PageId page_id, const DB::PageStorageSnapshotPtr & snap) const
    {
        return get(buildV3Id(TEST_NAMESPACE_ID, page_id), toConcreteSnapshot(snap));
    }
    PageIdV3Internal getNormalPageId(PageId page_id, const PageDirectorySnapshotPtr & snap) const
    {
        return getNormalPageId(buildV3Id(TEST_NAMESPACE_ID, page_id), snap, /*throw_on_not_exist*/ true);
    }
    PageIdV3Internal getNormalPageId(PageId page_id, const DB::PageStorageSnapshotPtr & snap) const
    {
        return getNormalPageId(buildV3Id(TEST_NAMESPACE_ID, page_id), toConcreteSnapshot(snap), /*throw_on_not_exist*/ true);
    }
#endif

    PageId getMaxId() const;

    std::set<PageIdV3Internal> getAllPageIds();

    void apply(PageEntriesEdit && edit, const WriteLimiterPtr & write_limiter = nullptr);

    std::pair<std::map<BlobFileId, PageIdAndVersionedEntries>, PageSize>
    getEntriesByBlobIds(const std::vector<BlobFileId> & blob_ids) const;

    void gcApply(PageEntriesEdit && migrated_edit, const WriteLimiterPtr & write_limiter = nullptr);

    bool tryDumpSnapshot(const ReadLimiterPtr & read_limiter = nullptr, const WriteLimiterPtr & write_limiter = nullptr);

    // Perform a GC for in-memory entries and return the removed entries.
    // If `return_removed_entries` is false, then just return an empty set.
    PageEntriesV3 gcInMemEntries(bool return_removed_entries = true);

    std::set<PageId> getAliveExternalIds(NamespaceId ns_id) const;

    PageEntriesEdit dumpSnapshotToEdit(PageDirectorySnapshotPtr snap = nullptr);

    size_t numPages() const
    {
        std::shared_lock read_lock(table_rw_mutex);
        return mvcc_table_directory.size();
    }

    // No copying and no moving
    DISALLOW_COPY_AND_MOVE(PageDirectory);

    friend class PageDirectoryFactory;
    friend class PageStorageControlV3;

private:
    // Only `std::map` is allow for `MVCCMap`. Cause `std::map::insert` ensure that
    // "No iterators or references are invalidated"
    // https://en.cppreference.com/w/cpp/container/map/insert
    using MVCCMapType = std::map<PageIdV3Internal, VersionedPageEntriesPtr>;

    static void applyRefEditRecord(
        MVCCMapType & mvcc_table_directory,
        const VersionedPageEntriesPtr & version_list,
        const PageEntriesEdit::EditRecord & rec,
        const PageVersion & version);

    static inline PageDirectorySnapshotPtr
    toConcreteSnapshot(const DB::PageStorageSnapshotPtr & ptr)
    {
        return std::static_pointer_cast<PageDirectorySnapshot>(ptr);
    }

private:
    PageId max_page_id;
    std::atomic<UInt64> sequence;
    mutable std::shared_mutex table_rw_mutex;
    MVCCMapType mvcc_table_directory;

    mutable std::mutex snapshots_mutex;
    mutable std::list<std::weak_ptr<PageDirectorySnapshot>> snapshots;

    mutable std::mutex external_ids_mutex;
    mutable std::list<std::weak_ptr<PageIdV3Internal>> external_ids;

    WALStorePtr wal;
    const UInt64 max_persisted_log_files;
    LoggerPtr log;
};

} // namespace DB::PS::V3
