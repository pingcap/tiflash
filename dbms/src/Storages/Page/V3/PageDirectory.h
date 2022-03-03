#pragma once

#include <Common/CurrentMetrics.h>
#include <Common/LogWithPrefix.h>
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

    UInt64 sequence;
    explicit PageDirectorySnapshot(UInt64 seq)
        : sequence(seq)
        , t_id(Poco::ThreadNumber::get())
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

    unsigned getTid() const
    {
        return t_id;
    }

private:
    const unsigned t_id;
    const TimePoint create_time;
};
using PageDirectorySnapshotPtr = std::shared_ptr<PageDirectorySnapshot>;

class VersionedPageEntries;
using VersionedPageEntriesPtr = std::shared_ptr<VersionedPageEntries>;

using PageLock = std::unique_ptr<std::lock_guard<std::mutex>>;
class VersionedPageEntries
{
public:
    [[nodiscard]] PageLock acquireLock() const
    {
        return std::make_unique<std::lock_guard<std::mutex>>(m);
    }

    void createNewEntry(const PageVersionType & ver, const PageEntryV3 & entry);

    bool createNewRef(const PageVersionType & ver, PageId ori_page_id);

    std::shared_ptr<PageId> createNewExternal(const PageVersionType & ver);

    void createDelete(const PageVersionType & ver)
    {
        auto page_lock = acquireLock();
        if (entries.empty() || !entries.rbegin()->second.isDelete())
        {
            entries.emplace(ver, VarEntry::newDelete());
        }
    }

    void fromRestored(const PageVersionType & ver, EditRecordType type, const PageEntryV3 & entry, PageId ori_page_id, Int64 being_ref_count)
    {
        auto page_lock = acquireLock();
        assert(entries.empty());
        entries.emplace(ver, VarEntry::fromRestored(type, entry, ori_page_id, being_ref_count));
    }

    enum ResolveResult
    {
        RESOLVE_FAIL,
        RESOLVE_TO_REF,
        RESOLVE_TO_NORMAL,
    };
    std::tuple<ResolveResult, PageId, PageVersionType>
    resolveToPageId(UInt64 seq, bool check_prev, PageEntryV3 * entry);

    Int64 incrRefCount(const PageVersionType & ver);

    std::optional<PageEntryV3> getEntry(UInt64 seq) const;

    /**
     * If there are entries point to file in `blob_ids`, take out the <page_id, ver, entry> and
     * store them into `blob_versioned_entries`.
     * Return the total size of entries in this version list.
     */
    PageSize getEntriesByBlobIds(
        const std::unordered_set<BlobFileId> & blob_ids,
        PageId page_id,
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
        PageId page_id,
        std::map<PageId, std::pair<PageVersionType, Int64>> * normal_entries_to_deref,
        PageEntriesV3 & entries_removed,
        const PageLock & page_lock);
    bool derefAndClean(
        UInt64 lowest_seq,
        PageId page_id,
        const PageVersionType & deref_ver,
        Int64 deref_count,
        PageEntriesV3 & entries_removed);

    void collapseTo(UInt64 seq, PageId page_id, PageEntriesEdit & edit);

    size_t size() const
    {
        auto lock = acquireLock();
        return entries.size();
    }

private:
    mutable std::mutex m;
    // Entries sorted by version
    std::multimap<PageVersionType, VarEntry> entries;
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
    explicit PageDirectory(WALStorePtr && wal);

    PageDirectorySnapshotPtr createSnapshot() const;

    std::tuple<size_t, double, unsigned> getSnapshotsStat() const;

    PageIDAndEntryV3 get(PageId page_id, const PageDirectorySnapshotPtr & snap) const;
    PageIDAndEntryV3 get(PageId page_id, const DB::PageStorageSnapshotPtr & snap) const
    {
        return get(page_id, toConcreteSnapshot(snap));
    }

    PageIDAndEntriesV3 get(const PageIds & page_ids, const PageDirectorySnapshotPtr & snap) const;
    PageIDAndEntriesV3 get(const PageIds & page_ids, const DB::PageStorageSnapshotPtr & snap) const
    {
        return get(page_ids, toConcreteSnapshot(snap));
    }

    PageId getNormalPageId(PageId page_id, const PageDirectorySnapshotPtr & snap) const;
    PageId getNormalPageId(PageId page_id, const DB::PageStorageSnapshotPtr & snap) const
    {
        return getNormalPageId(page_id, toConcreteSnapshot(snap));
    }

    PageId getMaxId() const;

    std::set<PageId> getAllPageIds();

    void apply(PageEntriesEdit && edit, const WriteLimiterPtr & write_limiter = nullptr);

    std::pair<std::map<BlobFileId, PageIdAndVersionedEntries>, PageSize>
    getEntriesByBlobIds(const std::vector<BlobFileId> & blob_ids) const;

    void gcApply(PageEntriesEdit && migrated_edit, const WriteLimiterPtr & write_limiter = nullptr);

    PageEntriesV3 gc(const WriteLimiterPtr & write_limiter = nullptr);

    std::set<PageId> getAliveExternalIds() const;

    PageEntriesEdit dumpSnapshotToEdit();

    size_t numPages() const
    {
        std::shared_lock read_lock(table_rw_mutex);
        return mvcc_table_directory.size();
    }

    // No copying
    PageDirectory(const PageDirectory &) = delete;
    PageDirectory & operator=(const PageDirectory &) = delete;
    // No moving
    PageDirectory(PageDirectory && rhs) = delete;
    PageDirectory & operator=(PageDirectory && rhs) = delete;

    friend class PageDirectoryFactory;

private:
    // Only `std::map` is allow for `MVCCMap`. Cause `std::map::insert` ensure that
    // "No iterators or references are invalidated"
    // https://en.cppreference.com/w/cpp/container/map/insert
    using MVCCMapType = std::map<PageId, VersionedPageEntriesPtr>;

    static void applyRefEditRecord(
        MVCCMapType & mvcc_table_directory,
        const VersionedPageEntriesPtr & version_list,
        const PageEntriesEdit::EditRecord & rec,
        const PageVersionType & version);

    static inline PageDirectorySnapshotPtr
    toConcreteSnapshot(const DB::PageStorageSnapshotPtr & ptr)
    {
        return std::static_pointer_cast<PageDirectorySnapshot>(ptr);
    }

private:
    std::atomic<UInt64> sequence;
    mutable std::shared_mutex table_rw_mutex;
    MVCCMapType mvcc_table_directory;

    mutable std::mutex snapshots_mutex;
    mutable std::list<std::weak_ptr<PageDirectorySnapshot>> snapshots;

    mutable std::mutex external_ids_mutex;
    mutable std::list<std::weak_ptr<PageId>> external_ids;

    WALStorePtr wal;

    LogWithPrefixPtr log;
};

} // namespace DB::PS::V3
