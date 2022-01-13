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

#ifdef FIU_ENABLE
#include <Common/randomSeed.h>

#include <pcg_random.hpp>
#include <thread>
#endif

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

struct EntryOrDelete
{
    bool is_delete;
    PageEntryV3 entry;

    explicit EntryOrDelete(bool del)
        : is_delete(del)
    {
        assert(del == true);
    }
    explicit EntryOrDelete(const PageEntryV3 & entry_)
        : is_delete(false)
        , entry(entry_)
    {}
};
using PageLock = std::unique_ptr<std::lock_guard<std::mutex>>;
class VersionedPageEntries
{
public:
    PageLock acquireLock() const
    {
        return std::make_unique<std::lock_guard<std::mutex>>(m);
    }

    void createNewVersion(UInt64 seq, const PageEntryV3 & entry)
    {
        entries.emplace(PageVersionType(seq), entry);
    }

    void createNewVersion(UInt64 seq, UInt64 epoch, const PageEntryV3 & entry)
    {
        entries.emplace(PageVersionType(seq, epoch), entry);
    }

    void createDelete(UInt64 seq)
    {
        entries.emplace(PageVersionType(seq), EntryOrDelete(/*del*/ true));
    }

    std::optional<PageEntryV3> getEntry(UInt64 seq) const;

    /**
     * Take out the `VersionedEntries` which exist in the `BlobFileId`.
     * Also return the total size of entries.
     */
    std::pair<VersionedEntries, PageSize> getEntriesByBlobId(BlobFileId blob_id);

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
    std::pair<PageEntriesV3, bool> deleteAndGC(UInt64 lowest_seq);

    size_t size() const
    {
        auto lock = acquireLock();
        return entries.size();
    }

private:
    mutable std::mutex m;
    // Entries sorted by version
    std::map<PageVersionType, EntryOrDelete> entries;
};

class PageDirectory
{
public:
    using ConcreteSnapshotRawPtr = PageDirectorySnapshot *;

    PageDirectory();

    void restore();

    PageDirectorySnapshotPtr createSnapshot() const;

    std::tuple<size_t, double, unsigned> getSnapshotsStat() const;

    PageIDAndEntryV3 get(PageId page_id, const DB::PageStorageSnapshotPtr & snap) const;
    PageIDAndEntryV3 get(PageId page_id, const PageDirectorySnapshotPtr & snap) const;

    PageIDAndEntriesV3 get(const PageIds & page_ids, const DB::PageStorageSnapshotPtr & snap) const;
    PageIDAndEntriesV3 get(const PageIds & page_ids, const PageDirectorySnapshotPtr & snap) const;

    std::set<PageId> getAllPageIds();

    void apply(PageEntriesEdit && edit);

    std::vector<PageEntriesV3> gc();

    std::pair<std::map<BlobFileId, PageIdAndVersionedEntries>, PageSize> getEntriesByBlobIds(const std::vector<BlobFileId> & blob_need_gc);

    std::set<PageId> gcApply(const PageIdAndVersionedEntryList & migrated_entries, bool need_scan_page_ids);

    size_t numPages() const
    {
        std::shared_lock read_lock(table_rw_mutex);
        return mvcc_table_directory.size();
    }

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    std::atomic<UInt64> sequence;
    mutable std::shared_mutex table_rw_mutex;
    using MVCCMapType = std::unordered_map<PageId, VersionedPageEntriesPtr>;
    MVCCMapType mvcc_table_directory;

    mutable std::list<std::weak_ptr<PageDirectorySnapshot>> snapshots;

    WALStore wal;

    LogWithPrefixPtr log;
};

} // namespace DB::PS::V3
