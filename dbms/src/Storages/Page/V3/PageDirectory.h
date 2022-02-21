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

struct EntryOrDelete
{
public:
    explicit EntryOrDelete(bool del)
        : entry_or_delete(nullptr)
    {
        assert(del == true);
    }
    explicit EntryOrDelete(std::shared_ptr<PageEntryV3> && entry)
        : entry_or_delete(std::move(entry))
    {
        assert(entry_or_delete != nullptr);
    }

    inline bool isDelete() const { return entry_or_delete == nullptr; }

    inline const std::shared_ptr<PageEntryV3> & entryPtr() const
    {
        assert(entry_or_delete != nullptr);
        return entry_or_delete;
    }

private:
    std::shared_ptr<PageEntryV3> entry_or_delete;
};

using PageLock = std::unique_ptr<std::lock_guard<std::mutex>>;
class VersionedPageEntries
{
public:
    [[nodiscard]] PageLock acquireLock() const
    {
        return std::make_unique<std::lock_guard<std::mutex>>(m);
    }

    void createNewVersion(UInt64 seq, std::shared_ptr<PageEntryV3> && entry)
    {
        entries.emplace(PageVersionType(seq), EntryOrDelete(std::move(entry)));
    }

    void createNewVersion(UInt64 seq, UInt64 epoch, std::shared_ptr<PageEntryV3> && entry)
    {
        entries.emplace(PageVersionType(seq, epoch), EntryOrDelete(std::move(entry)));
    }

    void createDelete(UInt64 seq)
    {
        entries.emplace(PageVersionType(seq), EntryOrDelete(/*del*/ true));
    }

    // Return the shared_ptr to the entry that is visible by `seq`.
    // If the entry is not visible (deleted or not exist for `seq`), then
    // return `nullptr`.
    std::shared_ptr<PageEntryV3> getEntry(UInt64 seq) const;

    std::shared_ptr<PageEntryV3> getEntryNotSafe(UInt64 seq) const;

    std::shared_ptr<PageEntryV3> getLatestEntryNotSafe() const;

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
    bool deleteAndGC(UInt64 lowest_seq);

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

// `CollapsingPageDirectory` only store the latest version
// of entry for the same page id. It is a util class for
// restoring from persisted logs and compacting logs.
// There is no concurrent security guarantee for this class.
class CollapsingPageDirectory
{
public:
    CollapsingPageDirectory();

    void apply(PageEntriesEdit && edit);

    void dumpTo(std::unique_ptr<LogWriter> & log_writer);

    struct CollapsedVersionEntry
    {
        PageVersionType ver;
        PageEntryV3 entry;
        Int64 ref_count;

        CollapsedVersionEntry()
            : ref_count(1)
        {}
        CollapsedVersionEntry(PageVersionType ver_, PageEntryV3 entry_)
            : ver(ver_)
            , entry(entry_)
            , ref_count(1)
        {}
    };
    std::unordered_map<PageId, std::pair<PageVersionType, PageId>> id_mapping;
    using CollapsingMapType = std::unordered_map<PageId, CollapsedVersionEntry>;
    CollapsingMapType entries_directory;

    PageId max_applied_page_id = 0;
    PageVersionType max_applied_ver;

    // No copying
    CollapsingPageDirectory(const CollapsingPageDirectory &) = delete;
    CollapsingPageDirectory & operator=(const CollapsingPageDirectory &) = delete;

private:
    PageId getOriId(PageId page_id) const;
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
    PageDirectory();
    PageDirectory(UInt64 init_seq, WALStorePtr && wal);

    static PageDirectoryPtr create(const CollapsingPageDirectory & collapsing_directory, WALStorePtr && wal);

    PageDirectorySnapshotPtr createSnapshot() const;

    std::tuple<size_t, double, unsigned> getSnapshotsStat() const;

    PageIDAndEntryV3 get(PageId page_id, const DB::PageStorageSnapshotPtr & snap) const;
    PageIDAndEntryV3 get(PageId page_id, const PageDirectorySnapshotPtr & snap) const;

    PageIDAndEntriesV3 get(const PageIds & page_ids, const DB::PageStorageSnapshotPtr & snap) const;
    PageIDAndEntriesV3 get(const PageIds & page_ids, const PageDirectorySnapshotPtr & snap) const;

    PageId getMaxId() const;

    std::set<PageId> getAllPageIds();

    void apply(PageEntriesEdit && edit);

    std::pair<std::map<BlobFileId, PageIdAndVersionedEntries>, PageSize>
    getEntriesByBlobIds(const std::vector<BlobFileId> & blob_need_gc);

    std::set<PageId> gcApply(PageEntriesEdit && migrated_edit, bool need_scan_page_ids);

    PageEntriesV3 gc();

    size_t numPages() const
    {
        std::shared_lock read_lock(table_rw_mutex);
        return mvcc_table_directory.size();
    }

    // No copying
    PageDirectory(const PageDirectory &) = delete;
    PageDirectory & operator=(const PageDirectory &) = delete;
    // No moving
    PageDirectory(PageDirectory && rhs) noexcept = delete;
    PageDirectory & operator=(PageDirectory && rhs) noexcept = delete;

private:
    inline std::shared_ptr<PageEntryV3> createRecyclableEntry(const PageEntryV3 & entry)
    {
        // If there exist a shared ptr to the entry inside `mvcc_table_directory`, applying `REF`
        // only increase the ref count of the shared ptr.
        // After this shared ptr to the entry is totally removed from `mvcc_table_directory`, it
        // will be emplaced to `pending_remove_entries`.
        return std::shared_ptr<PageEntryV3>(
            new PageEntryV3(entry),
            [this](PageEntryV3 * ptr) {
                if (ptr)
                {
                    // Copy the entry to recycle bin
                    this->pending_remove_entries.emplace_back(*ptr);
                    delete ptr;
                }
            });
    }

private:
    std::atomic<UInt64> sequence;

    // The lifetime of `pending_remove_entries` must be longer than `mvcc_table_directory`
    PageEntriesV3 pending_remove_entries;

    mutable std::shared_mutex table_rw_mutex;
    using MVCCMapType = std::unordered_map<PageId, VersionedPageEntriesPtr>;
    MVCCMapType mvcc_table_directory;

    mutable std::mutex snapshots_mutex;
    mutable std::list<std::weak_ptr<PageDirectorySnapshot>> snapshots;

    WALStorePtr wal;

    LogWithPrefixPtr log;
};

} // namespace DB::PS::V3
