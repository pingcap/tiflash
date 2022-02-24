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
    enum class Type
    {
        NORMAL,
        REF,
        EXTERNAL,
        DELETE,
    };
    Type type;
    PageEntryV3 entry;
    PageId origin_page_id;
    Int64 being_ref_count = 1;

    static EntryOrDelete newDelete()
    {
        return EntryOrDelete{
            .type = Type::DELETE,
            .entry = {}, // meaningless
            .origin_page_id = 0, // meaningless
            .being_ref_count = 1, // meaningless
        };
    }
    static EntryOrDelete newNormalEntry(const PageEntryV3 & entry)
    {
        return EntryOrDelete{
            .type = Type::NORMAL,
            .entry = entry,
            .origin_page_id = 0, // meaningless
            .being_ref_count = 1,
        };
    }
    static EntryOrDelete newRepalcingEntry(const EntryOrDelete & ori_entry, const PageEntryV3 & entry)
    {
        return EntryOrDelete{
            .type = Type::NORMAL,
            .entry = entry,
            .origin_page_id = 0, // meaningless
            .being_ref_count = ori_entry.being_ref_count,
        };
    }
    static EntryOrDelete newRefEntry(PageId ori_id)
    {
        return EntryOrDelete{
            .type = Type::REF,
            .entry = {}, // meaningless
            .origin_page_id = ori_id,
            .being_ref_count = 1, // meaningless
        };
    }
    static EntryOrDelete newExternal()
    {
        return EntryOrDelete{
            .type = Type::EXTERNAL,
            .entry = {}, // meaningless
            .origin_page_id = 0, // meaningless
            .being_ref_count = 1, // meaningless
        };
    }

    bool isDelete() const { return type == Type::DELETE; }
    bool isExternal() const { return type == Type::EXTERNAL; }
    bool isRef() const { return type == Type::REF; }
    bool isNormal() const { return type == Type::NORMAL; }

    String toDebugString() const
    {
        return fmt::format(
            "{{type:{}, entry:{}, ori_page_id:{}, being_ref_count:{}}}",
            static_cast<Int32>(type),
            ::DB::PS::V3::toDebugString(entry),
            origin_page_id,
            being_ref_count);
    }
};
using PageLock = std::unique_ptr<std::lock_guard<std::mutex>>;
class VersionedPageEntries
{
public:
    [[nodiscard]] PageLock acquireLock() const
    {
        return std::make_unique<std::lock_guard<std::mutex>>(m);
    }

    inline void createNewVersion(UInt64 seq, const PageEntryV3 & entry)
    {
        createNewVersion(seq, 0, entry);
    }

    void createNewVersion(UInt64 seq, UInt64 epoch, const PageEntryV3 & entry);

    void createDelete(UInt64 seq)
    {
        auto page_lock = acquireLock();
        if (entries.empty() || !entries.rbegin()->second.isDelete())
        {
            entries.emplace(PageVersionType(seq), EntryOrDelete::newDelete());
        }
    }

    bool createNewRefVersion(UInt64 seq, PageId ori_page_id);

    enum ResolveResult
    {
        RESOLVE_FAIL,
        RESOLVE_TO_REF,
        RESOLVE_TO_NORMAL,
    };
    std::tuple<ResolveResult, PageId, PageVersionType>
    resolveToPageId(UInt64 seq);

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
    std::pair<PageEntriesV3, bool> cleanOutdatedEntries(
        UInt64 lowest_seq,
        PageId page_id,
        std::map<PageId, std::pair<PageVersionType, Int64>> * normal_entries_to_deref,
        const PageLock & page_lock);
    std::pair<PageEntriesV3, bool> derefAndClean(UInt64 lowest_seq, PageId page_id, const PageVersionType & deref_ver, Int64 deref_count);

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

    using CollapsingMapType = std::unordered_map<PageId, std::pair<PageVersionType, PageEntryV3>>;
    CollapsingMapType table_directory;

    PageId max_applied_page_id = 0;
    PageVersionType max_applied_ver;

    // No copying
    CollapsingPageDirectory(const CollapsingPageDirectory &) = delete;
    CollapsingPageDirectory & operator=(const CollapsingPageDirectory &) = delete;
};

// `PageDirectory` store multi-versions entries for the same
// page id. User can acquire a snapshot from it and get a
// consist result by the snapshot.
// All its functions are consider concurrent safe.
// User should call `gc` periodic to remove outdated version
// of entries in order to keep the memory consumption as well
// as the restoring time in a reasonable level.
class PageDirectory
{
public:
    PageDirectory();
    PageDirectory(UInt64 init_seq, WALStorePtr && wal);

    static PageDirectory create(const CollapsingPageDirectory & collapsing_directory, WALStorePtr && wal);

    PageDirectorySnapshotPtr createSnapshot() const;

    std::tuple<size_t, double, unsigned> getSnapshotsStat() const;

    PageIDAndEntryV3 get(PageId page_id, const DB::PageStorageSnapshotPtr & snap) const;
    PageIDAndEntryV3 get(PageId page_id, const PageDirectorySnapshotPtr & snap) const;

    PageIDAndEntriesV3 get(const PageIds & page_ids, const DB::PageStorageSnapshotPtr & snap) const;
    PageIDAndEntriesV3 get(const PageIds & page_ids, const PageDirectorySnapshotPtr & snap) const;

    PageId getMaxId() const;

    std::set<PageId> getAllPageIds();

    void apply(PageEntriesEdit && edit, const WriteLimiterPtr & write_limiter = nullptr);

    std::pair<std::map<BlobFileId, PageIdAndVersionedEntries>, PageSize>
    getEntriesByBlobIds(const std::vector<BlobFileId> & blob_ids) const;

    std::set<PageId> gcApply(PageEntriesEdit && migrated_edit, bool need_scan_page_ids, const WriteLimiterPtr & write_limiter = nullptr);

    std::vector<PageEntriesV3> gc(const WriteLimiterPtr & write_limiter = nullptr, const ReadLimiterPtr & read_limiter = nullptr);

    size_t numPages() const
    {
        std::shared_lock read_lock(table_rw_mutex);
        return mvcc_table_directory.size();
    }

    // No copying
    PageDirectory(const PageDirectory &) = delete;
    PageDirectory & operator=(const PageDirectory &) = delete;
    // Only moving
    PageDirectory(PageDirectory && rhs) noexcept
    {
        *this = std::move(rhs);
    }
    PageDirectory & operator=(PageDirectory && rhs) noexcept
    {
        if (this != &rhs)
        {
            // Note: Not making it thread safe for moving, don't
            // care about `table_rw_mutex` and `snapshots_mutex`
            sequence.store(rhs.sequence.load());
            mvcc_table_directory = std::move(rhs.mvcc_table_directory);
            snapshots = std::move(rhs.snapshots);
            wal = std::move(rhs.wal);
            log = std::move(rhs.log);
        }
        return *this;
    }

private:
    std::atomic<UInt64> sequence;
    mutable std::shared_mutex table_rw_mutex;
    // Only `std::map` is allow for `MVCCMap`. Cause `std::map::insert` ensure that
    // "No iterators or references are invalidated"
    // https://en.cppreference.com/w/cpp/container/map/insert
    using MVCCMapType = std::map<PageId, VersionedPageEntriesPtr>;
    MVCCMapType mvcc_table_directory;

    mutable std::mutex snapshots_mutex;
    mutable std::list<std::weak_ptr<PageDirectorySnapshot>> snapshots;

    WALStorePtr wal;

    LogWithPrefixPtr log;
};

} // namespace DB::PS::V3
