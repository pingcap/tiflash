#pragma once

#include <Storages/Page/Page.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/V3/MapUtils.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/WALStore.h>
#include <common/types.h>

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

namespace DB::PS::V3
{
class PageDirectorySnapshot : public DB::PageStorageSnapshot
{
public:
    UInt64 sequence;
    explicit PageDirectorySnapshot(UInt64 seq)
        : sequence(seq)
    {}
};
using PageDirectorySnapshotPtr = std::shared_ptr<PageDirectorySnapshot>;

class PageDirectory
{
public:
    PageDirectorySnapshotPtr createSnapshot() const;

    PageIDAndEntryV3 get(PageId page_id, const PageDirectorySnapshotPtr & snap) const;
    PageIDAndEntriesV3 get(const PageIds & page_ids, const PageDirectorySnapshotPtr & snap) const;

    void apply(PageEntriesEdit && edit);

    bool gc();

private:
    // `apply` with create a version={directory.sequence, epoch=0}.
    // After data compaction and page entries need to be updated, will create
    // some entries with a version={old_sequence, epoch=old_epoch+1}.
    struct VersionType
    {
        UInt64 sequence; // The write sequence
        UInt64 epoch; // The GC epoch

        explicit VersionType(UInt64 seq)
            : sequence(seq)
            , epoch(0)
        {}

        bool operator<(const VersionType & rhs) const
        {
            if (sequence == rhs.sequence)
                return epoch < rhs.epoch;
            return sequence < rhs.sequence;
        }
    };

    using PageLock = std::unique_ptr<std::lock_guard<std::mutex>>;
    class VersionedPageEntries
    {
    public:
        PageLock
        acquireLock() const
        {
            return std::make_unique<std::lock_guard<std::mutex>>(m);
        }

        void createNewVersion(UInt64 seq, const PageEntryV3 & entry)
        {
            entries.emplace(VersionType(seq), entry);
        }

        std::optional<PageEntryV3> getEntry(UInt64 seq);

    private:
        mutable std::mutex m;
        // Entries sorted by version
        std::map<VersionType, PageEntryV3> entries;
    };
    using VersionedPageEntriesPtr = std::shared_ptr<VersionedPageEntries>;

    mutable std::shared_mutex table_rw_mutex;
    std::atomic<UInt64> sequence;
    using MVCCMapType = std::unordered_map<PageId, VersionedPageEntriesPtr>;
    MVCCMapType mvcc_table_directory;

    std::list<std::weak_ptr<PageDirectorySnapshot>> snapshots;

    WALStore wal;
};

} // namespace DB::PS::V3
