#pragma once

#include <Common/LogWithPrefix.h>
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
    PageDirectory();

    void restore();

    PageDirectorySnapshotPtr createSnapshot() const;

    PageIDAndEntryV3 get(PageId page_id, const PageDirectorySnapshotPtr & snap) const;
    PageIDAndEntriesV3 get(const PageIds & page_ids, const PageDirectorySnapshotPtr & snap) const;

    void apply(PageEntriesEdit && edit);

    bool gc();

private:
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

        void createDelete(UInt64 seq)
        {
            entries.emplace(PageVersionType(seq), EntryOrDelete(/*del*/ true));
        }

        std::optional<PageEntryV3> getEntry(UInt64 seq) const;

    private:
        mutable std::mutex m;
        // Entries sorted by version
        std::map<PageVersionType, EntryOrDelete> entries;
    };
    using VersionedPageEntriesPtr = std::shared_ptr<VersionedPageEntries>;

private:
    std::atomic<UInt64> sequence;
    mutable std::shared_mutex table_rw_mutex;
    using MVCCMapType = std::unordered_map<PageId, VersionedPageEntriesPtr>;
    MVCCMapType mvcc_table_directory;

    mutable std::list<std::weak_ptr<PageDirectorySnapshot>> snapshots;

    WALStore wal;

    LogWithPrefixPtr log;
};

} // namespace DB::PS::V3
