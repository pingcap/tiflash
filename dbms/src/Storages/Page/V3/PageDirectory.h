#pragma once

#include <Storages/Page/Page.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/WALStore.h>

#include <memory>
#include <shared_mutex>
#include <unordered_map>

namespace DB::PS::V3
{
class PageDirectorySnapshot : public DB::PageStorageSnapshot
{
public:
};
using PageDirectorySnapshotPtr = std::shared_ptr<PageDirectorySnapshot>;

class PageDirectory
{
public:
    PageDirectorySnapshotPtr createSnapshot() const;

    PageIDAndEntriesV3 get(const PageId & read_id, const PageDirectorySnapshotPtr & snap) const;
    PageIDAndEntriesV3 get(const PageIds & read_ids, const PageDirectorySnapshotPtr & snap) const;

    void apply(PageEntriesEdit && edit);

    bool gc();

private:
    struct VersionType
    {
        UInt64 sequence = 0;
        UInt64 epoch = 0;
    };
    struct VersionedPageEntry
    {
        VersionType ver;
        PageEntryV3 entry;
    };
    class VersionedPageEntries
    {
        std::list<VersionedPageEntry> entries;
        mutable std::mutex m;
    };

    std::shared_mutex table_rw_mutex;
    std::unordered_map<PageId, VersionedPageEntries> mvcc_table_directory;

    std::list<std::weak_ptr<PageDirectorySnapshot>> snapshots;

    WALStore wal;
};

} // namespace DB::PS::V3
