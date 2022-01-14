#pragma once

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Core/Block.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/ColumnBigFile.h>
#include <Storages/DeltaMerge/ColumnDeleteRangeFile.h>
#include <Storages/DeltaMerge/ColumnFile.h>
#include <Storages/DeltaMerge/ColumnFileSetSnapshot.h>
#include <Storages/DeltaMerge/ColumnStableFile.h>
#include <Storages/DeltaMerge/ColumnTinyFile.h>
#include <Storages/DeltaMerge/DeltaIndex.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/MinorCompaction.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/Page/PageDefines.h>
#include "FlushColumnFileTask.h"


namespace DB
{
namespace DM
{
class ColumnStableFileSet;
using ColumnStableFileSetPtr = std::shared_ptr<ColumnStableFileSet>;
class ColumnStableFileSetSnapshot;
using ColumnStableFileSetSnapshotPtr = std::shared_ptr<ColumnStableFileSetSnapshot>;
class ColumnStableFileSetReader;
using ColumnStableFileSetReaderPtr = std::shared_ptr<ColumnStableFileSetReader>;

class ColumnStableFileSet : public std::enable_shared_from_this<ColumnStableFileSet>
    , private boost::noncopyable
{
public:
    using ColumnStableFileLevel = ColumnStableFiles;
    using ColumnStableFileLevels = std::vector<ColumnStableFileLevel>;

private:
    PageId metadata_id;
    ColumnStableFileLevels column_stable_file_levels;

    std::atomic<size_t> rows = 0;
    std::atomic<size_t> bytes = 0;
    std::atomic<size_t> deletes = 0;

    /// We need to run compact.
    std::atomic_bool should_compact = false;

    UInt64 flush_version = 0;

    size_t next_compaction_level = 0;
    UInt64 minor_compaction_version = 0;

    Poco::Logger * log;

public:
    ColumnStableFileSet(PageId metadata_id_, const ColumnStableFiles & column_stable_files = {});

    /// Restore the metadata of this instance.
    /// Only called after reboot.
    static ColumnStableFileSetPtr restore(DMContext & context, const RowKeyRange & segment_range, PageId id);

    String simpleInfo() const { return "ColumnStableFileSet [" + DB::toString(metadata_id) + "]"; }
    String info() const
    {
        // TODO: levels
        return "{ColumnStableFileSet [" + DB::toString(metadata_id) + "]: ";
    }

    void saveMeta(WriteBatches & wbs) const;

    void recordRemoveColumnFilesPages(WriteBatches & wbs) const;

    PageId getId() const { return metadata_id; }

    size_t getPackCount() const
    {
        size_t count = 0;
        for (const auto & level : column_stable_file_levels)
        {
            count += level.size();
        }
        return count;
    }
    size_t getRows() const { return rows; }
    size_t getBytes() const { return bytes; }
    size_t getDeletes() const { return deletes; }

    size_t getTotalCacheRows() const;
    size_t getTotalCacheBytes() const;
    size_t getValidCacheRows() const;

    bool isShouldCompact() const { return should_compact; }

    UInt64 getCurrentFlushVersion() { return flush_version; }

    bool appendColumnStableFilesToLevel0(size_t prev_flush_version, const ColumnStableFiles & column_files, WriteBatches & wbs);

    MinorCompactionPtr pickUpMinorCompaction(DMContext & context);

    bool installCompactionResults(const MinorCompactionPtr & compaction);

    ColumnStableFileSetSnapshotPtr createSnapshot(const DMContext & context);
};


class ColumnStableFileSetSnapshot : public std::enable_shared_from_this<ColumnStableFileSetSnapshot>
    , private boost::noncopyable
{
    friend class ColumnStableFileSet;

private:
    StorageSnapshotPtr storage_snap;

    ColumnFileSetSnapshotPtr column_file_set_snapshot;

public:

    ColumnStableFileSetSnapshotPtr clone()
    {
        auto c = std::make_shared<ColumnStableFileSetSnapshot>();
        c->storage_snap = storage_snap;
        c->column_file_set_snapshot = column_file_set_snapshot->clone();
        return c;
    }

    size_t getColumnFilesCount() const { return column_file_set_snapshot->getColumnFilesCount(); }
    size_t getRows() const { return column_file_set_snapshot->getRows(); }
    size_t getBytes() const { return column_file_set_snapshot->getBytes(); }
    size_t getDeletes() const { return column_file_set_snapshot->getDeletes(); }

    RowKeyRange getSquashDeleteRange() { return column_file_set_snapshot->getSquashDeleteRange(); }

    const auto & getStorageSnapshot() { return storage_snap; }
};

class ColumnStableFileSetReader
{

};

}
}
