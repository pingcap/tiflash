#pragma once

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Core/Block.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnBigFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnDeleteRangeFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSetSnapshot.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSetReader.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnStableFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnTinyFile.h>
#include <Storages/DeltaMerge/Delta/MinorCompaction.h>
#include <Storages/DeltaMerge/DeltaIndex.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaTree.h>
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

    ColumnStableFiles
    checkHeadAndCloneTail(DMContext & context, const RowKeyRange & target_range, const ColumnFiles & head_column_files, WriteBatches & wbs) const;


    void recordRemoveColumnFilesPages(WriteBatches & wbs) const;

    PageId getId() const { return metadata_id; }

    size_t getColumnFileCount() const
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

    bool installCompactionResults(const MinorCompactionPtr & compaction, WriteBatches & wbs);

    ColumnFileSetSnapshotPtr createSnapshot(const DMContext & context);
};

}
}
