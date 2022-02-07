#pragma once

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Core/Block.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileBig.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDeleteRange.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFilePersisted.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSetReader.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSetSnapshot.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/Delta/ColumnFileFlushTask.h>
#include <Storages/DeltaMerge/Delta/MinorCompaction.h>
#include <Storages/DeltaMerge/DeltaIndex.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/Page/PageDefines.h>
#include <fmt/format.h>


namespace DB
{
namespace DM
{
class ColumnFilePersistedSet;
using ColumnFilePersistedSetPtr = std::shared_ptr<ColumnFilePersistedSet>;

/// This class is not thread safe, manipulate on it requires acquire extra synchronization on the DeltaValueSpace
class ColumnFilePersistedSet : public std::enable_shared_from_this<ColumnFilePersistedSet>
    , private boost::noncopyable
{
public:
    using ColumnFilePersistedLevel = ColumnFilePersisteds;
    using ColumnFilePersistedLevels = std::vector<ColumnFilePersistedLevel>;

private:
    PageId metadata_id;
    ColumnFilePersistedLevels persisted_files_levels;
    std::atomic<size_t> persisted_files_count;

    std::atomic<size_t> rows = 0;
    std::atomic<size_t> bytes = 0;
    std::atomic<size_t> deletes = 0;

    /// below are just state resides in memory
    UInt64 flush_version = 0;
    size_t next_compaction_level = 0;
    UInt64 minor_compaction_version = 0;

    Poco::Logger * log;

private:
    inline void updateColumnFileStats();

    void checkColumnFiles(const ColumnFilePersistedLevels & new_column_file_levels);

public:
    explicit ColumnFilePersistedSet(PageId metadata_id_, const ColumnFilePersisteds & persisted_column_files = {});

    /// Restore the metadata of this instance.
    /// Only called after reboot.
    static ColumnFilePersistedSetPtr restore(DMContext & context, const RowKeyRange & segment_range, PageId id);

    String simpleInfo() const { return "ColumnFilePersistedSet [" + DB::toString(metadata_id) + "]"; }
    String info() const
    {
        String levels_summary;
        for (size_t i = 0; i < persisted_files_levels.size(); i++)
        {
            levels_summary += fmt::format("[{}: {}]", i, persisted_files_levels[i].size());
            if (i != persisted_files_levels.size() - 1)
                levels_summary += ",";
        }

        return fmt::format("ColumnFilePersistedSet [{}][levels summary: {}]: {} column files, {} rows, {} bytes, {} deletes.",
                           metadata_id,
                           levels_summary,
                           persisted_files_count.load(),
                           rows.load(),
                           bytes.load(),
                           deletes.load());
    }
    String levelsInfo() const
    {
        String levels_info;
        for (size_t i = 0; i < persisted_files_levels.size(); i++)
            levels_info += fmt::format("[{}]: {}", i, columnFilesToString(persisted_files_levels[i]));
        return levels_info;
    }

    void saveMeta(WriteBatches & wbs) const;

    void recordRemoveColumnFilesPages(WriteBatches & wbs) const;

    BlockPtr getLastSchema();

    ColumnFilePersisteds
    checkHeadAndCloneTail(DMContext & context, const RowKeyRange & target_range, const ColumnFiles & head_column_files, WriteBatches & wbs) const;

    PageId getId() const { return metadata_id; }

    size_t getColumnFileCount() const { return persisted_files_count.load(); }
    size_t getRows() const { return rows.load(); }
    size_t getBytes() const { return bytes.load(); }
    size_t getDeletes() const { return deletes.load(); }

    size_t getTotalCacheRows() const;
    size_t getTotalCacheBytes() const;
    size_t getValidCacheRows() const;

    size_t getCurrentFlushVersion() const { return flush_version; }

    /// Check whether the task_flush_version is valid,
    /// and if it is valid then increase the internal flush version.
    bool checkAndIncreaseFlushVersion(size_t task_flush_version);

    bool appendPersistedColumnFilesToLevel0(const ColumnFilePersisteds & column_files, WriteBatches & wbs);

    /// Choose a level in which exists some small column files that can be compacted to a larger column file
    MinorCompactionPtr pickUpMinorCompaction(DMContext & context);

    /// Update the metadata to commit the compaction results
    bool installCompactionResults(const MinorCompactionPtr & compaction, WriteBatches & wbs);

    ColumnFileSetSnapshotPtr createSnapshot(const DMContext & context);
};

} // namespace DM
} // namespace DB
