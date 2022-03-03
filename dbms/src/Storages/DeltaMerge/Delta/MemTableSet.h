#pragma once

#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSetSnapshot.h>
#include <Storages/DeltaMerge/Delta/ColumnFileFlushTask.h>

namespace DB
{
namespace DM
{
class MemTableSet;
using MemTableSetPtr = std::shared_ptr<MemTableSet>;

/// MemTableSet contains column file which data just resides in memory and it cannot be restored after restart.
/// And the column files will be flushed periodically to ColumnFilePersistedSet.
///
/// This class is not thread safe, manipulate on it requires acquire extra synchronization on the DeltaValueSpace
class MemTableSet : public std::enable_shared_from_this<MemTableSet>
    , private boost::noncopyable
{
private:
    /// To avoid serialize the same schema between continuous ColumnFileInMemory and ColumnFileTiny instance.
    BlockPtr last_schema;

    ColumnFiles column_files;

    std::atomic<size_t> rows = 0;
    std::atomic<size_t> bytes = 0;
    std::atomic<size_t> deletes = 0;

    Poco::Logger * log;

private:
    void appendColumnFileInner(const ColumnFilePtr & column_file);

public:
    explicit MemTableSet(const BlockPtr & last_schema_, const ColumnFiles & in_memory_files = {})
        : last_schema(last_schema_)
        , column_files(in_memory_files)
        , log(&Poco::Logger::get("MemTableSet"))
    {
        for (const auto & file : column_files)
        {
            rows += file->getRows();
            bytes += file->getBytes();
            deletes += file->getDeletes();
        }
    }

    String info() const
    {
        return fmt::format("MemTableSet: {} column files, {} rows, {} bytes, {} deletes",
                           column_files.size(),
                           rows.load(),
                           bytes.load(),
                           deletes.load());
    }

    ColumnFiles cloneColumnFiles(DMContext & context, const RowKeyRange & target_range, WriteBatches & wbs);

    size_t getColumnFileCount() const { return column_files.size(); }
    size_t getRows() const { return rows; }
    size_t getBytes() const { return bytes; }
    size_t getDeletes() const { return deletes; }

    /// The following methods returning false means this operation failed, caused by other threads could have done
    /// some updates on this instance. E.g. this instance have been abandoned.
    /// Caller should try again from the beginning.
    void appendColumnFile(const ColumnFilePtr & column_file);

    void appendToCache(DMContext & dm_context, const Block & block, size_t offset, size_t limit);

    void appendDeleteRange(const RowKeyRange & delete_range);

    void ingestColumnFiles(const RowKeyRange & range, const ColumnFiles & new_column_files, bool clear_data_in_range);

    /// Create a constant snapshot for read.
    ColumnFileSetSnapshotPtr createSnapshot();

    /// Build a flush task which will try to flush all column files in MemTableSet now
    ColumnFileFlushTaskPtr buildFlushTask(DMContext & context, size_t rows_offset, size_t deletes_offset, size_t flush_version);

    void removeColumnFilesInFlushTask(const ColumnFileFlushTask & flush_task);
};

} // namespace DM
} // namespace DB