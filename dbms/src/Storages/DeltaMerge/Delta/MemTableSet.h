// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
/// This class is mostly not thread safe, manipulate on it requires acquire extra synchronization on the DeltaValueSpace
/// Only the method that just access atomic variable can be called without extra synchronization
class MemTableSet : public std::enable_shared_from_this<MemTableSet>
    , private boost::noncopyable
{
private:
    /// To avoid serialize the same schema between continuous ColumnFileInMemory and ColumnFileTiny instance.
    BlockPtr last_schema;

    // Note that we must update `column_files_count` for outer thread-safe after `column_files` changed
    ColumnFiles column_files;
    // TODO: check the proper memory_order when use this atomic variable
    std::atomic<size_t> column_files_count;

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
        column_files_count = column_files.size();
        for (const auto & file : column_files)
        {
            rows += file->getRows();
            bytes += file->getBytes();
            deletes += file->getDeletes();
            if (auto * m_file = file->tryToInMemoryFile(); m_file)
            {
                last_schema = m_file->getSchema();
            }
            else if (auto * t_file = file->tryToTinyFile(); t_file)
            {
                last_schema = t_file->getSchema();
            }
        }
    }

    /// Thread safe part start
    String info() const
    {
        return fmt::format("MemTableSet: {} column files, {} rows, {} bytes, {} deletes",
                           column_files_count.load(),
                           rows.load(),
                           bytes.load(),
                           deletes.load());
    }

    size_t getColumnFileCount() const { return column_files_count.load(); }
    size_t getRows() const { return rows.load(); }
    size_t getBytes() const { return bytes.load(); }
    size_t getDeletes() const { return deletes.load(); }
    /// Thread safe part end

    ColumnFiles cloneColumnFiles(DMContext & context, const RowKeyRange & target_range, WriteBatches & wbs);

    void recordRemoveColumnFilesPages(WriteBatches & wbs) const;

    /// The following methods returning false means this operation failed, caused by other threads could have done
    /// some updates on this instance. E.g. this instance have been abandoned.
    /// Caller should try again from the beginning.

    /// Append a ColumnFile into this MemTableSet. The ColumnFile may be flushed later.
    /// Note that some ColumnFiles may not contain block data, but only a reference to the block data stored in disk.
    /// See different ColumnFile implementations for details.
    void appendColumnFile(const ColumnFilePtr & column_file);

    /// Append the block data into a ColumnFileInMemory (may be reused).
    /// The ColumnFileInMemory will be stored in this MemTableSet and flushed later.
    void appendToCache(DMContext & dm_context, const Block & block, size_t offset, size_t limit);

    void appendDeleteRange(const RowKeyRange & delete_range);

    void ingestColumnFiles(const RowKeyRange & range, const ColumnFiles & new_column_files, bool clear_data_in_range);

    /// Create a constant snapshot for read.
    ColumnFileSetSnapshotPtr createSnapshot(const StorageSnapshotPtr & storage_snap);

    /// Build a flush task which will try to flush all column files in this MemTableSet at this moment.
    ColumnFileFlushTaskPtr buildFlushTask(DMContext & context, size_t rows_offset, size_t deletes_offset, size_t flush_version);

    void removeColumnFilesInFlushTask(const ColumnFileFlushTask & flush_task);
};

} // namespace DM
} // namespace DB