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
class MemTableSet
    : public std::enable_shared_from_this<MemTableSet>
    , private boost::noncopyable
{
#ifndef DBMS_PUBLIC_GTEST
private:
#else
public:
#endif
    // Note that we must update `column_files_count` for outer thread-safe after `column_files` changed
    ColumnFiles column_files;
    // TODO: check the proper memory_order when use this atomic variable
    std::atomic<size_t> column_files_count;

    std::atomic<size_t> rows = 0;
    std::atomic<size_t> bytes = 0;
    std::atomic<size_t> deletes = 0;

    LoggerPtr log;

private:
    void appendColumnFileInner(const ColumnFilePtr & column_file);

public:
    explicit MemTableSet(const ColumnFiles & in_memory_files = {})
        : column_files(in_memory_files)
        , log(Logger::get())
    {
        column_files_count = column_files.size();
        for (const auto & file : column_files)
        {
            rows += file->getRows();
            bytes += file->getBytes();
            deletes += file->getDeletes();
        }
    }

    /**
     * Resets the logger by using the one from the segment.
     * Segment_log is not available when constructing, because usually
     * at that time the segment has not been constructed yet.
     */
    void resetLogger(const LoggerPtr & segment_log) { log = segment_log; }

    /// Thread safe part start
    String info() const
    {
        return fmt::format(
            "MemTableSet: {} column files, {} rows, {} bytes, {} deletes",
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

    /**
     * For memtable, there are two cases after the snapshot is created:
     * 1. CFs in memtable may be flushed into persist, causing CFs in snapshot no longer exist in memtable.
     * 2. There are new writes, producing new CFs in memtable.
     *
     * This function will compare the CFs in memtable with the CFs in memtable-snapshot.
     *
     * @returns two pairs: < NewColumnFiles, RemovedColumnFiles >
     *   NewColumnFiles     -- These CFs in memtable are newly appended compared to the snapshot.
     *   FlushedColumnFiles -- These CFs in snapshot were flushed, no longer exist in memtable.
     *
     * Note: there may be CFs newly appended and then removed (e.g. write + flush). These CFs will
     * not be included in the result.
     */
    std::pair<ColumnFiles, ColumnFiles> diffColumnFiles(const ColumnFiles & column_files_in_snapshot) const;

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

    /**
     * Create a snapshot for reading data from it.
     *
     * **WARNING**:
     *
     * When you specify `disable_sharing == false` (which is the default value), the content of this snapshot may be
     * still mutable. For example, there may be concurrent writes when you hold this snapshot, causing the
     * snapshot to be changed. However it is guaranteed that only new data will be appended. No data will be lost when you
     * are holding this snapshot.
     *
     * `disable_sharing == true` seems nice, but it may cause flush to be less efficient when used frequently.
     * Only specify it when really needed.
     */
    ColumnFileSetSnapshotPtr createSnapshot(
        const IColumnFileDataProviderPtr & data_provider,
        bool disable_sharing = false);

    /// Build a flush task which will try to flush all column files in this MemTableSet at this moment.
    ColumnFileFlushTaskPtr buildFlushTask(
        DMContext & context,
        size_t rows_offset,
        size_t deletes_offset,
        size_t flush_version);

    void removeColumnFilesInFlushTask(const ColumnFileFlushTask & flush_task);
};

} // namespace DM
} // namespace DB
