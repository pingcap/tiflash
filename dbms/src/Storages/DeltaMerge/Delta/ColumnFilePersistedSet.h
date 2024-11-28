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
#include <Storages/Page/PageDefinesBase.h>
#include <fmt/format.h>

namespace DB
{
namespace DM
{
class ColumnFilePersistedSet;
using ColumnFilePersistedSetPtr = std::shared_ptr<ColumnFilePersistedSet>;

/// This class is mostly not thread safe, manipulate on it requires acquire extra synchronization on the DeltaValueSpace
/// Only the method that just access atomic variable can be called without extra synchronization
class ColumnFilePersistedSet
    : public std::enable_shared_from_this<ColumnFilePersistedSet>
    , private boost::noncopyable
{
private:
    PageIdU64 metadata_id;
    ColumnFilePersisteds persisted_files;
    // TODO: check the proper memory_order when use this atomic variable
    std::atomic<size_t> persisted_files_count = 0;

    std::atomic<size_t> rows = 0;
    std::atomic<size_t> bytes = 0;
    std::atomic<size_t> deletes = 0;

    /// below are just state resides in memory
    UInt64 flush_version = 0;
    UInt64 minor_compaction_version = 0;

    LoggerPtr log;

private:
    inline void updateColumnFileStats();

    void checkColumnFiles(const ColumnFilePersisteds & new_column_files);

public:
    explicit ColumnFilePersistedSet(PageIdU64 metadata_id_, const ColumnFilePersisteds & persisted_column_files = {});

    /// Restore the metadata of this instance.
    /// Only called after reboot.
    static ColumnFilePersistedSetPtr restore(DMContext & context, const RowKeyRange & segment_range, PageIdU64 id);
    /// Restore from a checkpoint from other peer.
    /// Only used in FAP.
    static ColumnFilePersistedSetPtr restore( //
        DMContext & context,
        const RowKeyRange & segment_range,
        ReadBuffer & buf,
        PageIdU64 id);

    static ColumnFilePersistedSetPtr createFromCheckpoint( //
        const LoggerPtr & parent_log,
        DMContext & context,
        UniversalPageStoragePtr temp_ps,
        const RowKeyRange & segment_range,
        PageIdU64 delta_id,
        WriteBatches & wbs);

    /**
     * Resets the logger by using the one from the segment.
     * Segment_log is not available when constructing, because usually
     * at that time the segment has not been constructed yet.
     */
    void resetLogger(const LoggerPtr & segment_log) { log = segment_log; }

    /// Thread safe part start
    String simpleInfo() const { return "ColumnFilePersistedSet [" + DB::toString(metadata_id) + "]"; }
    String info() const
    {
        return fmt::format(
            "ColumnFilePersistedSet [{}]: {} column files, {} rows, {} bytes, {} deletes.",
            metadata_id,
            persisted_files_count.load(),
            rows.load(),
            bytes.load(),
            deletes.load());
    }
    /// Thread safe part end
    String detailInfo() const { return ColumnFile::filesToString(persisted_files); }

    const ColumnFilePersisteds & getFiles() const { return persisted_files; }

    void saveMeta(WriteBuffer & buf) const;
    void saveMeta(WriteBatches & wbs) const;

    void recordRemoveColumnFilesPages(WriteBatches & wbs) const;

    /**
     * Return newly appended column files compared to `previous_column_files`.
     * If `previous_column_files` is not the prefix of the current column files, exceptions will be thrown.
     *
     * Example:
     *  A, B, C, D          Current Column File
     *  A, B                Previous Column File
     *        C, D          Return Value
     *
     * Example of throws exception:
     *  A, B, C, D          Current Column File
     *     B                Previous Column File
     */
    ColumnFilePersisteds diffColumnFiles(const ColumnFiles & previous_column_files) const;

    /// Thread safe part start
    PageIdU64 getId() const { return metadata_id; }

    size_t getColumnFileCount() const { return persisted_files_count.load(); }
    size_t getRows() const { return rows.load(); }
    size_t getBytes() const { return bytes.load(); }
    size_t getDeletes() const { return deletes.load(); }
    /// Thread safe part end

    size_t getCurrentFlushVersion() const { return flush_version; }

    /// Check whether the task_flush_version is valid,
    /// and if it is valid then increase the internal flush version.
    bool checkAndIncreaseFlushVersion(size_t task_flush_version);

    bool appendPersistedColumnFiles(const ColumnFilePersisteds & column_files, WriteBatches & wbs);

    bool updatePersistedColumnFilesAfterAddingIndex(
        const ColumnFilePersisteds & new_persisted_files,
        WriteBatches & wbs);

    /// Choose all small column files that can be compacted to larger column files
    MinorCompactionPtr pickUpMinorCompaction(DMContext & context);

    /// Update the metadata to commit the compaction results
    bool installCompactionResults(const MinorCompactionPtr & compaction, WriteBatches & wbs);

    ColumnFileSetSnapshotPtr createSnapshot(const IColumnFileDataProviderPtr & data_provider);
};

} // namespace DM
} // namespace DB
