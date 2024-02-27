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

#include <Columns/ColumnsCommon.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Core/Block.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileBig.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDeleteRange.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileInMemory.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/DMContext_fwd.h>
#include <Storages/DeltaMerge/Delta/ColumnFilePersistedSet.h>
#include <Storages/DeltaMerge/Delta/MemTableSet.h>
#include <Storages/DeltaMerge/DeltaIndex.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>
#include <Storages/Page/PageDefinesBase.h>


namespace DB::DM
{
namespace tests
{
class SegmentReadTaskTest;
}

using GenPageId = std::function<PageIdU64()>;
class DeltaValueSpace;
class DeltaValueSnapshot;

using DeltaValueSpacePtr = std::shared_ptr<DeltaValueSpace>;
using DeltaSnapshotPtr = std::shared_ptr<DeltaValueSnapshot>;

class DeltaValueReader;
using DeltaValueReaderPtr = std::shared_ptr<DeltaValueReader>;

using DeltaIndexCompacted = DefaultDeltaTree::CompactedEntries;
using DeltaIndexCompactedPtr = DefaultDeltaTree::CompactedEntriesPtr;
using DeltaIndexIterator = DeltaIndexCompacted::Iterator;

struct WriteBatches;
class StoragePool;

class DeltaValueSpace
    : public std::enable_shared_from_this<DeltaValueSpace>
    , private boost::noncopyable
{
public:
    using Lock = std::unique_lock<std::recursive_mutex>;

private:
    /// column files in `persisted_file_set` are all persisted in disks and can be restored after restart.
    /// column files in `mem_table_set` just resides in memory.
    ///
    /// Note that `persisted_file_set` and `mem_table_set` also forms a one-dimensional space
    /// Specifically, files in `persisted_file_set` precedes files in `mem_table_set`.
    ColumnFilePersistedSetPtr persisted_file_set;
    MemTableSetPtr mem_table_set;

    /// This instance has been abandoned. Like after merge delta, split/merge.
    std::atomic_bool abandoned = false;
    /// Current segment is being compacted, split, merged or merged delta.
    /// Note that those things can not be done at the same time.
    std::atomic_bool is_updating = false;

    /// Note that it's safe to do multiple flush concurrently but only one of them can succeed,
    /// and other thread's work is just a waste of resource.
    /// So we only allow one flush task running at any time to aviod waste resource.
    std::atomic_bool is_flushing = false;

    std::atomic<size_t> last_try_flush_rows = 0;
    std::atomic<size_t> last_try_flush_bytes = 0;
    std::atomic<size_t> last_try_compact_column_files = 0;
    std::atomic<size_t> last_try_merge_delta_rows = 0;
    std::atomic<size_t> last_try_merge_delta_bytes = 0;
    std::atomic<size_t> last_try_split_rows = 0;
    std::atomic<size_t> last_try_split_bytes = 0;
    std::atomic<size_t> last_try_place_delta_index_rows = 0;

    DeltaIndexPtr delta_index;
    // `delta_index_epoch` is used in disaggregated mode by compute-nodes to identify if the delta_index has changed.
    // To avoid persisting it, we use `std::chrono::steady_clock` to make it monotonic increase.
    // Accuracy of `std::chrono::steady_clock` in Linux is nanosecond. This is much smaller than the interval between two flushes.
    UInt64 delta_index_epoch = std::chrono::steady_clock::now().time_since_epoch().count();

    // Protects the operations in this instance.
    // It is a recursive_mutex because the lock may be also used by the parent segment as its update lock.
    mutable std::recursive_mutex mutex;

    LoggerPtr log;

private:
    void saveMeta(WriteBuffer & buf) const;

public:
    explicit DeltaValueSpace(
        PageIdU64 id_,
        const ColumnFilePersisteds & persisted_files = {},
        const ColumnFiles & in_memory_files = {});

    explicit DeltaValueSpace(ColumnFilePersistedSetPtr && persisted_file_set_);

    /// Restore the metadata of this instance.
    /// Only called after reboot.
    static DeltaValueSpacePtr restore(DMContext & context, const RowKeyRange & segment_range, PageIdU64 id);
    /// Restore from a checkpoint from other peer.
    /// Only used in FAP.
    static DeltaValueSpacePtr restore(
        DMContext & context,
        const RowKeyRange & segment_range,
        ReadBuffer & buf,
        PageIdU64 id);


    static DeltaValueSpacePtr createFromCheckpoint( //
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
    void resetLogger(const LoggerPtr & segment_log)
    {
        log = segment_log;
        mem_table_set->resetLogger(segment_log);
        persisted_file_set->resetLogger(segment_log);
    }

    /// The following 3 methods are just for test purposes
    MemTableSetPtr getMemTableSet() const { return mem_table_set; }
    ColumnFilePersistedSetPtr getPersistedFileSet() const { return persisted_file_set; }
    UInt64 getDeltaIndexEpoch() const { return delta_index_epoch; }

    String simpleInfo() const { return "<delta_id=" + DB::toString(persisted_file_set->getId()) + ">"; }
    String info() const { return fmt::format("{}. {}", mem_table_set->info(), persisted_file_set->info()); }

    std::optional<Lock> getLock() const
    {
        Lock my_lock(mutex);
        if (abandoned)
            return std::nullopt;
        return my_lock;
    }

    /// Abandon this instance.
    void abandon(DMContext & context);

    bool hasAbandoned() const { return abandoned.load(std::memory_order_relaxed); }

    void saveMeta(WriteBatches & wbs) const;
    std::string serializeMeta() const;

    void recordRemoveColumnFilesPages(WriteBatches & wbs) const;

    /**
     * Clone these newly appended column files since `update_snapshot` was created.
     * The clone is implemented by creating ref pages.
     *
     * Exceptions will be thrown if:
     * - The `update_snapshot` is not SnapshotForUpdate.
     * - Some column files in the `update_snapshot` is missing in the current instance.
     */
    std::pair<ColumnFiles, ColumnFilePersisteds> cloneNewlyAppendedColumnFiles(
        const Lock & lock,
        DMContext & context,
        const RowKeyRange & target_range,
        const DeltaValueSnapshot & update_snapshot,
        WriteBatches & wbs) const;
    std::pair<ColumnFiles, ColumnFilePersisteds> cloneAllColumnFiles(
        const Lock & lock,
        DMContext & context,
        const RowKeyRange & target_range,
        WriteBatches & wbs) const;

    PageIdU64 getId() const { return persisted_file_set->getId(); }

    size_t getColumnFileCount() const
    {
        return persisted_file_set->getColumnFileCount() + mem_table_set->getColumnFileCount();
    }
    size_t getRows(bool use_unsaved = true) const
    {
        return use_unsaved ? persisted_file_set->getRows() + mem_table_set->getRows() : persisted_file_set->getRows();
    }
    size_t getBytes(bool use_unsaved = true) const
    {
        return use_unsaved ? persisted_file_set->getBytes() + mem_table_set->getBytes()
                           : persisted_file_set->getBytes();
    }
    size_t getDeletes() const { return persisted_file_set->getDeletes() + mem_table_set->getDeletes(); }

    size_t getUnsavedRows() const { return mem_table_set->getRows(); }
    size_t getUnsavedBytes() const { return mem_table_set->getBytes(); }

    size_t getTotalCacheRows() const;
    size_t getTotalCacheBytes() const;
    size_t getValidCacheRows() const;

    bool isFlushing() const { return is_flushing; }

    bool isUpdating() const { return is_updating; }

    bool tryLockUpdating()
    {
        bool v = false;
        // Other thread is doing structure update, just return.
        if (!is_updating.compare_exchange_strong(v, true))
        {
            LOG_DEBUG(
                log,
                "Cannot get update lock because DeltaValueSpace is updating. Current update operation will be "
                "discarded, delta={}",
                simpleInfo());
            return false;
        }
        return true;
    }

    bool releaseUpdating()
    {
        bool v = true;
        if (!is_updating.compare_exchange_strong(v, false))
        {
            LOG_ERROR(
                log,
                "!!!========================= delta={} is expected to be updating=========================!!!",
                simpleInfo());
            return false;
        }
        else
            return true;
    }

    std::atomic<size_t> & getLastTryFlushRows() { return last_try_flush_rows; }
    std::atomic<size_t> & getLastTryFlushBytes() { return last_try_flush_bytes; }
    std::atomic<size_t> & getLastTryCompactColumnFiles() { return last_try_compact_column_files; }
    std::atomic<size_t> & getLastTryMergeDeltaRows() { return last_try_merge_delta_rows; }
    std::atomic<size_t> & getLastTryMergeDeltaBytes() { return last_try_merge_delta_bytes; }
    std::atomic<size_t> & getLastTrySplitRows() { return last_try_split_rows; }
    std::atomic<size_t> & getLastTrySplitBytes() { return last_try_split_bytes; }
    std::atomic<size_t> & getLastTryPlaceDeltaIndexRows() { return last_try_place_delta_index_rows; }

    size_t getDeltaIndexBytes()
    {
        std::scoped_lock lock(mutex);
        return delta_index->getDeltaTree()->getBytes();
    }
    size_t getPlacedDeltaRows() const
    {
        std::scoped_lock lock(mutex);
        return delta_index->getPlacedStatus().first;
    }
    size_t getPlacedDeltaDeletes() const
    {
        std::scoped_lock lock(mutex);
        return delta_index->getPlacedStatus().second;
    }

    size_t updatesInDeltaTree() const
    {
        std::scoped_lock lock(mutex);

        auto delta_tree = delta_index->getDeltaTree();
        return delta_tree->numInserts() + delta_tree->numDeletes();
    }

public:
    /// The following methods returning false means this operation failed, caused by other threads could have done
    /// some updates on this instance. E.g. this instance have been abandoned.
    /// Caller should try again from the beginning.

    bool appendColumnFile(DMContext & context, const ColumnFilePtr & column_file);

    bool appendToCache(DMContext & context, const Block & block, size_t offset, size_t limit);

    bool appendDeleteRange(DMContext & context, const RowKeyRange & delete_range);

    bool ingestColumnFiles(
        DMContext & context,
        const RowKeyRange & range,
        const ColumnFiles & column_files,
        bool clear_data_in_range);

    /// Flush the data of column files which haven't write to disk yet, and also save the metadata of column files.
    bool flush(DMContext & context);

    /// Compact fragment column files in the delta layer into bigger column files, to save some IOPS during reading.
    /// It does not merge the delta into stable layer.
    /// a.k.a. minor compaction.
    bool compact(DMContext & context);

    /**
     * Create a snapshot for read. The snapshot always contains memtable, persisted delta and stable.
     *
     * WARN: Although it is named as "snapshot", the content may be mutable. See doc of `for_update` for details.
     *
     * @param for_update: Specify `true` if you want to make structural updates based on the content of this snapshot,
     *                    like flush, split, merge, delta merge, etc. SnapshotForUpdate is exclusive: you can only
     *                    create one at a time. When there is an alive SnapshotForUpdate, you can still create
     *                    snapshots not for update.
     *
     *                    When `for_update == false`, the memtable of this snapshot may be mutable. For example, there
     *                    may be concurrent writes when you hold this snapshot, causing the memtable to be changed.
     *                    However it is guaranteed that only new data will be appended. No data will be lost when you
     *                    are holding this snapshot.
     *
     *                    When `for_update == true`, it is guaranteed that the content of this snapshot will never
     *                    change.
     *
     * @returns empty if this instance is abandoned. In this case you may want to try again with a new instance.
     * @returns empty if `for_update == true` is specified and there is another alive for_update snapshot.
     */
    DeltaSnapshotPtr createSnapshot(const DMContext & context, bool for_update, CurrentMetrics::Metric type);
};

class DeltaValueSnapshot
    : public std::enable_shared_from_this<DeltaValueSnapshot>
    , private boost::noncopyable
{
    friend class DeltaValueSpace;
    friend struct DB::DM::Remote::Serializer;

#ifndef DBMS_PUBLIC_GTEST
private:
#else
public:
#endif
    bool is_update{false};

    // The delta index of cached.
    DeltaIndexPtr shared_delta_index;
    UInt64 delta_index_epoch = 0;

    ColumnFileSetSnapshotPtr mem_table_snap;

    ColumnFileSetSnapshotPtr persisted_files_snap;

    // We need a reference to original delta object, to release the "is_updating" lock.
    DeltaValueSpacePtr delta;

    const CurrentMetrics::Metric type;

public:
    DeltaSnapshotPtr clone()
    {
        // We only allow one for_update snapshots to exist, so it cannot be cloned.
        RUNTIME_CHECK(!is_update);

        auto c = std::make_shared<DeltaValueSnapshot>(type);
        c->is_update = is_update;
        c->shared_delta_index = shared_delta_index;
        c->delta_index_epoch = delta_index_epoch;
        c->mem_table_snap = mem_table_snap->clone();
        c->persisted_files_snap = persisted_files_snap->clone();

        c->delta = delta;

        return c;
    }

    explicit DeltaValueSnapshot(CurrentMetrics::Metric type_)
        : type(type_)
    {
        CurrentMetrics::add(type);
    }

    ~DeltaValueSnapshot()
    {
        if (is_update)
            delta->releaseUpdating();
        CurrentMetrics::sub(type);
    }

    ColumnFileSetSnapshotPtr getMemTableSetSnapshot() const { return mem_table_snap; }
    ColumnFileSetSnapshotPtr getPersistedFileSetSnapshot() const { return persisted_files_snap; }

    size_t getColumnFileCount() const
    {
        return mem_table_snap->getColumnFileCount() + persisted_files_snap->getColumnFileCount();
    }
    size_t getRows() const { return mem_table_snap->getRows() + persisted_files_snap->getRows(); }
    size_t getBytes() const { return mem_table_snap->getBytes() + persisted_files_snap->getBytes(); }
    size_t getDeletes() const { return mem_table_snap->getDeletes() + persisted_files_snap->getDeletes(); }

    size_t getMemTableSetRowsOffset() const { return persisted_files_snap->getRows(); }
    size_t getMemTableSetDeletesOffset() const { return persisted_files_snap->getDeletes(); }

    RowKeyRange getSquashDeleteRange() const;

    const auto & getSharedDeltaIndex() const { return shared_delta_index; }
    size_t getDeltaIndexEpoch() const { return delta_index_epoch; }

    bool isForUpdate() const { return is_update; }

    void setMemTableSetSnapshot(const ColumnFileSetSnapshotPtr & mem_table_snap_) { mem_table_snap = mem_table_snap_; }
};

class DeltaValueReader
{
    friend class DeltaValueInputStream;

private:
    DeltaSnapshotPtr delta_snap;
    // The delta index which we actually use. Could be cloned from shared_delta_index with some updates and compacts.
    // We only keep this member here to prevent it from being released.
    DeltaIndexCompactedPtr compacted_delta_index;

    ColumnFileSetReaderPtr mem_table_reader;

    ColumnFileSetReaderPtr persisted_files_reader;

    // The columns expected to read. Note that we will do reading exactly in this column order.
    ColumnDefinesPtr col_defs;
    RowKeyRange segment_range;

private:
    DeltaValueReader() = default;

public:
    DeltaValueReader(
        const DMContext & context_,
        const DeltaSnapshotPtr & delta_snap_,
        const ColumnDefinesPtr & col_defs_,
        const RowKeyRange & segment_range_);

    // If we need to read columns besides pk and version, a DeltaValueReader can NOT be used more than once.
    // This method create a new reader based on then current one. It will reuse some caches in the current reader.
    DeltaValueReaderPtr createNewReader(const ColumnDefinesPtr & new_col_defs);

    void setDeltaIndex(const DeltaIndexCompactedPtr & delta_index_) { compacted_delta_index = delta_index_; }

    const auto & getDeltaSnap() { return delta_snap; }

    // Use for DeltaMergeBlockInputStream to read delta rows, and merge with stable rows.
    // This method will check whether offset and limit are valid. It only return those valid rows.
    size_t readRows(
        MutableColumns & output_cols,
        size_t offset,
        size_t limit,
        const RowKeyRange * range,
        std::vector<UInt32> * row_ids = nullptr);

    // Get blocks or delete_ranges of `ExtraHandleColumn` and `VersionColumn`.
    // If there are continuous blocks, they will be squashed into one block.
    // We use the result to update DeltaTree.
    BlockOrDeletes getPlaceItems(size_t rows_begin, size_t deletes_begin, size_t rows_end, size_t deletes_end);

    bool shouldPlace(
        const DMContext & context,
        DeltaIndexPtr my_delta_index,
        const RowKeyRange & segment_range,
        const RowKeyRange & relevant_range,
        UInt64 max_version);
};

class DeltaValueInputStream : public SkippableBlockInputStream
{
private:
    ColumnFileSetInputStream mem_table_input_stream;
    ColumnFileSetInputStream persisted_files_input_stream;

    bool persisted_files_done = false;
    size_t read_rows = 0;

public:
    DeltaValueInputStream(
        const DMContext & context_,
        const DeltaSnapshotPtr & delta_snap_,
        const ColumnDefinesPtr & col_defs_,
        const RowKeyRange & segment_range_)
        : mem_table_input_stream(context_, delta_snap_->getMemTableSetSnapshot(), col_defs_, segment_range_)
        , persisted_files_input_stream(context_, delta_snap_->getPersistedFileSetSnapshot(), col_defs_, segment_range_)
    {}

    String getName() const override { return "DeltaValue"; }
    Block getHeader() const override { return persisted_files_input_stream.getHeader(); }

    bool getSkippedRows(size_t &) override { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

    /// Skip next block in the stream.
    /// Return the number of rows skipped.
    /// Return 0 if meet the end of the stream.
    size_t skipNextBlock() override
    {
        size_t skipped_rows = 0;
        if (persisted_files_done)
        {
            skipped_rows = mem_table_input_stream.skipNextBlock();
            read_rows += skipped_rows;
            return skipped_rows;
        }

        if (skipped_rows = persisted_files_input_stream.skipNextBlock(); skipped_rows > 0)
        {
            read_rows += skipped_rows;
            return skipped_rows;
        }
        else
        {
            persisted_files_done = true;
            skipped_rows = mem_table_input_stream.skipNextBlock();
            read_rows += skipped_rows;
            return skipped_rows;
        }
    }

    Block readWithFilter(const IColumn::Filter & filter) override
    {
        auto block = read();
        if (size_t passed_count = countBytesInFilter(filter); passed_count != block.rows())
        {
            for (auto & col : block)
            {
                col.column = col.column->filter(filter, passed_count);
            }
        }
        return block;
    }

    Block read() override
    {
        auto block = doRead();
        block.setStartOffset(read_rows);
        read_rows += block.rows();
        return block;
    }

    // Read block from old to new.
    Block doRead()
    {
        if (persisted_files_done)
            return mem_table_input_stream.read();

        Block block = persisted_files_input_stream.read();
        if (block)
            return block;
        else
        {
            persisted_files_done = true;
            return mem_table_input_stream.read();
        }
    }
};

} // namespace DB::DM
