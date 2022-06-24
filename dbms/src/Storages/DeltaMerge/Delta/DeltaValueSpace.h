// Copyright 2022 PingCAP, Ltd.
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
#include <Storages/DeltaMerge/ColumnFile/ColumnFileInMemory.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/Delta/ColumnFilePersistedSet.h>
#include <Storages/DeltaMerge/Delta/MemTableSet.h>
#include <Storages/DeltaMerge/DeltaIndex.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/Page/PageDefines.h>

namespace DB
{
namespace DM
{
using GenPageId = std::function<PageId()>;
class DeltaValueSpace;
class DeltaValueSnapshot;

using DeltaValueSpacePtr = std::shared_ptr<DeltaValueSpace>;
using DeltaSnapshotPtr = std::shared_ptr<DeltaValueSnapshot>;

class DeltaValueReader;
using DeltaValueReaderPtr = std::shared_ptr<DeltaValueReader>;

using DeltaIndexCompacted = DefaultDeltaTree::CompactedEntries;
using DeltaIndexCompactedPtr = DefaultDeltaTree::CompactedEntriesPtr;
using DeltaIndexIterator = DeltaIndexCompacted::Iterator;

struct DMContext;
struct WriteBatches;
class StoragePool;

class DeltaValueSpace
    : public std::enable_shared_from_this<DeltaValueSpace>
    , private boost::noncopyable
{
public:
    using Lock = std::unique_lock<std::mutex>;

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

    // Protects the operations in this instance.
    mutable std::mutex mutex;

    Poco::Logger * log;

public:
    explicit DeltaValueSpace(PageId id_, const ColumnFilePersisteds & persisted_files = {}, const ColumnFiles & in_memory_files = {});

    explicit DeltaValueSpace(ColumnFilePersistedSetPtr && persisted_file_set_);

    /// Restore the metadata of this instance.
    /// Only called after reboot.
    static DeltaValueSpacePtr restore(DMContext & context, const RowKeyRange & segment_range, PageId id);

    /// The following two methods are just for test purposes
    MemTableSetPtr getMemTableSet() const { return mem_table_set; }
    ColumnFilePersistedSetPtr getPersistedFileSet() const { return persisted_file_set; }

    String simpleInfo() const { return "Delta [" + DB::toString(persisted_file_set->getId()) + "]"; }
    String info() const
    {
        return fmt::format("{}. {}", mem_table_set->info(), persisted_file_set->info());
    }

    bool getLock(Lock & lock) const
    {
        Lock my_lock(mutex);
        if (abandoned)
            return false;
        lock = std::move(my_lock);
        return true;
    }

    /// Abandon this instance.
    void abandon(DMContext & context);

    bool hasAbandoned() const { return abandoned.load(std::memory_order_relaxed); }

    void saveMeta(WriteBatches & wbs) const;

    void recordRemoveColumnFilesPages(WriteBatches & wbs) const;

    /// First check whether 'head_column_files' is exactly the head of column files in this instance.
    ///   If yes, then clone the tail of column files, using ref pages.
    ///   Otherwise, throw an exception.
    ///
    /// Note that this method is expected to be called by some one who already have lock on this instance.
    /// And the `head_column_files` must just reside in `persisted_file_set`.
    std::pair<ColumnFilePersisteds, ColumnFiles>
    checkHeadAndCloneTail(DMContext & context, const RowKeyRange & target_range, const ColumnFiles & head_column_files, WriteBatches & wbs) const;

    PageId getId() const { return persisted_file_set->getId(); }

    size_t getColumnFileCount() const { return persisted_file_set->getColumnFileCount() + mem_table_set->getColumnFileCount(); }
    size_t getRows(bool use_unsaved = true) const
    {
        return use_unsaved ? persisted_file_set->getRows() + mem_table_set->getRows() : persisted_file_set->getRows();
    }
    size_t getBytes(bool use_unsaved = true) const
    {
        return use_unsaved ? persisted_file_set->getBytes() + mem_table_set->getBytes() : persisted_file_set->getBytes();
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
            LOG_FMT_DEBUG(log, "{} Stop create snapshot because updating", simpleInfo());
            return false;
        }
        return true;
    }

    bool releaseUpdating()
    {
        bool v = true;
        if (!is_updating.compare_exchange_strong(v, false))
        {
            LOG_FMT_ERROR(log, "!!!=========================delta [{}] is expected to be updating=========================!!!", getId());
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

    bool ingestColumnFiles(DMContext & context, const RowKeyRange & range, const ColumnFiles & column_files, bool clear_data_in_range);

    /// Flush the data of column files which haven't write to disk yet, and also save the metadata of column files.
    bool flush(DMContext & context);

    /// Compact fragment column files in the delta layer into bigger column files, to save some IOPS during reading.
    /// It does not merge the delta into stable layer.
    /// a.k.a. minor compaction.
    bool compact(DMContext & context);

    /// Create a constant snapshot for read.
    /// Returns empty if this instance is abandoned, you should try again.
    /// for_update: true means this snapshot is created for Segment split/merge, delta merge, or flush.
    DeltaSnapshotPtr createSnapshot(const DMContext & context, bool for_update, CurrentMetrics::Metric type);
};

class DeltaValueSnapshot
    : public std::enable_shared_from_this<DeltaValueSnapshot>
    , private boost::noncopyable
{
    friend class DeltaValueSpace;

private:
    bool is_update;

    // The delta index of cached.
    DeltaIndexPtr shared_delta_index;

    ColumnFileSetSnapshotPtr mem_table_snap;

    ColumnFileSetSnapshotPtr persisted_files_snap;

    // We need a reference to original delta object, to release the "is_updating" lock.
    DeltaValueSpacePtr _delta;

    const CurrentMetrics::Metric type;

public:
    DeltaSnapshotPtr clone()
    {
        if (unlikely(is_update))
            throw Exception("Should not call this method when is_update is true", ErrorCodes::LOGICAL_ERROR);

        auto c = std::make_shared<DeltaValueSnapshot>(type);
        c->is_update = is_update;
        c->shared_delta_index = shared_delta_index;
        c->mem_table_snap = mem_table_snap->clone();
        c->persisted_files_snap = persisted_files_snap->clone();

        c->_delta = _delta;

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
            _delta->releaseUpdating();
        CurrentMetrics::sub(type);
    }

    // Only used when `is_update` is true
    ColumnFiles & getColumnFilesInSnapshot() const
    {
        if (unlikely(!is_update))
            throw Exception("Should not call this method when is_update is true", ErrorCodes::LOGICAL_ERROR);
        /// when `is_update` is true, we just create snapshot for saved files,
        /// so `mem_table_snap` must be nullptr.
        return persisted_files_snap->getColumnFiles();
    }

    ColumnFileSetSnapshotPtr getMemTableSetSnapshot() const { return mem_table_snap; }
    ColumnFileSetSnapshotPtr getPersistedFileSetSnapshot() const { return persisted_files_snap; }

    size_t getColumnFileCount() const { return (mem_table_snap ? mem_table_snap->getColumnFileCount() : 0) + persisted_files_snap->getColumnFileCount(); }
    size_t getRows() const { return (mem_table_snap ? mem_table_snap->getRows() : 0) + persisted_files_snap->getRows(); }
    size_t getBytes() const { return (mem_table_snap ? mem_table_snap->getBytes() : 0) + persisted_files_snap->getBytes(); }
    size_t getDeletes() const { return (mem_table_snap ? mem_table_snap->getDeletes() : 0) + persisted_files_snap->getDeletes(); }

    size_t getMemTableSetRowsOffset() const { return persisted_files_snap->getRows(); }
    size_t getMemTableSetDeletesOffset() const { return persisted_files_snap->getDeletes(); }

    RowKeyRange getSquashDeleteRange() const;

    const auto & getSharedDeltaIndex() { return shared_delta_index; }
};

class DeltaValueReader
{
    friend class DeltaValueInputStream;

private:
    DeltaSnapshotPtr delta_snap;
    // The delta index which we actually use. Could be cloned from shared_delta_index with some updates and compacts.
    // We only keep this member here to prevent it from being released.
    DeltaIndexCompactedPtr _compacted_delta_index;

    ColumnFileSetReaderPtr mem_table_reader;

    ColumnFileSetReaderPtr persisted_files_reader;

    // The columns expected to read. Note that we will do reading exactly in this column order.
    ColumnDefinesPtr col_defs;
    RowKeyRange segment_range;

private:
    DeltaValueReader() = default;

public:
    DeltaValueReader(const DMContext & context_,
                     const DeltaSnapshotPtr & delta_snap_,
                     const ColumnDefinesPtr & col_defs_,
                     const RowKeyRange & segment_range_);

    // If we need to read columns besides pk and version, a DeltaValueReader can NOT be used more than once.
    // This method create a new reader based on then current one. It will reuse some caches in the current reader.
    DeltaValueReaderPtr createNewReader(const ColumnDefinesPtr & new_col_defs);

    void setDeltaIndex(const DeltaIndexCompactedPtr & delta_index_) { _compacted_delta_index = delta_index_; }

    const auto & getDeltaSnap() { return delta_snap; }

    // Use for DeltaMergeBlockInputStream to read delta rows, and merge with stable rows.
    // This method will check whether offset and limit are valid. It only return those valid rows.
    size_t readRows(MutableColumns & output_cols, size_t offset, size_t limit, const RowKeyRange * range);

    // Get blocks or delete_ranges of `ExtraHandleColumn` and `VersionColumn`.
    // If there are continuous blocks, they will be squashed into one block.
    // We use the result to update DeltaTree.
    BlockOrDeletes getPlaceItems(size_t rows_begin, size_t deletes_begin, size_t rows_end, size_t deletes_end);

    bool shouldPlace(const DMContext & context,
                     DeltaIndexPtr my_delta_index,
                     const RowKeyRange & segment_range,
                     const RowKeyRange & relevant_range,
                     UInt64 max_version);
};

class DeltaValueInputStream : public IBlockInputStream
{
private:
    ColumnFileSetInputStream mem_table_input_stream;
    ColumnFileSetInputStream persisted_files_input_stream;

    bool persisted_files_done = false;

public:
    DeltaValueInputStream(const DMContext & context_,
                          const DeltaSnapshotPtr & delta_snap_,
                          const ColumnDefinesPtr & col_defs_,
                          const RowKeyRange & segment_range_)
        : mem_table_input_stream(context_, delta_snap_->getMemTableSetSnapshot(), col_defs_, segment_range_)
        , persisted_files_input_stream(context_, delta_snap_->getPersistedFileSetSnapshot(), col_defs_, segment_range_)
    {}

    String getName() const override { return "DeltaValue"; }
    Block getHeader() const override { return persisted_files_input_stream.getHeader(); }

    Block read() override
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

} // namespace DM
} // namespace DB
