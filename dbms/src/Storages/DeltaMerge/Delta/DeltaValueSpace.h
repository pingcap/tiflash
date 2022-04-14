#pragma once

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Core/Block.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/Delta/DeltaPack.h>
#include <Storages/DeltaMerge/Delta/DeltaPackBlock.h>
#include <Storages/DeltaMerge/Delta/DeltaPackDeleteRange.h>
#include <Storages/DeltaMerge/Delta/DeltaPackFile.h>
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
using DeltaSnapshotPtr   = std::shared_ptr<DeltaValueSnapshot>;

class DeltaValueReader;
using DeltaValueReaderPtr = std::shared_ptr<DeltaValueReader>;

using DeltaIndexCompacted    = DefaultDeltaTree::CompactedEntries;
using DeltaIndexCompactedPtr = DefaultDeltaTree::CompactedEntriesPtr;
using DeltaIndexIterator     = DeltaIndexCompacted::Iterator;

struct DMContext;
struct WriteBatches;
class StoragePool;

static std::atomic_uint64_t NEXT_PACK_ID{0};

class BlockOrDelete
{
private:
    Block  block;
    size_t block_offset;

    RowKeyRange delete_range;

public:
    BlockOrDelete(Block && block_, size_t offset_) : block(block_), block_offset(offset_) {}
    BlockOrDelete(const RowKeyRange & delete_range_) : delete_range(delete_range_) {}

    bool   isBlock() { return (bool)block; }
    auto & getBlock() { return block; };
    auto   getBlockOffset() { return block_offset; }
    auto & getDeleteRange() { return delete_range; }
};

using BlockOrDeletes = std::vector<BlockOrDelete>;

class DeltaValueSpace : public std::enable_shared_from_this<DeltaValueSpace>, private boost::noncopyable
{
    friend class DeltaValueSpaceSnapshot;

public:
    using Lock = std::unique_lock<std::mutex>;

private:
    PageId     id;
    DeltaPacks packs;

    std::atomic<size_t> rows    = 0;
    std::atomic<size_t> bytes   = 0;
    std::atomic<size_t> deletes = 0;

    std::atomic<size_t> unsaved_rows    = 0;
    std::atomic<size_t> unsaved_bytes   = 0;
    std::atomic<size_t> unsaved_deletes = 0;

    /// This instance has been abandoned. Like after merge delta, split/merge.
    std::atomic_bool abandoned = false;
    /// We need to run compact.
    std::atomic_bool shouldCompact = false;
    /// Current segment is being compacted, split, merged or merged delta.
    /// Note that those things can not be done at the same time.
    std::atomic_bool is_updating = false;

    std::atomic<size_t> last_try_flush_rows             = 0;
    std::atomic<size_t> last_try_flush_bytes            = 0;
    std::atomic<size_t> last_try_compact_packs          = 0;
    std::atomic<size_t> last_try_merge_delta_rows       = 0;
    std::atomic<size_t> last_try_merge_delta_bytes      = 0;
    std::atomic<size_t> last_try_split_rows             = 0;
    std::atomic<size_t> last_try_split_bytes            = 0;
    std::atomic<size_t> last_try_place_delta_index_rows = 0;

    DeltaIndexPtr delta_index;

    // Protects the operations in this instance.
    mutable std::mutex mutex;

    Logger * log;

private:
    BlockPtr lastSchema();

    void checkPacks(const DeltaPacks & new_packs);

    void appendPackInner(const DeltaPackPtr & pack);

public:
    DeltaValueSpace(PageId id_, const DeltaPacks & packs_ = {});

    /// Restore the metadata of this instance.
    /// Only called after reboot.
    static DeltaValueSpacePtr restore(DMContext & context, const RowKeyRange & segment_range, PageId id);

    String simpleInfo() const { return "Delta [" + DB::toString(id) + "]"; }
    String info() const
    {
        return "{Delta [" + DB::toString(id) + "]: " + DB::toString(packs.size()) + " packs, " + DB::toString(rows.load()) + " rows, "
            + DB::toString(unsaved_rows.load()) + " unsaved_rows, " + DB::toString(unsaved_bytes.load()) + " unsaved_bytes, "
            + DB::toString(deletes.load()) + " deletes, " + DB::toString(unsaved_deletes.load()) + " unsaved_deletes}";
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

    void recordRemovePacksPages(WriteBatches & wbs) const;

    /// First check whether 'head_packs' is exactly the head of packs in this instance.
    ///   If yes, then clone the tail of packs, using ref pages.
    ///   Otherwise, throw an exception.
    ///
    /// Note that this method is expected to be called by some one who already have lock on this instance.
    DeltaPacks
    checkHeadAndCloneTail(DMContext & context, const RowKeyRange & target_range, const DeltaPacks & head_packs, WriteBatches & wbs) const;

    PageId getId() const { return id; }

    size_t getPackCount() const { return packs.size(); }
    size_t getRows(bool use_unsaved = true) const { return use_unsaved ? rows.load() : rows - unsaved_rows; }
    size_t getBytes(bool use_unsaved = true) const { return use_unsaved ? bytes.load() : bytes - unsaved_bytes; }
    size_t getDeletes() const { return deletes; }

    size_t getUnsavedRows() const { return unsaved_rows; }
    size_t getUnsavedBytes() const { return unsaved_bytes; }
    size_t getUnsavedDeletes() const { return unsaved_deletes; }

    size_t getTotalCacheRows() const;
    size_t getTotalCacheBytes() const;
    size_t getValidCacheRows() const;

    bool isUpdating() const { return is_updating; }
    bool isShouldCompact() const { return shouldCompact; }

    bool tryLockUpdating()
    {
        bool v = false;
        // Other thread is doing structure update, just return.
        if (!is_updating.compare_exchange_strong(v, true))
        {
            LOG_DEBUG(log, simpleInfo() << " Stop create snapshot because updating");
            return false;
        }
        return true;
    }

    bool releaseUpdating()
    {
        bool v = true;
        if (!is_updating.compare_exchange_strong(v, false))
        {
            LOG_ERROR(log, "!!!=========================delta [" << getId() << "] is expected to be updating=========================!!!");
            return false;
        }
        else
            return true;
    }

    std::atomic<size_t> & getLastTryFlushRows() { return last_try_flush_rows; }
    std::atomic<size_t> & getLastTryFlushBytes() { return last_try_flush_bytes; }
    std::atomic<size_t> & getLastTryCompactPacks() { return last_try_compact_packs; }
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

    bool appendPack(DMContext & context, const DeltaPackPtr & pack);

    bool appendToCache(DMContext & context, const Block & block, size_t offset, size_t limit);

    bool appendDeleteRange(DMContext & context, const RowKeyRange & delete_range);

    bool ingestPacks(DMContext & context, const RowKeyRange & range, const DeltaPacks & packs, bool clear_data_in_range);

    /// Flush the data of packs which haven't write to disk yet, and also save the metadata of packs.
    bool flush(DMContext & context);

    /// Compacts fragment packs into bigger one, to save some IOPS during reading.
    bool compact(DMContext & context);

    /// Create a constant snapshot for read.
    /// Returns empty if this instance is abandoned, you should try again.
    /// for_update: true means this snapshot is created for Segment split/merge, delta merge, or flush.
    DeltaSnapshotPtr createSnapshot(const DMContext & context, bool for_update, CurrentMetrics::Metric type);
};

class DeltaValueSnapshot : public std::enable_shared_from_this<DeltaValueSnapshot>, private boost::noncopyable
{
    friend class DeltaValueSpace;

private:
    bool is_update;

    // The delta index of cached.
    DeltaIndexPtr shared_delta_index;

    StorageSnapshotPtr storage_snap;

    DeltaPacks packs;
    size_t     rows;
    size_t     bytes;
    size_t     deletes;

    bool   is_common_handle;
    size_t rowkey_column_size;

    // We need a reference to original delta object, to release the "is_updating" lock.
    DeltaValueSpacePtr _delta;

    CurrentMetrics::Metric type;

public:
    DeltaSnapshotPtr clone()
    {
        if (unlikely(is_update))
            throw Exception("Should not call this method when is_update is true", ErrorCodes::LOGICAL_ERROR);

        auto c                = std::make_shared<DeltaValueSnapshot>(type);
        c->is_update          = is_update;
        c->shared_delta_index = shared_delta_index;
        c->storage_snap       = storage_snap;
        c->packs              = packs;
        c->rows               = rows;
        c->bytes              = bytes;
        c->deletes            = deletes;
        c->is_common_handle   = is_common_handle;
        c->rowkey_column_size = rowkey_column_size;

        c->_delta = _delta;

        return c;
    }

    DeltaValueSnapshot(CurrentMetrics::Metric type_)
    {
        type = type_;
        CurrentMetrics::add(type);
    }

    ~DeltaValueSnapshot()
    {
        if (is_update)
            _delta->releaseUpdating();
        CurrentMetrics::sub(type);
    }

    DeltaPacks & getPacks() { return packs; }

    size_t getPackCount() const { return packs.size(); }
    size_t getRows() const { return rows; }
    size_t getBytes() const { return bytes; }
    size_t getDeletes() const { return deletes; }

    RowKeyRange getSquashDeleteRange() const;

    const auto & getStorageSnapshot() { return storage_snap; }
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

    // The columns expected to read. Note that we will do reading exactly in this column order.
    ColumnDefinesPtr col_defs;
    RowKeyRange      segment_range;

    // The row count of each pack. Cache here to speed up checking.
    std::vector<size_t> pack_rows;
    // The cumulative rows of packs. Used to fast locate specific packs according to rows offset by binary search.
    std::vector<size_t> pack_rows_end;

    std::vector<DeltaPackReaderPtr> pack_readers;

private:
    DeltaValueReader() = default;

    Block readPKVersion(size_t offset, size_t limit);

public:
    DeltaValueReader(const DMContext &        context_,
                     const DeltaSnapshotPtr & delta_snap_,
                     const ColumnDefinesPtr & col_defs_,
                     const RowKeyRange &      segment_range_);

    // If we need to read columns besides pk and version, a DeltaValueReader can NOT be used more than once.
    // This method create a new reader based on then current one. It will reuse some cachees in the current reader.
    DeltaValueReaderPtr createNewReader(const ColumnDefinesPtr & new_col_defs);

    void setDeltaIndex(const DeltaIndexCompactedPtr & delta_index_) { _compacted_delta_index = delta_index_; }

    const auto & getDeltaSnap() { return delta_snap; }

    // Use for DeltaMergeBlockInputStream to read delta rows, and merge with stable rows.
    // This method will check whether offset and limit are valid. It only return those valid rows.
    size_t readRows(MutableColumns & output_columns, size_t offset, size_t limit, const RowKeyRange * range);

    // Get blocks or delete_ranges of `ExtraHandleColumn` and `VersionColumn`.
    // If there are continuous blocks, they will be squashed into one block.
    // We use the result to update DeltaTree.
    BlockOrDeletes getPlaceItems(size_t rows_begin, size_t deletes_begin, size_t rows_end, size_t deletes_end);

    bool shouldPlace(const DMContext &   context,
                     DeltaIndexPtr       my_delta_index,
                     const RowKeyRange & segment_range,
                     const RowKeyRange & relevant_range,
                     UInt64              max_version);
};

class DeltaValueInputStream : public IBlockInputStream
{
private:
    DeltaValueReader reader;
    DeltaPacks &     packs;
    size_t           pack_count;

    DeltaPackReaderPtr cur_pack_reader = {};
    size_t             next_pack_index = 0;

public:
    DeltaValueInputStream(const DMContext &        context_,
                          const DeltaSnapshotPtr & delta_snap_,
                          const ColumnDefinesPtr & col_defs_,
                          const RowKeyRange &      segment_range_)
        : reader(context_, delta_snap_, col_defs_, segment_range_), packs(reader.delta_snap->getPacks()), pack_count(packs.size())
    {
    }

    String getName() const override { return "DeltaValue"; }
    Block  getHeader() const override { return toEmptyBlock(*(reader.col_defs)); }

    Block read() override
    {
        while (cur_pack_reader || next_pack_index < pack_count)
        {
            if (!cur_pack_reader)
            {
                if (packs[next_pack_index]->isDeleteRange())
                {
                    ++next_pack_index;
                    continue;
                }
                else
                {
                    cur_pack_reader = reader.pack_readers[next_pack_index];
                    ++next_pack_index;
                }
            }
            Block block = cur_pack_reader->readNextBlock();
            if (block)
                return block;
            else
                cur_pack_reader = {};
        }
        return {};
    }
};

} // namespace DM
} // namespace DB
