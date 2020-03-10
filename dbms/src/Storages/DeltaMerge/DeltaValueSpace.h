#pragma once

#include <Common/Exception.h>
#include <Core/Block.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/WriteBatch.h>

namespace DB
{
namespace DM
{

using GenPageId = std::function<PageId()>;
class DeltaValueSpace;
using DeltaValueSpacePtr = std::shared_ptr<DeltaValueSpace>;

struct BlockOrDelete
{
    BlockOrDelete() = default;
    BlockOrDelete(Block && block_) : block(block_) {}
    BlockOrDelete(const HandleRange & delete_range_) : delete_range(delete_range_) {}

    Block       block;
    HandleRange delete_range;
};
using BlockOrDeletes = std::vector<BlockOrDelete>;

class DeltaValueSpace : public std::enable_shared_from_this<DeltaValueSpace>, private boost::noncopyable
{
public:
    static const UInt64 CURRENT_VERSION;
    using BlockPtr = std::shared_ptr<Block>;
    using Lock     = std::unique_lock<std::mutex>;

    struct Cache
    {
        Cache(const Block & header) : block(header.cloneEmpty()) {}
        std::mutex mutex;
        Block      block;
    };
    using CachePtr      = std::shared_ptr<Cache>;
    using ColIdToOffset = std::unordered_map<ColId, size_t>;

    struct Pack
    {
        UInt64      rows;
        UInt64      bytes;
        BlockPtr    schema;
        HandleRange delete_range;
        PageId      data_page = 0;

        /// The members below are not serialized.

        CachePtr cache;
        size_t   cache_offset = 0;

        ColIdToOffset colid_to_offset;

        // Already persisted to disk or not.
        bool saved = false;

        bool isDeleteRange() const { return !delete_range.none(); }
        bool isCached() const { return !isDeleteRange() && (bool)cache; }
        /// This pack is not a delete range, the data in it has not been saved to disk.
        bool isMutable() const { return !isDeleteRange() && data_page == 0; }
        /// This pack's metadata has been saved to disk.
        bool isSaved() const { return saved; }
        void setSchema(const BlockPtr & schema_)
        {
            schema = schema_;
            colid_to_offset.clear();
            for (size_t i = 0; i < schema->columns(); ++i)
                colid_to_offset.emplace(schema->getByPosition(i).column_id, i);
        }

        String toString()
        {
            return "{rows:" + DB::toString(rows)                //
                + ",bytes:" + DB::toString(bytes)               //
                + ",has_schema:" + DB::toString((bool)schema)   //
                + ",delete_range:" + delete_range.toString()    //
                + ",data_page:" + DB::toString(data_page)       //
                + ",has_cache:" + DB::toString((bool)cache)     //
                + ",cache_offset:" + DB::toString(cache_offset) //
                + ",saved:" + DB::toString(saved) + "}";
        }
    };
    using PackPtr = std::shared_ptr<Pack>;
    using Packs   = std::vector<PackPtr>;

    struct Snapshot : public std::enable_shared_from_this<Snapshot>, private boost::noncopyable
    {
        bool is_update;

        DeltaValueSpacePtr delta;
        StorageSnapshotPtr storage_snap;

        Packs  packs;
        size_t rows;
        size_t deletes;

        ColumnDefines       column_defines;
        std::vector<size_t> pack_rows;
        std::vector<size_t> pack_rows_end; // Speed up pack search.

        // The data of packs when reading.
        std::vector<Columns> packs_data;

        ~Snapshot()
        {
            if (is_update)
            {
                bool v = true;
                if (!delta->is_updating.compare_exchange_strong(v, false))
                {
                    Logger * logger = &Logger::get("DeltaValueSpace::Snapshot");
                    LOG_ERROR(logger,
                              "!!!=========================delta [" << delta->getId()
                                                                    << "] is expected to be updating=========================!!!");
                }
            }
        }

        size_t getPackCount() { return packs.size(); }
        size_t getRows() { return rows; }
        size_t getDeletes() { return deletes; }

        void                prepare(const DMContext & context, const ColumnDefines & column_defines_);
        BlockInputStreamPtr prepareForStream(const DMContext & context, const ColumnDefines & column_defines_);

        const Columns & getColumnsOfPack(size_t pack_index, size_t col_num);

        size_t         read(const HandleRange & range, MutableColumns & output_columns, size_t offset, size_t limit);
        Block          read(size_t col_num, size_t offset, size_t limit);
        Block          read(size_t pack_index);
        BlockOrDeletes getMergeBlocks(size_t rows_begin, size_t deletes_begin, size_t rows_end, size_t deletes_end);
    };
    using SnapshotPtr = std::shared_ptr<Snapshot>;

private:
    PageId id;
    Packs  packs;

    std::atomic<size_t> rows    = 0;
    std::atomic<size_t> bytes   = 0;
    std::atomic<size_t> deletes = 0;

    std::atomic<size_t> unsaved_rows    = 0;
    std::atomic<size_t> unsaved_deletes = 0;

    /// This instance has been abandoned. Like after merge delta, split/merge.
    std::atomic_bool abandoned = false;
    /// We need to run compact.
    std::atomic_bool shouldCompact = false;
    /// Current segment is being compacted, split, merged or merged delta.
    /// Note that those things can not be done at the same time.
    std::atomic_bool is_updating = false;

    std::atomic<size_t> last_try_flush_rows       = 0;
    std::atomic<size_t> last_try_compact_packs    = 0;
    std::atomic<size_t> last_try_merge_delta_rows = 0;
    std::atomic<size_t> last_try_split_rows       = 0;

    CachePtr last_cache;

    // Protects the operations in this instance.
    mutable std::mutex mutex;

    Logger * log;

private:
    BlockPtr lastSchema();

    void setUp();

    void checkNewPacks(const Packs & new_packs);

public:
    DeltaValueSpace(PageId id_, const Packs & packs_ = {});

    String simpleInfo() const { return "Delta [" + DB::toString(id) + "]"; }
    String info() const
    {
        return "{Delta [" + DB::toString(id) + "]: " + DB::toString(packs.size()) + " packs, " + DB::toString(rows.load()) + " rows, "
            + DB::toString(unsaved_rows.load()) + " unsaved_rows, " + DB::toString(deletes.load()) + " deletes, "
            + DB::toString(unsaved_deletes.load()) + " unsaved_deletes}";
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
    void abandon()
    {
        bool v = false;
        if (!abandoned.compare_exchange_strong(v, true))
            throw Exception("Try to abandon a already abandoned DeltaValueSpace", ErrorCodes::LOGICAL_ERROR);
    }

    bool hasAbandoned() { return abandoned.load(std::memory_order_relaxed); }

    /// Restore the metadata of this instance.
    /// Only called after reboot.
    void restore(DMContext & context);

    void saveMeta(WriteBatches & wbs) const;

    void recordRemovePacksPages(WriteBatches & wbs) const;

    /// First check whether 'head_packs' is exactly the head of packs in this instance.
    ///   If yes, then clone the tail of packs, using ref pages.
    ///   Otherwise, throw an exception.
    ///
    /// Note that this method is expected to be called by some one who already have lock on this instance.
    Packs checkHeadAndCloneTail(DMContext & context, const HandleRange & target_range, const Packs & head_packs, WriteBatches & wbs) const;

    PageId getId() const { return id; }

    size_t getPackCount() const { return packs.size(); }
    size_t getRows(bool use_unsaved = true) const { return use_unsaved ? rows.load() : rows - unsaved_rows; }
    size_t getBytes() const { return bytes; }
    size_t getDeletes() const { return deletes; }

    size_t getUnsavedRows() const { return unsaved_rows; }
    size_t getUnsavedDeletes() const { return unsaved_deletes; }

    size_t getTotalCacheRows() const;
    size_t getTotalCacheBytes() const;
    size_t getValidCacheRows() const;

    bool isUpdating() const { return is_updating; }
    bool isShouldCompact() const { return shouldCompact; }

    std::atomic<size_t> & getLastTryFlushRows() { return last_try_flush_rows; }
    std::atomic<size_t> & getLastTryCompactPacks() { return last_try_compact_packs; }
    std::atomic<size_t> & getLastTryMergeDeltaRows() { return last_try_merge_delta_rows; }
    std::atomic<size_t> & getLastTrySplitRows() { return last_try_split_rows; }

public:
    static PageId writePackData(DMContext & context, const Block & block, size_t offset, size_t limit, WriteBatches & wbs);

    static PackPtr writePack(DMContext & context, const Block & block, size_t offset, size_t limit, WriteBatches & wbs);

    /// Return false means this operation failed, caused by other threads could have done some updates on this instance. E.g. this instance have been abandoned.
    /// Caller should try again from the beginning.
    bool appendToDisk(DMContext & context, const PackPtr & pack);

    bool appendToCache(DMContext & context, const Block & block, size_t offset, size_t limit);

    bool appendDeleteRange(DMContext & context, const HandleRange & delete_range);

    /// Flush the data of packs which haven't write to disk yet, and also save the metadata of packs.
    bool flush(DMContext & context);

    /// Compacts the fragment packs into bigger one, to save some IOPS during reading.
    bool compact(DMContext & context);

    /// Create a constant snapshot for read.
    /// Returns empty if this instance is abandoned, you should try again.
    SnapshotPtr createSnapshot(const DMContext & context, bool is_update = false);
};

using Pack             = DeltaValueSpace::Pack;
using PackPtr          = DeltaValueSpace::PackPtr;
using Packs            = DeltaValueSpace::Packs;
using DeltaSnapshot    = DeltaValueSpace::Snapshot;
using DeltaSnapshotPtr = DeltaValueSpace::SnapshotPtr;

} // namespace DM
} // namespace DB
