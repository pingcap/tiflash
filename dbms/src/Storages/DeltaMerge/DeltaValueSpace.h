#pragma once

#include <Common/Exception.h>
#include <Core/Block.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/Page/PageDefines.h>

namespace DB
{
namespace DM
{

using GenPageId = std::function<PageId()>;
class DeltaValueSpace;
using DeltaValueSpacePtr = std::shared_ptr<DeltaValueSpace>;
struct WriteBatches;
class StoragePool;
struct DMContext;

struct BlockOrDelete
{
    BlockOrDelete() = default;
    BlockOrDelete(Block && block_) : block(block_) {}
    BlockOrDelete(const HandleRange & delete_range_) : delete_range(delete_range_) {}

    Block       block;
    HandleRange delete_range;
};
using BlockOrDeletes = std::vector<BlockOrDelete>;

class DeltaValueSpace : private boost::noncopyable
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
        // Can be appended into new rows or not.
        bool appendable = true;

        bool isDeleteRange() const { return !delete_range.none(); }
        bool isCached() const { return !isDeleteRange() && (bool)cache; }
        /// Whether its column data can be flushed.
        bool dataFlushable() const { return !isDeleteRange() && data_page == 0; }
        /// This pack is the last one, and not a delete range, and can be appended into new rows.
        bool isAppendable() const { return !isDeleteRange() && data_page == 0 && appendable && (bool)cache; }
        /// This pack's metadata has been saved to disk.
        bool isSaved() const { return saved; }
        void setSchema(const BlockPtr & schema_)
        {
            schema = schema_;
            colid_to_offset.clear();
            for (size_t i = 0; i < schema->columns(); ++i)
                colid_to_offset.emplace(schema->getByPosition(i).column_id, i);
        }

        std::pair<DataTypePtr, MutableColumnPtr> getDataTypeAndEmptyColumn(ColId column_id) const
        {
            // Note that column_id must exist
            auto index    = colid_to_offset.at(column_id);
            auto col_type = schema->getByPosition(index).type;
            return {col_type, col_type->createColumn()};
        }

        String toString()
        {
            String s = "{rows:" + DB::toString(rows)                       //
                + ",bytes:" + DB::toString(bytes)                          //
                + ",has_schema:" + DB::toString((bool)schema)              //
                + ",delete_range:" + delete_range.toString()               //
                + ",data_page:" + DB::toString(data_page)                  //
                + ",has_cache:" + DB::toString((bool)cache)                //
                + ",cache_offset:" + DB::toString(cache_offset)            //
                + ",saved:" + DB::toString(saved)                          //
                + ",appendable:" + DB::toString(appendable)                //
                + ",schema:" + (schema ? schema->dumpStructure() : "none") //
                + ",cache_block:" + (cache ? cache->block.dumpStructure() : "none") + ")";
            return s;
        }
    };
    using PackPtr      = std::shared_ptr<Pack>;
    using ConstPackPtr = std::shared_ptr<const Pack>;
    using Packs        = std::vector<PackPtr>;

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

    bool hasAbandoned() const { return abandoned.load(std::memory_order_relaxed); }

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

    friend struct DeltaSnapshot;
};

using Pack         = DeltaValueSpace::Pack;
using PackPtr      = DeltaValueSpace::PackPtr;
using ConstPackPtr = DeltaValueSpace::ConstPackPtr;
using Packs        = DeltaValueSpace::Packs;

} // namespace DM
} // namespace DB
