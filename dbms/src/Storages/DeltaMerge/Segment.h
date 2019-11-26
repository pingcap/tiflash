#pragma once

#include <Core/Block.h>

#include <Interpreters/ExpressionActions.h>
#include <Storages/DeltaMerge/Chunk.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/DiskValueSpace.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Index/MinMax.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/StableValueSpace.h>
#include <Storages/DeltaMerge/StoragePool.h>

namespace DB
{
namespace DM
{

class Segment;
struct SegmentSnapshot;

using SegmentPtr  = std::shared_ptr<Segment>;
using SegmentPair = std::pair<SegmentPtr, SegmentPtr>;
using Segments    = std::vector<SegmentPtr>;

using SegmentAndStorageSnap = std::pair<SegmentSnapshot, StorageSnapshot>;
using SegmentSnapAndChunks  = std::pair<SegmentSnapshot, Chunks>;

struct DeltaValueSpace
{
    DeltaValueSpace(const ColumnDefine & handle_define, const ColumnDefines & column_defines, const Block & block)
    {
        columns.reserve(column_defines.size());
        columns_ptr.reserve(column_defines.size());
        for (const auto & c : column_defines)
        {

            auto & col = block.getByName(c.name).column;
            columns.emplace_back(col);
            columns_ptr.emplace_back(col.get());

            if (c.name == handle_define.name)
            {
                handle_column = toColumnVectorDataPtr<Handle>(col);
            }
        }
        rows = block.rows();
    }

    void insertValue(IColumn & des, size_t column_index, UInt64 value_id) //
    {
        if ((unlikely(value_id >= rows)))
            throw Exception("value_id is expected to < " + DB::toString(rows) + ", now " + DB::toString(value_id));
        des.insertFrom(*(columns_ptr[column_index]), value_id);
    }

    Handle getHandle(size_t value_id) //
    {
        if ((unlikely(value_id >= rows)))
            throw Exception("value_id is expected to < " + DB::toString(rows) + ", now " + DB::toString(value_id));
        return (*handle_column)[value_id];
    }

    size_t    getRows() { return rows; }
    Columns & getColumns() { return columns; }

    Columns                        columns;
    ColumnRawPtrs                  columns_ptr;
    PaddedPODArray<Handle> const * handle_column;
    size_t                         rows;
};
using DeltaValueSpacePtr = std::shared_ptr<DeltaValueSpace>;

/// A structure stores the informations to constantly read a segment instance.
struct SegmentSnapshot
{
    StableValueSpacePtr stable        = {};
    DiskValueSpacePtr   delta         = {};
    size_t              delta_rows    = 0;
    size_t              delta_deletes = 0;
    DeltaIndexPtr       delta_index   = {};

    // Whether this snapshot contains the cache of delta or not.
    bool delta_has_cache = false;

    SegmentSnapshot() = default;

    explicit operator bool() { return (bool)delta; }
};

/// A segment contains many rows of a table. A table is split into segments by consecutive ranges.
///
/// The data of stable value space is stored in "data" storage, while data of delta value space is stored in "log" storage.
/// And all meta data is stored in "meta" storage.
class Segment : private boost::noncopyable
{
public:
    using Version = UInt32;
    static const Version CURRENT_VERSION;

    using DeltaTree = DefaultDeltaTree;
    using OpContext = DiskValueSpace::OpContext;

    struct ReadInfo
    {
        StorageSnapshot storage_snap;
        SegmentSnapshot segment_snap;

        DeltaValueSpacePtr   delta_value_space;
        DeltaIndexPtr        index;
        DeltaIndex::Iterator index_begin;
        DeltaIndex::Iterator index_end;

        ColumnDefines read_columns;

        explicit operator bool() const { return (bool)delta_value_space; }
    };

    Segment(const Segment &) = delete;
    Segment & operator=(const Segment &) = delete;
    Segment & operator=(Segment &&) = delete;

    Segment(UInt64                      epoch_, //
            const HandleRange &         range_,
            PageId                      segment_id_,
            PageId                      next_segment_id_,
            const DiskValueSpacePtr &   delta_,
            const StableValueSpacePtr & stable_);

    static SegmentPtr newSegment(DMContext &         context, //
                                 const HandleRange & range_,
                                 PageId              segment_id,
                                 PageId              next_segment_id,
                                 PageId              delta_id,
                                 PageId              stable_id);
    static SegmentPtr newSegment(DMContext &         context, //
                                 const HandleRange & range,
                                 PageId              segment_id,
                                 PageId              next_segment_id);

    static SegmentPtr restoreSegment(DMContext & context, PageId segment_id);

    void serialize(WriteBatch & wb);

    /// Write an update. This function only guarantees atomic on segment level.
    void write(DMContext & dm_context, const BlockOrDelete & update);

    /// Use #createAppendTask and #applyAppendTask to build higher atomic level.
    AppendTaskPtr createAppendTask(const OpContext & context, WriteBatches & wbs, const BlockOrDelete & update);
    void          applyAppendTask(const OpContext & context, const AppendTaskPtr & task, const BlockOrDelete & update);

    SegmentSnapshot getReadSnapshot(bool use_delta_cache = true) const;

    BlockInputStreamPtr getInputStream(const DMContext &       dm_context,
                                       const ColumnDefines &   columns_to_read,
                                       const SegmentSnapshot & segment_snap,
                                       const StorageSnapshot & storage_snap,
                                       const HandleRanges &    read_ranges,
                                       const RSOperatorPtr &   filter,
                                       UInt64                  max_version,
                                       size_t                  expected_block_size);

    BlockInputStreamPtr getInputStream(const DMContext &     dm_context,
                                       const ColumnDefines & columns_to_read,
                                       const HandleRanges &  read_ranges         = {HandleRange::newAll()},
                                       const RSOperatorPtr & filter              = {},
                                       UInt64                max_version         = MAX_UINT64,
                                       size_t                expected_block_size = DEFAULT_BLOCK_SIZE);

    BlockInputStreamPtr getInputStreamRaw(const DMContext &       dm_context,
                                          const ColumnDefines &   columns_to_read,
                                          const SegmentSnapshot & segment_snap,
                                          const StorageSnapshot & storage_snap,
                                          bool                    do_range_filter);

    BlockInputStreamPtr getInputStreamRaw(const DMContext & dm_context, const ColumnDefines & columns_to_read);

    SegmentPair split(DMContext &             dm_context, //
                      const SegmentSnapshot & segment_snap,
                      const StorageSnapshot & storage_snap,
                      WriteBatches &          wbs) const;
    SegmentPair split(DMContext & dm_context) const;

    static SegmentPtr merge(DMContext &             dm_context,
                            const SegmentPtr &      left,
                            const SegmentSnapshot & left_snap,
                            const SegmentPtr &      right,
                            const SegmentSnapshot & right_snap,
                            const StorageSnapshot & storage_snap,
                            WriteBatches &          wbs);
    static SegmentPtr merge(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right);

    StableValueSpacePtr prepareMergeDelta(DMContext &             dm_context,
                                          const SegmentSnapshot & segment_snap,
                                          const StorageSnapshot & storage_snap,
                                          WriteBatches &          wbs) const;
    SegmentPtr applyMergeDelta(const SegmentSnapshot & segment_snap, WriteBatches & wbs, const StableValueSpacePtr & new_stable) const;

    /// Note that we should replace this object with return object, or we can not read latest data after `mergeDelta`.
    WARN_UNUSED_RESULT
    SegmentPtr mergeDelta(DMContext &             dm_context,
                          const SegmentSnapshot & segment_snap,
                          const StorageSnapshot & storage_snap,
                          WriteBatches &          wbs) const;
    WARN_UNUSED_RESULT
    SegmentPtr mergeDelta(DMContext & dm_context) const;

    /// Flush delta's cache chunks.
    void flushCache(DMContext & dm_context);

    size_t getEstimatedRows() const;
    size_t getEstimatedStableRows() const;
    size_t getEstimatedBytes() const;

    PageId segmentId() const { return segment_id; }
    PageId nextSegmentId() const { return next_segment_id; }

    void check(DMContext & dm_context, const String & when) const;

    const HandleRange &         getRange() const { return range; }
    const DiskValueSpace &      getDelta() const { return *delta; }
    const StableValueSpacePtr & getStable() const { return stable; }

    size_t getPlacedDeltaRows() const { return placed_delta_rows; }
    size_t getPlacedDeltaDeletes() const { return placed_delta_deletes; }

    size_t getDeltaRawRows(bool with_delta_cache = true) const;
    // Insert and delete operations' count in DeltaTree
    size_t updatesInDeltaTree() const;

    std::atomic_bool & isMergeDelta() { return is_merge_delta; }
    std::atomic_bool & isBackgroundMergeDelta() { return is_background_merge_delta; }

    String simpleInfo() const;
    String info() const;

private:
    template <bool add_tag_column>
    ReadInfo getReadInfo(const DMContext &       dm_context,
                         const ColumnDefines &   read_columns,
                         const SegmentSnapshot & segment_snap,
                         const StorageSnapshot & storage_snap) const;

    template <bool add_tag_column>
    static ColumnDefines arrangeReadColumns(const ColumnDefine & handle, const ColumnDefines & columns_to_read);

    template <class IndexIterator = DeltaIndex::Iterator>
    BlockInputStreamPtr getPlacedStream(const DMContext &           dm_context,
                                        const ColumnDefines &       read_columns,
                                        const HandleRange &         handle_range,
                                        const RSOperatorPtr &       filter,
                                        const StableValueSpacePtr & stable_snap,
                                        const DeltaValueSpacePtr &  delta_value_space,
                                        const IndexIterator &       delta_index_begin,
                                        const IndexIterator &       delta_index_end,
                                        size_t                      index_size,
                                        size_t                      expected_block_size) const;

    /// Merge delta & stable, and then take the middle one.
    Handle getSplitPointSlow(DMContext & dm_context, const ReadInfo & read_info) const;
    /// Only look up in the stable vs.
    Handle getSplitPointFast(DMContext & dm_context, const StableValueSpacePtr & stable_snap) const;

    SegmentPair doSplitLogical(DMContext &             dm_context,
                               const SegmentSnapshot & segment_snap,
                               const StorageSnapshot & storage_snap,
                               Handle                  split_point,
                               WriteBatches &          wbs) const;
    SegmentPair doSplitPhysical(DMContext &             dm_context,
                                const SegmentSnapshot & segment_snap,
                                const StorageSnapshot & storage_snap,
                                WriteBatches &          wbs) const;

    static SegmentPtr doMergeLogical(DMContext &             dm_context, //
                                     const StorageSnapshot & storage_snap,
                                     const SegmentPtr &      left,
                                     const SegmentPtr &      right,
                                     WriteBatches &          wbs);

    void doFlushCache(DMContext & dm_context, WriteBatch & remove_log_wb);

    /// Make sure that all delta chunks have been placed.
    DeltaIndexPtr ensurePlace(const DMContext &           dm_context,
                              const StorageSnapshot &     storage_snapshot,
                              const StableValueSpacePtr & stable_snap,
                              const DiskValueSpace &      to_place_delta,
                              size_t                      delta_rows_limit,
                              size_t                      delta_deletes_limit,
                              const DeltaValueSpacePtr &  delta_value_space) const;

    /// Reference the inserts/updates by delta tree.
    void placeUpsert(const DMContext &           dm_context,
                     const StableValueSpacePtr & stable_snap,
                     const DeltaValueSpacePtr &  delta_value_space,
                     size_t                      delta_value_space_offset,
                     Block &&                    block,
                     DeltaTree &                 delta_tree) const;
    /// Reference the deletes by delta tree.
    void placeDelete(const DMContext &           dm_context,
                     const StableValueSpacePtr & stable_snap,
                     const DeltaValueSpacePtr &  delta_value_space,
                     const HandleRange &         delete_range,
                     DeltaTree &                 delta_tree) const;

    size_t stableRows() const;
    size_t deltaRows() const;
    size_t estimatedRows() const;
    size_t estimatedBytes() const;

private:
    const UInt64      epoch; // After split / merge / merge delta, epoch got increase by 1.
    const HandleRange range;
    const PageId      segment_id;
    const PageId      next_segment_id;

    DiskValueSpacePtr delta;
    //    DMFilePtr         stable;
    StableValueSpacePtr stable;

    std::atomic_bool is_merge_delta            = false;
    std::atomic_bool is_background_merge_delta = false;

    mutable DeltaTreePtr delta_tree;
    mutable size_t       placed_delta_rows    = 0;
    mutable size_t       placed_delta_deletes = 0;

    // Used to synchronize between read threads and write thread.
    // Write thread holds a unique lock, and read threads hold shared lock.
    mutable std::shared_mutex read_write_mutex;
    // Used to synchronize between read threads.
    // Mainly to protect delta_tree's update between them.
    mutable std::mutex read_read_mutex;

    Logger * log;
};

} // namespace DM
} // namespace DB