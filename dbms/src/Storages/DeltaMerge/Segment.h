#pragma once

#include <Core/Block.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/DeltaMerge/Delta/Snapshot.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/DeltaValueSpace.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Index/MinMax.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>
#include <Storages/Page/PageDefines.h>

namespace DB
{
class WriteBatch;
namespace DM
{

class Segment;
struct SegmentSnapshot;
using SegmentSnapshotPtr = std::shared_ptr<SegmentSnapshot>;
class StableValueSpace;
using StableValueSpacePtr = std::shared_ptr<StableValueSpace>;
class DeltaValueSpace;
using DeltaValueSpacePtr = std::shared_ptr<DeltaValueSpace>;

using SegmentPtr  = std::shared_ptr<Segment>;
using SegmentPair = std::pair<SegmentPtr, SegmentPtr>;
using Segments    = std::vector<SegmentPtr>;

/// A structure stores the informations to constantly read a segment instance.
struct SegmentSnapshot : private boost::noncopyable
{
    DeltaSnapshotPtr    delta;
    StableValueSpacePtr stable;

    SegmentSnapshot(const DeltaSnapshotPtr & delta_, const StableValueSpacePtr & stable_) : delta(delta_), stable(stable_) {}
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

    struct ReadInfo
    {
        DeltaIndexPtr        index;
        DeltaIndex::Iterator index_begin;
        DeltaIndex::Iterator index_end;

        ColumnDefines read_columns;

        explicit operator bool() const { return (bool)index; }
    };

    struct SplitInfo
    {
        bool   is_logical;
        Handle split_point;

        StableValueSpacePtr my_stable;
        StableValueSpacePtr other_stable;
    };

    Segment(const Segment &) = delete;
    Segment & operator=(const Segment &) = delete;
    Segment & operator=(Segment &&) = delete;

    Segment(UInt64                      epoch_, //
            const HandleRange &         range_,
            PageId                      segment_id_,
            PageId                      next_segment_id_,
            const DeltaValueSpacePtr &  delta_,
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

    bool writeToDisk(DMContext & dm_context, const PackPtr & pack);
    bool writeToCache(DMContext & dm_context, const Block & block, size_t offset, size_t limit);
    bool write(DMContext & dm_context, const Block & block); // For test only
    bool write(DMContext & dm_context, const HandleRange & delete_range);

    SegmentSnapshotPtr createSnapshot(const DMContext & dm_context, bool is_update = false) const;

    BlockInputStreamPtr getInputStream(const DMContext &          dm_context,
                                       const ColumnDefines &      columns_to_read,
                                       const SegmentSnapshotPtr & segment_snap,
                                       const HandleRanges &       read_ranges,
                                       const RSOperatorPtr &      filter,
                                       UInt64                     max_version,
                                       size_t                     expected_block_size);

    BlockInputStreamPtr getInputStream(const DMContext &     dm_context,
                                       const ColumnDefines & columns_to_read,
                                       const HandleRanges &  read_ranges         = {HandleRange::newAll()},
                                       const RSOperatorPtr & filter              = {},
                                       UInt64                max_version         = MAX_UINT64,
                                       size_t                expected_block_size = DEFAULT_BLOCK_SIZE);

    BlockInputStreamPtr getInputStreamRaw(const DMContext &          dm_context,
                                          const ColumnDefines &      columns_to_read,
                                          const SegmentSnapshotPtr & segment_snap,
                                          bool                       do_range_filter);

    BlockInputStreamPtr getInputStreamRaw(const DMContext & dm_context, const ColumnDefines & columns_to_read);

    /// For those split, merge and mergeDelta methods, we should use prepareXXX/applyXXX combo in real production.
    /// split(), merge() and mergeDelta() are only used in test cases.

    SegmentPair split(DMContext & dm_context) const;
    SplitInfo   prepareSplit(DMContext & dm_context, const SegmentSnapshotPtr & segment_snap, WriteBatches & wbs) const;
    SegmentPair
    applySplit(DMContext & dm_context, const SegmentSnapshotPtr & segment_snap, WriteBatches & wbs, SplitInfo & split_info) const;

    static SegmentPtr          merge(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right);
    static StableValueSpacePtr prepareMerge(DMContext &                dm_context, //
                                            const SegmentPtr &         left,
                                            const SegmentSnapshotPtr & left_snap,
                                            const SegmentPtr &         right,
                                            const SegmentSnapshotPtr & right_snap,
                                            WriteBatches &             wbs);
    static SegmentPtr          applyMerge(DMContext &                 dm_context, //
                                          const SegmentPtr &          left,
                                          const SegmentSnapshotPtr &  left_snap,
                                          const SegmentPtr &          right,
                                          const SegmentSnapshotPtr &  right_snap,
                                          WriteBatches &              wbs,
                                          const StableValueSpacePtr & merged_stable);

    SegmentPtr          mergeDelta(DMContext & dm_context) const;
    StableValueSpacePtr prepareMergeDelta(DMContext & dm_context, const SegmentSnapshotPtr & segment_snap, WriteBatches & wbs) const;
    SegmentPtr          applyMergeDelta(DMContext &                 dm_context,
                                        const SegmentSnapshotPtr &  segment_snap,
                                        WriteBatches &              wbs,
                                        const StableValueSpacePtr & new_stable) const;

    /// Flush delta's cache packs.
    bool flushCache(DMContext & dm_context);

    size_t getEstimatedRows() const;
    size_t getEstimatedStableRows() const;
    size_t getEstimatedBytes() const;

    PageId segmentId() const { return segment_id; }
    PageId nextSegmentId() const { return next_segment_id; }

    void check(DMContext & dm_context, const String & when) const;

    const HandleRange &         getRange() const { return range; }
    const DeltaValueSpacePtr &  getDelta() const { return delta; }
    const StableValueSpacePtr & getStable() const { return stable; }

    size_t getPlacedDeltaRows() const { return placed_delta_rows; }
    size_t getPlacedDeltaDeletes() const { return placed_delta_deletes; }

    size_t getDeltaRawRows(bool use_unsaved = true) const;
    // Insert and delete operations' count in DeltaTree
    size_t updatesInDeltaTree() const;

    String simpleInfo() const;
    String info() const;

    using Lock = DeltaValueSpace::Lock;
    bool getUpdateLock(Lock & lock) const { return delta->getLock(lock); }

    Lock mustGetUpdateLock() const
    {
        Lock lock;
        if (!getUpdateLock(lock))
            throw Exception("Segment [" + DB::toString(segmentId()) + "] get update lock failed", ErrorCodes::LOGICAL_ERROR);
        return lock;
    }

    void abandon()
    {
        LOG_DEBUG(log, "Abandon segment [" << segment_id << "]");
        delta->abandon();
    }
    bool hasAbandoned() { return delta->hasAbandoned(); }

private:
    template <bool add_tag_column>
    ReadInfo getReadInfo(const DMContext & dm_context, const ColumnDefines & read_columns, const SegmentSnapshotPtr & segment_snap) const;

    template <bool add_tag_column>
    static ColumnDefines arrangeReadColumns(const ColumnDefine & handle, const ColumnDefines & columns_to_read);

    template <class IndexIterator = DeltaIndex::Iterator, bool skippable_place = false>
    SkippableBlockInputStreamPtr getPlacedStream(const DMContext &           dm_context,
                                                 const ColumnDefines &       read_columns,
                                                 const HandleRange &         handle_range,
                                                 const RSOperatorPtr &       filter,
                                                 const StableValueSpacePtr & stable_snap,
                                                 DeltaSnapshotPtr &          delta_snap,
                                                 const IndexIterator &       delta_index_begin,
                                                 const IndexIterator &       delta_index_end,
                                                 size_t                      index_size,
                                                 size_t                      expected_block_size) const;

    /// Merge delta & stable, and then take the middle one.
    Handle getSplitPointSlow(DMContext & dm_context, const ReadInfo & read_info, const SegmentSnapshotPtr & segment_snap) const;
    /// Only look up in the stable vs.
    Handle getSplitPointFast(DMContext & dm_context, const StableValueSpacePtr & stable_snap) const;

    SplitInfo prepareSplitLogical(DMContext &                dm_context, //
                                  const SegmentSnapshotPtr & segment_snap,
                                  Handle                     split_point,
                                  WriteBatches &             wbs) const;
    SplitInfo prepareSplitPhysical(DMContext & dm_context, const SegmentSnapshotPtr & segment_snap, WriteBatches & wbs) const;


    /// Make sure that all delta packs have been placed.
    DeltaIndexPtr ensurePlace(const DMContext & dm_context, const StableValueSpacePtr & stable_snap, DeltaSnapshotPtr & delta_snap) const;

    /// Reference the inserts/updates by delta tree.
    template <bool skippable_place>
    void placeUpsert(const DMContext &           dm_context,
                     const StableValueSpacePtr & stable_snap,
                     DeltaSnapshotPtr &          delta_snap,
                     size_t                      delta_value_space_offset,
                     Block &&                    block,
                     DeltaTree &                 delta_tree) const;
    /// Reference the deletes by delta tree.
    template <bool skippable_place>
    void placeDelete(const DMContext &           dm_context,
                     const StableValueSpacePtr & stable_snap,
                     DeltaSnapshotPtr &          delta_snap,
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

    const DeltaValueSpacePtr  delta;
    const StableValueSpacePtr stable;

    mutable DeltaTreePtr delta_tree;
    mutable size_t       placed_delta_rows    = 0;
    mutable size_t       placed_delta_deletes = 0;

    // Used to synchronize between read threads.
    // Mainly to protect delta_tree's update between them.
    mutable std::mutex read_read_mutex;

    Logger * log;
};

} // namespace DM
} // namespace DB
