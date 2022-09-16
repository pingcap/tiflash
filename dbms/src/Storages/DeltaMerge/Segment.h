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

#include <Common/nocopyable.h>
#include <Core/Block.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/DeltaMerge/Delta/DeltaValueSpace.h>
#include <Storages/DeltaMerge/DeltaIndex.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>
#include <Storages/DeltaMerge/StableValueSpace.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/WriteBatch.h>

namespace DB::DM
{
class Segment;
struct SegmentSnapshot;
using SegmentSnapshotPtr = std::shared_ptr<SegmentSnapshot>;
class StableValueSpace;
using StableValueSpacePtr = std::shared_ptr<StableValueSpace>;
class DeltaValueSpace;
using DeltaValueSpacePtr = std::shared_ptr<DeltaValueSpace>;
class RSOperator;
using RSOperatorPtr = std::shared_ptr<RSOperator>;

using SegmentPtr = std::shared_ptr<Segment>;
using SegmentPair = std::pair<SegmentPtr, SegmentPtr>;
using Segments = std::vector<SegmentPtr>;

/// A structure stores the informations to constantly read a segment instance.
struct SegmentSnapshot : private boost::noncopyable
{
    DeltaSnapshotPtr delta;
    StableSnapshotPtr stable;

    SegmentSnapshot(DeltaSnapshotPtr && delta_, StableSnapshotPtr && stable_)
        : delta(std::move(delta_))
        , stable(std::move(stable_))
    {}

    SegmentSnapshotPtr clone() { return std::make_shared<SegmentSnapshot>(delta->clone(), stable->clone()); }

    UInt64 getBytes() { return delta->getBytes() + stable->getBytes(); }
    UInt64 getRows() { return delta->getRows() + stable->getRows(); }
};

/// A segment contains many rows of a table. A table is split into segments by consecutive ranges.
///
/// The data of stable value space is stored in "data" storage, while data of delta value space is stored in "log" storage.
/// And all meta data is stored in "meta" storage.
class Segment : private boost::noncopyable
{
public:
    using DeltaTree = DefaultDeltaTree;
    using Lock = DeltaValueSpace::Lock;

    struct ReadInfo
    {
    private:
        DeltaValueReaderPtr delta_reader;

    public:
        DeltaIndexIterator index_begin;
        DeltaIndexIterator index_end;

        ColumnDefinesPtr read_columns;

        ReadInfo(
            DeltaValueReaderPtr delta_reader_,
            DeltaIndexIterator index_begin_,
            DeltaIndexIterator index_end_,
            ColumnDefinesPtr read_columns_)
            : delta_reader(delta_reader_)
            , index_begin(index_begin_)
            , index_end(index_end_)
            , read_columns(read_columns_)
        {
        }

        DeltaValueReaderPtr getDeltaReader() const { return delta_reader->createNewReader(read_columns); }
        DeltaValueReaderPtr getDeltaReader(ColumnDefinesPtr columns) const { return delta_reader->createNewReader(columns); }
    };

    struct SplitInfo
    {
        bool is_logical;
        RowKeyValue split_point;

        StableValueSpacePtr my_stable;
        StableValueSpacePtr other_stable;
    };

    DISALLOW_COPY_AND_MOVE(Segment);

    Segment(
        UInt64 epoch_,
        const RowKeyRange & rowkey_range_,
        PageId segment_id_,
        PageId next_segment_id_,
        const DeltaValueSpacePtr & delta_,
        const StableValueSpacePtr & stable_);

    static SegmentPtr newSegment(
        DMContext & context,
        const ColumnDefinesPtr & schema,
        const RowKeyRange & rowkey_range,
        PageId segment_id,
        PageId next_segment_id,
        PageId delta_id,
        PageId stable_id);
    static SegmentPtr newSegment(
        DMContext & context,
        const ColumnDefinesPtr & schema,
        const RowKeyRange & rowkey_range,
        PageId segment_id,
        PageId next_segment_id);

    static SegmentPtr restoreSegment(DMContext & context, PageId segment_id);

    void serialize(WriteBatch & wb);

    /// Attach a new ColumnFile into the Segment. The ColumnFile will be added to MemFileSet and flushed to disk later.
    /// The block data of the passed in ColumnFile should be placed on disk before calling this function.
    /// To write new block data, you can use `writeToCache`.
    bool writeToDisk(DMContext & dm_context, const ColumnFilePtr & column_file);

    /// Write a block of data into the MemTableSet part of the Segment. The data will be flushed to disk later.
    bool writeToCache(DMContext & dm_context, const Block & block, size_t offset, size_t limit);

    /// For test only.
    bool write(DMContext & dm_context, const Block & block, bool flush_cache = true);

    bool write(DMContext & dm_context, const RowKeyRange & delete_range);
    bool ingestColumnFiles(DMContext & dm_context, const RowKeyRange & range, const ColumnFiles & column_files, bool clear_data_in_range);

    SegmentSnapshotPtr createSnapshot(const DMContext & dm_context, bool for_update, CurrentMetrics::Metric metric) const;

    BlockInputStreamPtr getInputStream(
        const DMContext & dm_context,
        const ColumnDefines & columns_to_read,
        const SegmentSnapshotPtr & segment_snap,
        const RowKeyRanges & read_ranges,
        const RSOperatorPtr & filter,
        UInt64 max_version,
        size_t expected_block_size);

    BlockInputStreamPtr getInputStream(
        const DMContext & dm_context,
        const ColumnDefines & columns_to_read,
        const RowKeyRanges & read_ranges,
        const RSOperatorPtr & filter = {},
        UInt64 max_version = std::numeric_limits<UInt64>::max(),
        size_t expected_block_size = DEFAULT_BLOCK_SIZE);

    /// Return a stream which is suitable for exporting data.
    ///  reorganize_block: put those rows with the same pk rows into the same block or not.
    BlockInputStreamPtr getInputStreamForDataExport(
        const DMContext & dm_context,
        const ColumnDefines & columns_to_read,
        const SegmentSnapshotPtr & segment_snap,
        const RowKeyRange & data_range,
        size_t expected_block_size = DEFAULT_BLOCK_SIZE,
        bool reorganize_block = true) const;

    BlockInputStreamPtr getInputStreamRaw(
        const DMContext & dm_context,
        const ColumnDefines & columns_to_read,
        const SegmentSnapshotPtr & segment_snap,
        const RowKeyRanges & data_ranges,
        const RSOperatorPtr & filter,
        bool filter_delete_mark = true,
        size_t expected_block_size = DEFAULT_BLOCK_SIZE);

    BlockInputStreamPtr getInputStreamRaw(
        const DMContext & dm_context,
        const ColumnDefines & columns_to_read,
        bool filter_delete_mark = false);

    /// For those split, merge and mergeDelta methods, we should use prepareXXX/applyXXX combo in real production.
    /// split(), merge() and mergeDelta() are only used in test cases.

    /**
     * Note: There is also DeltaMergeStore::SegmentSplitMode, which shadows this enum.
     */
    enum class SplitMode
    {
        /**
         * Split according to settings.
         *
         * If logical split is allowed in the settings, logical split will be tried first.
         * Logical split may fall back to physical split when calculating split point failed.
         */
        Auto,

        /**
         * Do logical split. If split point is not specified and cannot be calculated out,
         * the split will fail.
         */
        Logical,

        /**
         * Do physical split.
         */
        Physical,
    };

    /**
     * Only used in tests as a shortcut.
     * Normally you should use `prepareSplit` and `applySplit`.
     */
    [[nodiscard]] SegmentPair split(DMContext & dm_context, const ColumnDefinesPtr & schema_snap, std::optional<RowKeyValue> opt_split_at = std::nullopt, SplitMode opt_split_mode = SplitMode::Auto) const;

    std::optional<SplitInfo> prepareSplit(
        DMContext & dm_context,
        const ColumnDefinesPtr & schema_snap,
        const SegmentSnapshotPtr & segment_snap,
        std::optional<RowKeyValue> opt_split_at,
        SplitMode split_mode,
        WriteBatches & wbs) const;

    std::optional<SplitInfo> prepareSplit(
        DMContext & dm_context,
        const ColumnDefinesPtr & schema_snap,
        const SegmentSnapshotPtr & segment_snap,
        WriteBatches & wbs) const
    {
        return prepareSplit(dm_context, schema_snap, segment_snap, std::nullopt, SplitMode::Auto, wbs);
    }

    /**
     * Should be protected behind the Segment update lock.
     */
    [[nodiscard]] SegmentPair applySplit(
        const Lock &,
        DMContext & dm_context,
        const SegmentSnapshotPtr & segment_snap,
        WriteBatches & wbs,
        SplitInfo & split_info) const;

    /// Merge delta & stable, and then take the middle one.
    std::optional<RowKeyValue> getSplitPointSlow(
        DMContext & dm_context,
        const ReadInfo & read_info,
        const SegmentSnapshotPtr & segment_snap) const;
    /// Only look up in the stable vs.
    std::optional<RowKeyValue> getSplitPointFast(
        DMContext & dm_context,
        const StableSnapshotPtr & stable_snap) const;

    enum class PrepareSplitLogicalStatus
    {
        Success,
        FailCalculateSplitPoint,
        FailOther,
    };

    std::pair<std::optional<SplitInfo>, PrepareSplitLogicalStatus> prepareSplitLogical(
        DMContext & dm_context,
        const ColumnDefinesPtr & schema_snap,
        const SegmentSnapshotPtr & segment_snap,
        std::optional<RowKeyValue> opt_split_point,
        WriteBatches & wbs) const;
    std::optional<SplitInfo> prepareSplitPhysical(
        DMContext & dm_context,
        const ColumnDefinesPtr & schema_snap,
        const SegmentSnapshotPtr & segment_snap,
        std::optional<RowKeyValue> opt_split_point,
        WriteBatches & wbs) const;

    /**
     * Only used in tests as a shortcut.
     * Normally you should use `prepareMerge` and `applyMerge`.
     */
    [[nodiscard]] static SegmentPtr merge(
        DMContext & dm_context,
        const ColumnDefinesPtr & schema_snap,
        const std::vector<SegmentPtr> & ordered_segments);

    static StableValueSpacePtr prepareMerge(
        DMContext & dm_context,
        const ColumnDefinesPtr & schema_snap,
        const std::vector<SegmentPtr> & ordered_segments,
        const std::vector<SegmentSnapshotPtr> & ordered_snapshots,
        WriteBatches & wbs);

    /**
     * Should be protected behind the update lock for all related segments.
     */
    [[nodiscard]] static SegmentPtr applyMerge(
        const std::vector<Lock> &,
        DMContext & dm_context,
        const std::vector<SegmentPtr> & ordered_segments,
        const std::vector<SegmentSnapshotPtr> & ordered_snapshots,
        WriteBatches & wbs,
        const StableValueSpacePtr & merged_stable);

    /**
     * Only used in tests as a shortcut.
     * Normally you should use `prepareMergeDelta` and `applyMergeDelta`.
     */
    [[nodiscard]] SegmentPtr mergeDelta(DMContext & dm_context, const ColumnDefinesPtr & schema_snap) const;

    StableValueSpacePtr prepareMergeDelta(
        DMContext & dm_context,
        const ColumnDefinesPtr & schema_snap,
        const SegmentSnapshotPtr & segment_snap,
        WriteBatches & wbs) const;

    /**
     * Should be protected behind the Segment update lock.
     */
    [[nodiscard]] SegmentPtr applyMergeDelta(
        const Lock &,
        DMContext & dm_context,
        const SegmentSnapshotPtr & segment_snap,
        WriteBatches & wbs,
        const StableValueSpacePtr & new_stable) const;

    [[nodiscard]] SegmentPtr dropNextSegment(WriteBatches & wbs, const RowKeyRange & next_segment_range);

    /// Flush delta's cache packs.
    bool flushCache(DMContext & dm_context);
    void placeDeltaIndex(DMContext & dm_context);

    /// Compact the delta layer, merging fragment column files into bigger column files.
    /// It does not merge the delta into stable layer.
    bool compactDelta(DMContext & dm_context);

    size_t getEstimatedRows() const { return delta->getRows() + stable->getRows(); }
    size_t getEstimatedBytes() const { return delta->getBytes() + stable->getBytes(); }

    PageId segmentId() const { return segment_id; }
    PageId nextSegmentId() const { return next_segment_id; }

    void check(DMContext & dm_context, const String & when) const;

    const RowKeyRange & getRowKeyRange() const { return rowkey_range; }

    const DeltaValueSpacePtr & getDelta() const { return delta; }
    const StableValueSpacePtr & getStable() const { return stable; }

    String logId() const;
    String simpleInfo() const;
    String info() const;

    static String simpleInfo(const std::vector<SegmentPtr> & segments);
    static String info(const std::vector<SegmentPtr> & segments);

    bool getUpdateLock(Lock & lock) const { return delta->getLock(lock); }

    Lock mustGetUpdateLock() const
    {
        Lock lock;
        if (!getUpdateLock(lock))
            throw Exception(fmt::format("Segment get update lock failed, segment={}", simpleInfo()), ErrorCodes::LOGICAL_ERROR);
        return lock;
    }

    /// Marks this segment as abandoned.
    /// Note: Segment member functions never abandon the segment itself.
    /// The abandon state is usually triggered by the DeltaMergeStore.
    void abandon(DMContext & context)
    {
        LOG_FMT_DEBUG(log, "Abandon segment, segment={}", simpleInfo());
        delta->abandon(context);
    }

    /// Returns whether this segment has been marked as abandoned.
    /// Note: Segment member functions never abandon the segment itself.
    /// The abandon state is usually triggered by the DeltaMergeStore.
    bool hasAbandoned() const { return delta->hasAbandoned(); }

    bool isSplitForbidden() const { return split_forbidden; }
    void forbidSplit() { split_forbidden = true; }

    bool isValidDataRatioChecked() const { return check_valid_data_ratio.load(std::memory_order_relaxed); }
    void setValidDataRatioChecked() { check_valid_data_ratio.store(true, std::memory_order_relaxed); }

    void drop(const FileProviderPtr & file_provider, WriteBatches & wbs);

    bool isFlushing() const { return delta->isFlushing(); }

    RowsAndBytes getRowsAndBytesInRange(
        DMContext & dm_context,
        const SegmentSnapshotPtr & segment_snap,
        const RowKeyRange & check_range,
        bool is_exact);

    DB::Timestamp getLastCheckGCSafePoint() { return last_check_gc_safe_point.load(std::memory_order_relaxed); }

    void setLastCheckGCSafePoint(DB::Timestamp gc_safe_point) { last_check_gc_safe_point.store(gc_safe_point, std::memory_order_relaxed); }

private:
    ReadInfo getReadInfo(
        const DMContext & dm_context,
        const ColumnDefines & read_columns,
        const SegmentSnapshotPtr & segment_snap,
        const RowKeyRanges & read_ranges,
        UInt64 max_version = std::numeric_limits<UInt64>::max()) const;

    static ColumnDefinesPtr arrangeReadColumns(
        const ColumnDefine & handle,
        const ColumnDefines & columns_to_read);

    /// Create a stream which merged delta and stable streams together.
    template <bool skippable_place = false, class IndexIterator = DeltaIndexIterator>
    static SkippableBlockInputStreamPtr getPlacedStream(
        const DMContext & dm_context,
        const ColumnDefines & read_columns,
        const RowKeyRanges & rowkey_ranges,
        const RSOperatorPtr & filter,
        const StableSnapshotPtr & stable_snap,
        const DeltaValueReaderPtr & delta_reader,
        const IndexIterator & delta_index_begin,
        const IndexIterator & delta_index_end,
        size_t expected_block_size,
        UInt64 max_version = std::numeric_limits<UInt64>::max());

    /// Make sure that all delta packs have been placed.
    /// Note that the index returned could be partial index, and cannot be updated to shared index.
    /// Returns <placed index, this index is fully indexed or not>
    std::pair<DeltaIndexPtr, bool> ensurePlace(
        const DMContext & dm_context,
        const StableSnapshotPtr & stable_snap,
        const DeltaValueReaderPtr & delta_reader,
        const RowKeyRanges & read_ranges,
        UInt64 max_version) const;

    /// Reference the inserts/updates by delta tree.
    /// Returns fully placed or not. Some rows not match relevant_range are not placed.
    template <bool skippable_place>
    bool placeUpsert(
        const DMContext & dm_context,
        const StableSnapshotPtr & stable_snap,
        const DeltaValueReaderPtr & delta_reader,
        size_t delta_value_space_offset,
        Block && block,
        DeltaTree & delta_tree,
        const RowKeyRange & relevant_range,
        bool relevant_place) const;
    /// Reference the deletes by delta tree.
    /// Returns fully placed or not. Some rows not match relevant_range are not placed.
    template <bool skippable_place>
    bool placeDelete(
        const DMContext & dm_context,
        const StableSnapshotPtr & stable_snap,
        const DeltaValueReaderPtr & delta_reader,
        const RowKeyRange & delete_range,
        DeltaTree & delta_tree,
        const RowKeyRange & relevant_range,
        bool relevant_place) const;

private:
    /// The version of this segment. After split / merge / merge delta, epoch got increased by 1.
    const UInt64 epoch;

    RowKeyRange rowkey_range;
    bool is_common_handle;
    size_t rowkey_column_size;
    const PageId segment_id;
    const PageId next_segment_id;

    std::atomic<DB::Timestamp> last_check_gc_safe_point = 0;

    const DeltaValueSpacePtr delta;
    const StableValueSpacePtr stable;

    bool split_forbidden = false;
    // After logical split, it is very possible that only half of the data in the segment's DTFile is valid for this segment.
    // So we want to do merge delta on this kind of segment to clean out the invalid data.
    // This involves to check the valid data ratio in the background gc thread,
    // and to avoid doing this check repeatedly, we add this flag to indicate whether the valid data ratio has already been checked.
    std::atomic<bool> check_valid_data_ratio = false;

    LoggerPtr log;
};

} // namespace DB::DM
