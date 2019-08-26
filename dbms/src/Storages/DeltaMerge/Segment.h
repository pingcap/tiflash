#pragma once

#include <Core/Block.h>

#include <Storages/DeltaMerge/Chunk.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/DiskValueSpace.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/StoragePool.h>

namespace DB
{
namespace DM
{

class Segment;
using SegmentPtr  = std::shared_ptr<Segment>;
using SegmentPair = std::pair<SegmentPtr, SegmentPtr>;
using Segments    = std::vector<SegmentPtr>;

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
                handle_column = toColumnVectorDataPtr<Handle>(col);
        }
    }

    inline void insertValue(IColumn & des, size_t column_index, UInt64 value_id) //
    {
        des.insertFrom(*(columns_ptr[column_index]), value_id);
    }

    inline Handle getHandle(size_t value_id) //
    {
        return (*handle_column)[value_id];
    }

    inline size_t getRows() { return handle_column->size(); }

    Columns                        columns;
    ColumnRawPtrs                  columns_ptr;
    PaddedPODArray<Handle> const * handle_column;
};
using DeltaValueSpacePtr = std::shared_ptr<DeltaValueSpace>;

/// A structure stores the informations to constantly read a segment instance.
struct SegmentSnapshot
{
    DiskValueSpacePtr delta;
    size_t            delta_rows;

    SegmentSnapshot(const DiskValueSpacePtr & delta_, size_t delta_rows_) : delta{delta_}, delta_rows(delta_rows_) {}
};

/// A segment contains many rows of a table. A table is split into segments by succeeding ranges.
///
/// The data of stable value space is stored in "data" storage, while data of delta value space is stored in "log" storage.
/// And all meta data is stored in "meta" storage.
///
/// TODO: Currently we don't support DDL, e.g. update column type. Will add it later.
class Segment : private boost::noncopyable
{
public:
    using Version = UInt32;
    static const Version CURRENT_VERSION;

    using OpContext = DiskValueSpace::OpContext;

    struct ReadInfo
    {
        DeltaValueSpacePtr   delta_value_space;
        DeltaIndex::Iterator index_begin;
        DeltaIndex::Iterator index_end;

        ColumnDefines read_columns;
    };

    static SegmentPtr newSegment(DMContext & context, const HandleRange & range_, PageId segment_id_, PageId next_segment_id_);
    static SegmentPtr restoreSegment(DMContext & context, PageId segment_id);

    void serialize(WriteBatch & wb);

    /// Write an update. This function only guarantees atomic on segment level.
    void write(DMContext & dm_context, const BlockOrDelete & update);

    /// Use #createAppendTask and #applyAppendTask to build higher atomic level.
    AppendTaskPtr createAppendTask(const OpContext & context, AppendWriteBatches & wbs, const BlockOrDelete & update);
    void          applyAppendTask(const OpContext & context, const AppendTaskPtr & task, const BlockOrDelete & update);

    SegmentSnapshot     getReadSnapshot();
    BlockInputStreamPtr getInputStream(const DMContext &       dm_context,
                                       const SegmentSnapshot & segment_snap,
                                       const StorageSnapshot & storage_snaps,
                                       const ColumnDefines &   columns_to_read,
                                       const HandleRanges &    read_ranges,
                                       UInt64                  max_version,
                                       size_t                  expected_block_size);
    BlockInputStreamPtr getInputStreamRaw(const DMContext &       dm_context,
                                          const SegmentSnapshot & segment_snap,
                                          const StorageSnapshot & storage_snaps,
                                          const ColumnDefines &   columns_to_read);

    SegmentPair       split(DMContext & dm_context);
    static SegmentPtr merge(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right);

    bool shouldFlush(DMContext & dm_context) const;
    /// Flush delta into stable. i.e. delta merge.
    SegmentPtr flush(DMContext & dm_context);

    size_t getEstimatedRows();

    size_t getEstimatedBytes();

    PageId segmentId() { return segment_id; }
    PageId nextSegmentId() { return next_segment_id; }

    Segment(UInt64              epoch_, //
            const HandleRange & range_,
            PageId              segment_id_,
            PageId              next_segment_id_,
            PageId              delta_id,
            PageId              stable_id);

    Segment(UInt64              epoch_, //
            const HandleRange & range_,
            PageId              segment_id_,
            PageId              next_segment_id_,
            PageId              delta_id,
            const Chunks &      delta_chunks_,
            PageId              stable_id,
            const Chunks &      stable_chunks_);

    void check(DMContext & dm_context, const String & when);

    String simpleInfo() const { return "{" + DB::toString(segment_id) + ":" + range.toString() + "}"; }

    String info() const
    {
        return "{id:" + DB::toString(segment_id) + ", next: " + DB::toString(next_segment_id) + ", epoch: " + DB::toString(epoch)
            + ", range: " + range.toString() + "}";
    }

    const HandleRange & getRange() const { return range; }

    size_t delta_rows();
    size_t delta_deletes();

private:
    template <bool add_tag_column>
    ReadInfo getReadInfo(const DMContext &       dm_context,
                         const SegmentSnapshot & segment_snap,
                         const StorageSnapshot & storage_snaps,
                         const ColumnDefines &   columns_to_read);

    template <bool add_tag_column>
    static ColumnDefines arrangeReadColumns(const ColumnDefine & handle, const ColumnDefines & columns_to_read);

    template <class IndexIterator = DeltaIndex::Iterator>
    BlockInputStreamPtr getPlacedStream(const PageReader &         data_page_reader,
                                        const HandleRanges &       read_ranges,
                                        const ColumnDefines &      read_columns,
                                        const DeltaValueSpacePtr & delta_value_space,
                                        const IndexIterator &      delta_index_begin,
                                        const IndexIterator &      delta_index_end,
                                        size_t                     expected_block_size) const;

    /// Split this segment into two.
    /// Generates two new segment objects, the current object is not modified.
    SegmentPair doSplit(DMContext & dm_context, const PageReader & data_page_reader, const ReadInfo & read_info, Handle split_point) const;
    /// Merge this segment and the other into one.
    /// Generates a new segment object, the current object is not modified.
    static SegmentPtr doMerge(DMContext &        dm_context,
                              const PageReader & data_page_reader,
                              const SegmentPtr & left,
                              const ReadInfo &   left_snapshot,
                              const SegmentPtr & right,
                              const ReadInfo &   right_snapshot);
    /// Reset the content of this segment.
    /// Generates a new segment object, the current object is not modified.
    SegmentPtr reset(DMContext & dm_context, BlockInputStreamPtr & input_stream) const;

    /// Make sure that all delta chunks have been placed.
    DeltaIndexPtr ensurePlace(const DMContext &          dm_context,
                              const StorageSnapshot &    storage_snapshot,
                              const DiskValueSpacePtr &  to_place_delta,
                              const DeltaValueSpacePtr & delta_value_space);
    /// Reference the inserts/updates by delta tree.
    void placeUpsert(const DMContext &          dm_context,
                     const PageReader &         data_page_reader,
                     const DeltaValueSpacePtr & delta_value_space,
                     Block &&                   block);
    /// Reference the deletes by delta tree.
    void placeDelete(const DMContext &          dm_context,
                     const PageReader &         data_page_reader,
                     const DeltaValueSpacePtr & delta_value_space,
                     const HandleRange &        delete_range);

    Handle getSplitPoint(DMContext & dm_context, const PageReader & data_page_reader, const ReadInfo & read_info);

    size_t estimatedRows();
    size_t estimatedBytes();

private:
    const UInt64      epoch; // After split/merge, epoch got increase by 1.
    const HandleRange range;
    const PageId      segment_id;
    const PageId      next_segment_id;

    DiskValueSpacePtr delta;
    DiskValueSpacePtr stable;

    DeltaTreePtr delta_tree;
    size_t       placed_delta_rows    = 0;
    size_t       placed_delta_deletes = 0;

    // Used to synchronize between read threads and write thread.
    // Write thread holds a unique lock, and read thread holds shared lock.
    mutable std::shared_mutex read_write_mutex;
    // Used to synchronize between read threads.
    // Mainly to protect delta_tree updates between read threads.
    mutable std::mutex read_read_mutex;

    Logger * log;
};

} // namespace DM
} // namespace DB