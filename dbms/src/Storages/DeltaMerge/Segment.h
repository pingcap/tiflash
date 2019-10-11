#pragma once

#include <Core/Block.h>

#include <Storages/DeltaMerge/Chunk.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/DiskValueSpace.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Index/MinMax.h>
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

struct RemoveWriteBatches
{
    WriteBatch meta;
    WriteBatch data;
    WriteBatch log;

    void write(StoragePool & storage)
    {
        storage.meta().write(meta);
        storage.data().write(data);
        storage.log().write(log);
    }
};

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

    void insertValue(IColumn & des, size_t column_index, UInt64 value_id) //
    {
        des.insertFrom(*(columns_ptr[column_index]), value_id);
    }

    Handle getHandle(size_t value_id) //
    {
        return (*handle_column)[value_id];
    }

    Columns                        columns;
    ColumnRawPtrs                  columns_ptr;
    PaddedPODArray<Handle> const * handle_column;
};
using DeltaValueSpacePtr = std::shared_ptr<DeltaValueSpace>;

/// A structure stores the informations to constantly read a segment instance.
struct SegmentSnapshot
{
    DiskValueSpacePtr delta;
    size_t            delta_rows    = 0;
    size_t            delta_deletes = 0;

    SegmentSnapshot() = default;
    SegmentSnapshot(const DiskValueSpacePtr & delta_, size_t delta_rows_, size_t delta_deletes_)
        : delta{delta_}, delta_rows(delta_rows_), delta_deletes(delta_deletes_)
    {
    }

    explicit operator bool() { return (bool)delta; }
};

/// A segment contains many rows of a table. A table is split into segments by succeeding ranges.
///
/// The data of stable value space is stored in "data" storage, while data of delta value space is stored in "log" storage.
/// And all meta data is stored in "meta" storage.
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

        explicit operator bool() const { return (bool)delta_value_space; }
    };

    Segment(const Segment &) = delete;
    Segment & operator=(const Segment &) = delete;
    Segment & operator=(Segment &&) = delete;

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

    static SegmentPtr newSegment(DMContext & context, const HandleRange & range_, PageId segment_id_, PageId next_segment_id_);
    static SegmentPtr restoreSegment(DMContext & context, PageId segment_id);

    void serialize(WriteBatch & wb);

    /// Write an update. This function only guarantees atomic on segment level.
    void write(DMContext & dm_context, const BlockOrDelete & update);

    /// Use #createAppendTask and #applyAppendTask to build higher atomic level.
    AppendTaskPtr createAppendTask(const OpContext & context, AppendWriteBatches & wbs, const BlockOrDelete & update);
    void          applyAppendTask(const OpContext & context, const AppendTaskPtr & task, const BlockOrDelete & update);

    SegmentSnapshot     getReadSnapshot() const;
    BlockInputStreamPtr getInputStream(const DMContext &       dm_context,
                                       const SegmentSnapshot & segment_snap,
                                       const StorageSnapshot & storage_snaps,
                                       const ColumnDefines &   columns_to_read,
                                       const HandleRanges &    read_ranges,
                                       const RSOperatorPtr &   filter,
                                       UInt64                  max_version,
                                       size_t                  expected_block_size);
    BlockInputStreamPtr getInputStreamRaw(const DMContext &       dm_context,
                                          const SegmentSnapshot & segment_snap,
                                          const StorageSnapshot & storage_snaps,
                                          const ColumnDefines &   columns_to_read);

    SegmentPair       split(DMContext & dm_context, RemoveWriteBatches & remove_wbs) const;
    static SegmentPtr merge(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right, RemoveWriteBatches & remove_wbs);

    bool shouldFlushDelta(DMContext & dm_context) const;
    /// Flush delta into stable. i.e. delta merge.
    SegmentPtr flushDelta(DMContext & dm_context, RemoveWriteBatches & remove_wbs) const;

    /// Flush delta's cached chunks.
    void flushCache(DMContext & dm_context, WriteBatch & remove_log_wb);

    size_t getEstimatedRows() const;

    size_t getEstimatedBytes() const;

    PageId segmentId() const { return segment_id; }
    PageId nextSegmentId() const { return next_segment_id; }

    void check(DMContext & dm_context, const String & when) const;

    String simpleInfo() const { return "{" + DB::toString(segment_id) + ":" + range.toString() + "}"; }

    String info() const
    {
        return "{id:" + DB::toString(segment_id) + ", next: " + DB::toString(next_segment_id) + ", epoch: " + DB::toString(epoch)
            + ", range: " + range.toString() + "}";
    }

    const HandleRange & getRange() const { return range; }

    size_t delta_rows() const;
    size_t delta_deletes() const;

private:
    template <bool add_tag_column>
    ReadInfo getReadInfo(const DMContext &       dm_context,
                         const SegmentSnapshot & segment_snap,
                         const StorageSnapshot & storage_snaps,
                         const ColumnDefines &   columns_to_read) const;

    template <bool add_tag_column>
    static ColumnDefines arrangeReadColumns(const ColumnDefine & handle, const ColumnDefines & columns_to_read);

    template <class IndexIterator = DeltaIndex::Iterator>
    BlockInputStreamPtr getPlacedStream(const PageReader &         data_page_reader,
                                        const ColumnDefines &      read_columns,
                                        const RSOperatorPtr &      filter,
                                        const DeltaValueSpacePtr & delta_value_space,
                                        const IndexIterator &      delta_index_begin,
                                        const IndexIterator &      delta_index_end,
                                        size_t                     expected_block_size) const;

    /// Merge delta & stable, and then take the middle one.
    Handle getSplitPointSlow(DMContext & dm_context, const PageReader & data_page_reader, const ReadInfo & read_info) const;
    /// Only look up in the stable vs.
    Handle getSplitPointFast(DMContext & dm_context, const PageReader & data_page_reader) const;

    /// Split this segment into two.
    /// Generates two new segment objects, the current object is not modified.
    SegmentPair doSplit(DMContext &          dm_context,
                        const PageReader &   data_page_reader,
                        const ReadInfo &     read_info,
                        Handle               split_point,
                        RemoveWriteBatches & remove_wbs) const;

    SegmentPair doRefSplit(DMContext & dm_context, Handle split_point, RemoveWriteBatches & remove_wbs) const;

    /// Merge this segment and the other into one.
    /// Generates a new segment object, the current object is not modified.
    static SegmentPtr doMerge(DMContext &          dm_context,
                              const PageReader &   data_page_reader,
                              const SegmentPtr &   left,
                              const ReadInfo &     left_snapshot,
                              const SegmentPtr &   right,
                              const ReadInfo &     right_snapshot,
                              RemoveWriteBatches & remove_wbs);

    static SegmentPtr doMergeFast(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right, RemoveWriteBatches & remove_wbs);

    /// Reset the content of this segment.
    /// Generates a new segment object, the current object is not modified.
    SegmentPtr reset(DMContext & dm_context, BlockInputStreamPtr & input_stream, RemoveWriteBatches & remove_wbs) const;

    /// Make sure that all delta chunks have been placed.
    DeltaIndexPtr ensurePlace(const DMContext &          dm_context,
                              const StorageSnapshot &    storage_snapshot,
                              const DiskValueSpacePtr &  to_place_delta,
                              size_t                     delta_rows_limit,
                              size_t                     delta_deletes_limit,
                              const DeltaValueSpacePtr & delta_value_space) const;

    /// Reference the inserts/updates by delta tree.
    void placeUpsert(const DMContext &          dm_context,
                     const PageReader &         data_page_reader,
                     const DeltaValueSpacePtr & delta_value_space,
                     Block &&                   block) const;
    /// Reference the deletes by delta tree.
    void placeDelete(const DMContext &          dm_context,
                     const PageReader &         data_page_reader,
                     const DeltaValueSpacePtr & delta_value_space,
                     const HandleRange &        delete_range) const;

    MinMaxIndexPtr getMinMax(const ColumnDefine & column_define) const;

    size_t estimatedRows() const;
    size_t estimatedBytes() const;

private:
    const UInt64      epoch; // After split/merge, epoch got increase by 1.
    const HandleRange range;
    const PageId      segment_id;
    const PageId      next_segment_id;

    DiskValueSpacePtr delta;
    DiskValueSpacePtr stable;

    mutable DeltaTreePtr delta_tree;
    mutable size_t       placed_delta_rows    = 0;
    mutable size_t       placed_delta_deletes = 0;

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