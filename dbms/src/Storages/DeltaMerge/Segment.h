#pragma once

#include <Core/Block.h>

#include <Storages/DeltaMerge/Chunk.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/DiskValueSpace.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/StoragePool.h>

namespace DB
{

class Segment;
using SegmentPtr = std::shared_ptr<Segment>;
using Segments   = std::vector<SegmentPtr>;

/// A segment contains many rows of a table. A table is split into segments by succeeding ranges.
///
/// The data of stable value space is stored in "data" storage, while data of delta value space is stored in "log" storage.
/// And all meta data is stored in "meta" storage.
///
/// TODO: Currently we don't support DDL, e.g. update column type. Will add it later.
class Segment : private boost::noncopyable
{
public:
    using SharedLock = std::shared_lock<std::shared_mutex>;
    using Version    = UInt32;
    static const Version CURRENT_VERSION;

    static SegmentPtr newSegment(DMContext & context, const HandleRange & range_, PageId segment_id_, PageId next_segment_id_);
    static SegmentPtr restoreSegment(DMContext & context, PageId segment_id);

    void serialize(WriteBatch & wb);

    const HandleRange & getRange() { return range; }

    void write(DMContext & dm_context, Block && block);

    void deleteRange(DMContext & dm_context, const HandleRange & delete_range);

    BlockInputStreamPtr getInputStream(const DMContext &     dm_context, //
                                       const ColumnDefines & columns_to_read,
                                       size_t                expected_block_size,
                                       UInt64                max_version,
                                       bool                  is_raw);

    SegmentPtr split(DMContext & dm_context);

    void merge(DMContext & dm_context, const SegmentPtr & other);

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

    void swap(Segment & other);

private:
    BlockInputStreamPtr getPlacedStream(const ColumnDefine &  handle,
                                        const ColumnDefines & columns_to_read,
                                        StoragePool &         storage_pool,
                                        size_t                expected_block_size,
                                        SharedLock &&         lock);

    SegmentPtr doSplit(DMContext & dm_context, Handle split_point);
    void       doMerge(DMContext & dm_context, const SegmentPtr & other);

    void reset(DMContext & dm_context, BlockInputStreamPtr & input_stream);

    bool tryFlush(DMContext & dm_context, bool force = false);
    /// Flush delta into stable.
    void flush(DMContext & dm_context);
    /// Make sure that all delta chunks have been placed.
    void ensurePlace(const ColumnDefine & handle, StoragePool & storage);
    /// Reference the inserts/updates by delta tree.
    void placeUpsert(const ColumnDefine & handle, StoragePool & storage, Block && block);
    /// Reference the deletes by delta tree.
    void placeDelete(const ColumnDefine & handle, StoragePool & storage, const HandleRange & delete_range);

    Handle getSplitPoint(DMContext & dm_context);

    size_t estimatedRows();
    size_t estimatedBytes();

private:
    UInt64      epoch; // After split/merge, epoch got increase by 1.
    HandleRange range;
    PageId      segment_id;
    PageId      next_segment_id;

    DiskValueSpace delta;
    DiskValueSpace stable;

    DeltaTreePtr delta_tree;
    size_t       placed_delta_rows    = 0;
    size_t       placed_delta_deletes = 0;

    std::shared_mutex mutex;
};

} // namespace DB