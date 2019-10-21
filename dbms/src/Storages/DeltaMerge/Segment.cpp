#include <gperftools/malloc_extension.h>

#include <numeric>

#include <DataTypes/DataTypeFactory.h>

#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>

#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMerge.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaPlace.h>
#include <Storages/DeltaMerge/FilterHelper.h>
#include <Storages/DeltaMerge/Segment.h>

namespace ProfileEvents
{
extern const Event DMWriteBlock;
extern const Event DMWriteBlockNS;
extern const Event DMPlace;
extern const Event DMPlaceNS;
extern const Event DMPlaceUpsert;
extern const Event DMPlaceUpsertNS;
extern const Event DMPlaceDeleteRange;
extern const Event DMPlaceDeleteRangeNS;
extern const Event DMAppendDeltaPrepare;
extern const Event DMAppendDeltaPrepareNS;
extern const Event DMAppendDeltaCommitMemory;
extern const Event DMAppendDeltaCommitMemoryNS;
extern const Event DMAppendDeltaCommitDisk;
extern const Event DMAppendDeltaCommitDiskNS;
extern const Event DMAppendDeltaCleanUp;
extern const Event DMAppendDeltaCleanUpNS;
extern const Event DMSegmentSplit;
extern const Event DMSegmentSplitNS;
extern const Event DMSegmentGetSplitPoint;
extern const Event DMSegmentGetSplitPointNS;
extern const Event DMSegmentMerge;
extern const Event DMSegmentMergeNS;
extern const Event DMDeltaMerge;
extern const Event DMDeltaMergeNS;
} // namespace ProfileEvents


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_FORMAT_VERSION;
} // namespace ErrorCodes

namespace DM
{

const Segment::Version Segment::CURRENT_VERSION = 1;
const static size_t    SEGMENT_BUFFER_SIZE      = 128; // More than enough.
const static size_t    STABLE_CHUNK_ROWS        = DEFAULT_BLOCK_SIZE;

//==========================================================================================
// Segment ser/deser
//==========================================================================================

Segment::Segment(UInt64 epoch_, const HandleRange & range_, PageId segment_id_, PageId next_segment_id_, PageId delta_id, PageId stable_id)
    : epoch(epoch_),
      range(range_),
      segment_id(segment_id_),
      next_segment_id(next_segment_id_),
      delta(std::make_shared<DiskValueSpace>(true, delta_id)),
      stable(std::make_shared<DiskValueSpace>(false, stable_id)),
      delta_tree(std::make_shared<DefaultDeltaTree>()),
      log(&Logger::get("Segment"))
{
}

Segment::Segment(UInt64              epoch_, //
                 const HandleRange & range_,
                 PageId              segment_id_,
                 PageId              next_segment_id_,
                 DiskValueSpacePtr   delta_,
                 DiskValueSpacePtr   stable_)
    : epoch(epoch_),
      range(range_),
      segment_id(segment_id_),
      next_segment_id(next_segment_id_),
      delta(delta_),
      stable(stable_),
      delta_tree(std::make_shared<DefaultDeltaTree>()),
      log(&Logger::get("Segment"))
{
}

SegmentPtr Segment::newSegment(DMContext & context, const HandleRange & range, PageId segment_id, PageId next_segment_id)
{
    auto segment = std::make_shared<Segment>(INITIAL_EPOCH, //
                                             range,
                                             segment_id,
                                             next_segment_id,
                                             context.storage_pool.newMetaPageId(),
                                             context.storage_pool.newMetaPageId());

    WriteBatch meta_wb;
    WriteBatch data_wb;
    WriteBatch log_wb;

    // Write metadata.
    segment->serialize(meta_wb);
    segment->delta->replaceChunks(meta_wb, log_wb, {});
    segment->stable->replaceChunks(meta_wb, data_wb, {});

    context.storage_pool.meta().write(meta_wb);

    return segment;
}

SegmentPtr Segment::restoreSegment(DMContext & context, PageId segment_id)
{
    Page page = context.storage_pool.meta().read(segment_id);

    ReadBufferFromMemory buf(page.data.begin(), page.data.size());
    Version              version;
    readIntBinary(version, buf);
    if (version != CURRENT_VERSION)
        throw Exception("version not match", ErrorCodes::LOGICAL_ERROR);
    UInt64      epoch;
    HandleRange range;
    PageId      next_segment_id, delta_id, stable_id;

    readIntBinary(epoch, buf);
    readIntBinary(range.start, buf);
    readIntBinary(range.end, buf);
    readIntBinary(next_segment_id, buf);
    readIntBinary(delta_id, buf);
    readIntBinary(stable_id, buf);

    auto segment = std::make_shared<Segment>(epoch, range, segment_id, next_segment_id, delta_id, stable_id);

    segment->delta->restore(OpContext::createForLogStorage(context));
    segment->stable->restore(OpContext::createForDataStorage(context));

    return segment;
}

void Segment::serialize(WriteBatch & wb)
{
    MemoryWriteBuffer buf(0, SEGMENT_BUFFER_SIZE);
    writeIntBinary(CURRENT_VERSION, buf);
    writeIntBinary(epoch, buf);
    writeIntBinary(range.start, buf);
    writeIntBinary(range.end, buf);
    writeIntBinary(next_segment_id, buf);
    writeIntBinary(delta->pageId(), buf);
    writeIntBinary(stable->pageId(), buf);

    auto data_size = buf.count(); // Must be called before tryGetReadBuffer.
    wb.putPage(segment_id, 0, buf.tryGetReadBuffer(), data_size);
}

//==========================================================================================
// Segment public APIs.
//==========================================================================================

void Segment::write(DMContext & dm_context, const BlockOrDelete & update)
{
    auto         op_context   = OpContext::createForLogStorage(dm_context);
    auto &       storage_pool = dm_context.storage_pool;
    WriteBatches wbs;

    auto task = createAppendTask(op_context, wbs, update);

    wbs.writeLogAndData(storage_pool);
    wbs.writeMeta(storage_pool);

    applyAppendTask(op_context, task, update);

    wbs.writeRemoves(storage_pool);
}

AppendTaskPtr Segment::createAppendTask(const OpContext & opc, WriteBatches & wbs, const BlockOrDelete & update)
{
    if (update.block)
        LOG_DEBUG(log,
                  "Segment [" << DB::toString(segment_id) << "] create append task, write rows: " << DB::toString(update.block.rows()));
    else
        LOG_DEBUG(log, "Segment [" << DB::toString(segment_id) << "] create append task, delete range: " << update.delete_range.toString());

    EventRecorder recorder(ProfileEvents::DMAppendDeltaPrepare, ProfileEvents::DMAppendDeltaPrepareNS);

    // Create everything we need to do the update.
    // We only need a shared lock because this operation won't do any modifications.
    std::shared_lock lock(read_write_mutex);
    return delta->createAppendTask(opc, wbs, update);
}

void Segment::applyAppendTask(const OpContext & opc, const AppendTaskPtr & task, const BlockOrDelete & update)
{
    // Unique lock, to protect memory modifications against read threads.
    std::unique_lock segment_lock(read_write_mutex);

    EventRecorder recorder(ProfileEvents::DMAppendDeltaCommitMemory, ProfileEvents::DMAppendDeltaCommitMemoryNS);

    auto new_delta = delta->applyAppendTask(opc, task, update);

    if (update.block)
        LOG_DEBUG(log, "Segment [" << DB::toString(segment_id) << "] apply append task, write rows: " << DB::toString(update.block.rows()));
    else
        LOG_DEBUG(log, "Segment [" << DB::toString(segment_id) << "] apply append task, delete range: " << update.delete_range.toString());

    if (new_delta)
    {
        LOG_DEBUG(log, "Segment [" << DB::toString(segment_id) << "] update delta instance");
        delta = new_delta;
    }
}

SegmentSnapshot Segment::getReadSnapshot(bool use_delta_cache) const
{
    SegmentSnapshot segment_snap;
    {
        // Synchronize between read/write threads.
        std::shared_lock lock(read_write_mutex);

        size_t delta_rows = delta->num_rows();
        if (!use_delta_cache)
            delta_rows -= delta->cacheRows();

        // Stable is constant.
        segment_snap = SegmentSnapshot{
            .stable        = stable,
            .delta         = std::make_shared<DiskValueSpace>(*delta),
            .delta_rows    = delta_rows,
            .delta_deletes = delta->num_deletes(),
        };
    }

    {
        std::scoped_lock lock(read_read_mutex);

        // If current delta tree matches delta status, copy it now.
        if (segment_snap.delta_rows == placed_delta_rows && segment_snap.delta_deletes == placed_delta_deletes)
            segment_snap.delta_index = delta_tree->getEntriesCopy<Allocator<false>>();
    }
    return segment_snap;
}

BlockInputStreamPtr Segment::getInputStream(const DMContext &       dm_context,
                                            const ColumnDefines &   columns_to_read,
                                            const SegmentSnapshot & segment_snap,
                                            const StorageSnapshot & storage_snap,
                                            const HandleRanges &    read_ranges,
                                            const RSOperatorPtr &   filter,
                                            UInt64                  max_version,
                                            size_t                  expected_block_size)
{
    auto read_info = getReadInfo<true>(dm_context, columns_to_read, segment_snap, storage_snap);

    auto create_stream = [&](const HandleRange & read_range) {
        auto stream
            = getPlacedStream(read_info.storage_snap.data_reader,
                              read_info.read_columns,
                              withHanleRange(filter, read_range), // Here we filter out chunks which have irrelevant handle range roughly.
                              *read_info.segment_snap.stable,
                              read_info.delta_value_space,
                              read_info.index_begin,
                              read_info.index_end,
                              expected_block_size);
        stream = std::make_shared<DMHandleFilterBlockInputStream<true>>(stream, read_range, 0);
        return std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_MVCC>>(
            stream, dm_context.handle_column, max_version);
    };

    if (read_ranges.size() == 1)
    {
        LOG_TRACE(log, "Segment [" << DB::toString(segment_id) << "] is read by " << DB::toString(1) << " ranges");
        return create_stream(range.shrink(read_ranges[0]));
    }
    else
    {
        BlockInputStreams streams;
        for (auto & read_range : read_ranges)
        {
            HandleRange real_range = range.shrink(read_range);
            if (!real_range.none())
                streams.push_back(create_stream(real_range));
        }

        LOG_TRACE(log, "Segment [" << DB::toString(segment_id) << "] is read by " << DB::toString(streams.size()) << " ranges");

        return std::make_shared<ConcatBlockInputStream>(streams);
    }
}

BlockInputStreamPtr Segment::getInputStream(const DMContext &     dm_context,
                                            const ColumnDefines & columns_to_read,
                                            const HandleRanges &  read_ranges,
                                            const RSOperatorPtr & filter,
                                            UInt64                max_version,
                                            size_t                expected_block_size)
{
    return getInputStream(
        dm_context, columns_to_read, getReadSnapshot(), {dm_context.storage_pool}, read_ranges, filter, max_version, expected_block_size);
}

BlockInputStreamPtr Segment::getInputStreamRaw(const DMContext &       dm_context,
                                               const ColumnDefines &   columns_to_read,
                                               const SegmentSnapshot & segment_snap,
                                               const StorageSnapshot & storage_snap)
{
    auto & handle = dm_context.handle_column;

    ColumnDefines new_columns_to_read;
    new_columns_to_read.push_back(handle);

    for (const auto & c : columns_to_read)
    {
        if (c.id != handle.id)
            new_columns_to_read.push_back(c);
    }

    auto                delta_chunks = segment_snap.delta->getChunksBefore(segment_snap.delta_rows, segment_snap.delta_deletes);
    BlockInputStreamPtr delta_stream = std::make_shared<ChunkBlockInputStream>(delta_chunks, //
                                                                               new_columns_to_read,
                                                                               storage_snap.log_reader,
                                                                               EMPTY_FILTER);
    delta_stream                     = std::make_shared<DMHandleFilterBlockInputStream<false>>(delta_stream, range, 0);

    BlockInputStreamPtr stable_stream = std::make_shared<ChunkBlockInputStream>(segment_snap.stable->getChunks(), //
                                                                                new_columns_to_read,
                                                                                storage_snap.data_reader,
                                                                                EMPTY_FILTER);
    stable_stream                     = std::make_shared<DMHandleFilterBlockInputStream<true>>(stable_stream, range, 0);

    BlockInputStreams streams;
    streams.push_back(delta_stream);
    streams.push_back(stable_stream);
    return std::make_shared<ConcatBlockInputStream>(streams);
}

BlockInputStreamPtr Segment::getInputStreamRaw(const DMContext & dm_context, const ColumnDefines & columns_to_read)
{
    return getInputStreamRaw(dm_context, columns_to_read, getReadSnapshot(), {dm_context.storage_pool});
}

SegmentPair
Segment::split(DMContext & dm_context, const SegmentSnapshot & segment_snap, const StorageSnapshot & storage_snap, WriteBatches & wbs) const
{
    LOG_DEBUG(log, "Segment [" << DB::toString(segment_id) << "] start to split, estimated rows[" << DB::toString(estimatedRows()) << "]");

    SegmentPair res;

    auto merge_delta_then_split = [&]() {
        auto new_segment = mergeDelta(dm_context, segment_snap, storage_snap, wbs);

        // Write done the generated pages, otherwise they cannot be read by #split.
        wbs.writeLogAndData(dm_context.storage_pool);

        auto            new_segment_snap = new_segment->getReadSnapshot();
        StorageSnapshot new_storage_snap(dm_context.storage_pool);
        return new_segment->split(dm_context, new_segment_snap, new_storage_snap, wbs);
    };

    if (segment_snap.delta->num_rows() > segment_snap.stable->num_rows())
        return merge_delta_then_split();
    if (segment_snap.stable->num_chunks() <= 3)
        res = doSplitPhysical(dm_context, segment_snap, storage_snap, wbs);
    else
    {
        Handle split_point     = getSplitPointFast(dm_context, storage_snap.data_reader, *segment_snap.stable);
        bool   bad_split_point = !range.check(split_point) || split_point == range.start;
        if (bad_split_point)
            res = doSplitPhysical(dm_context, segment_snap, storage_snap, wbs);
        else
            res = doSplitLogical(dm_context, segment_snap, split_point, wbs);
    }

    LOG_DEBUG(log, "Segment [" << segment_id << "] split into " << res.first->info() << " and " << res.second->info());

    return res;
}

SegmentPair Segment::split(DMContext & dm_context) const
{
    WriteBatches wbs;
    auto &       storage_pool = dm_context.storage_pool;
    auto         res          = split(dm_context, getReadSnapshot(), {storage_pool}, wbs);
    wbs.writeAll(storage_pool);
    return res;
}

SegmentPtr Segment::merge(DMContext &             dm_context,
                          const SegmentPtr &      left,
                          const SegmentSnapshot & left_snap,
                          const SegmentPtr &      right,
                          const SegmentSnapshot & right_snap,
                          const StorageSnapshot & storage_snap,
                          WriteBatches &          wbs)
{
    LOG_DEBUG(left->log, "Merge segment [" << DB::toString(left->segment_id) << "] with [" << DB::toString(right->segment_id) << "]");

    SegmentPtr my_left  = left;
    SegmentPtr my_right = right;

    auto & left_chunks  = left->stable->getChunks();
    auto & right_chunks = right->stable->getChunks();
    if (!left_chunks.empty() && !right_chunks.empty())
    {
        if (left_chunks[left_chunks.size() - 1].getHandleFirstLast().second > right_chunks[0].getHandleFirstLast().first)
        {
            auto new_left  = left->mergeDelta(dm_context, left_snap, storage_snap, wbs);
            auto new_right = right->mergeDelta(dm_context, right_snap, storage_snap, wbs);

            my_left  = new_left;
            my_right = new_right;
        }
    }

    auto res = doMergeLogical(dm_context, my_left, my_right, wbs);

    LOG_DEBUG(left->log, "Done merge segment [" << DB::toString(left->segment_id) << "] with [" << DB::toString(right->segment_id) << "]");

    return res;
}

SegmentPtr Segment::merge(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right)
{
    WriteBatches wbs;
    auto &       storage_pool = dm_context.storage_pool;
    auto         res          = merge(dm_context, left, left->getReadSnapshot(), right, right->getReadSnapshot(), {storage_pool}, wbs);
    wbs.writeAll(storage_pool);
    return res;
}

DiskValueSpacePtr Segment::prepareMergeDelta(DMContext &             dm_context,
                                             const SegmentSnapshot & segment_snap,
                                             const StorageSnapshot & storage_snap,
                                             WriteBatches &          wbs) const
{
    LOG_DEBUG(log,
              "Segment [" << DB::toString(segment_id) << "] prepare merge delta start. delta chunks: " << DB::toString(delta->num_chunks())
                          << ", delta total rows: " << DB::toString(delta->num_rows()));

    EventRecorder recorder(ProfileEvents::DMDeltaMerge, ProfileEvents::DMDeltaMergeNS);

    auto & handle      = dm_context.handle_column;
    auto & columns     = dm_context.store_columns;
    auto   min_version = dm_context.min_version;

    auto read_info   = getReadInfo<false>(dm_context, columns, segment_snap, storage_snap);
    auto data_stream = getPlacedStream(storage_snap.data_reader,
                                       read_info.read_columns,
                                       EMPTY_FILTER,
                                       *segment_snap.stable,
                                       read_info.delta_value_space,
                                       read_info.index_begin,
                                       read_info.index_end,
                                       STABLE_CHUNK_ROWS);
    data_stream      = std::make_shared<DMHandleFilterBlockInputStream<true>>(data_stream, range, 0);
    data_stream      = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(data_stream, handle, min_version);

    OpContext opc               = OpContext::createForDataStorage(dm_context);
    Chunks    new_stable_chunks = DiskValueSpace::writeChunks(opc, data_stream, wbs.data);

    auto new_stable = std::make_shared<DiskValueSpace>(*segment_snap.stable);
    new_stable->replaceChunks(wbs.meta, wbs.removed_data, std::move(new_stable_chunks));

    LOG_DEBUG(log, "Segment [" << DB::toString(segment_id) << "] prepare merge delta done.");

    return new_stable;
}

SegmentPtr Segment::applyMergeDelta(const SegmentSnapshot & segment_snap, WriteBatches & wbs, const DiskValueSpacePtr & new_stable) const
{
    LOG_DEBUG(log, "Segment [" << DB::toString(segment_id) << "] apply merge delta start.");

    auto remove_delta_chunks = delta->getChunksBefore(segment_snap.delta_rows, segment_snap.delta_deletes);
    auto new_delta_chunks    = delta->getChunksAfter(segment_snap.delta_rows, segment_snap.delta_deletes);
    bool use_cache           = new_delta_chunks.size() > delta->cacheChunks();

    auto new_delta = std::make_shared<DiskValueSpace>(true, delta->pageId(), std::move(remove_delta_chunks));
    if (use_cache)
        new_delta->replaceChunks(wbs.meta, //
                                 wbs.removed_log,
                                 std::move(new_delta_chunks),
                                 delta->cloneCache(),
                                 delta->cacheChunks());
    else
        new_delta->replaceChunks(wbs.meta, //
                                 wbs.removed_log,
                                 std::move(new_delta_chunks));

    auto new_me = std::make_shared<Segment>(epoch + 1, //
                                            range,
                                            segment_id,
                                            next_segment_id,
                                            new_delta,
                                            new_stable);

    LOG_DEBUG(log, "Segment [" << DB::toString(segment_id) << "] apply merge delta done.");

    return new_me;
}

SegmentPtr Segment::mergeDelta(DMContext &             dm_context,
                               const SegmentSnapshot & segment_snap,
                               const StorageSnapshot & storage_snap,
                               WriteBatches &          wbs) const
{
    auto new_stable = prepareMergeDelta(dm_context, segment_snap, storage_snap, wbs);
    return applyMergeDelta(segment_snap, wbs, new_stable);
}

SegmentPtr Segment::mergeDelta(DMContext & dm_context) const
{
    WriteBatches wbs;
    auto &       storage_pool = dm_context.storage_pool;
    auto         res          = mergeDelta(dm_context, getReadSnapshot(), {storage_pool}, wbs);
    wbs.writeAll(storage_pool);
    return res;
}

void Segment::check(DMContext & dm_context, const String & when) const
{
    auto &     storage_pool = dm_context.storage_pool;
    PageReader reader(storage_pool.meta(), storage_pool.meta().getSnapshot());
    delta->check(reader, when);
    stable->check(reader, when);
}

void Segment::flushCache(DMContext & dm_context)
{
    WriteBatch remove_log_wb;
    doFlushCache(dm_context, remove_log_wb);
    dm_context.storage_pool.log().write(remove_log_wb);
}

size_t Segment::getEstimatedRows() const
{
    std::shared_lock lock(read_write_mutex);
    return estimatedRows();
}

size_t Segment::getEstimatedBytes() const
{
    std::shared_lock lock(read_write_mutex);
    return estimatedBytes();
}

size_t Segment::deltaRows(bool with_delta_cache) const
{
    std::shared_lock lock(read_write_mutex);
    auto             rows = delta->num_rows();
    if (!with_delta_cache)
        rows -= delta->cacheRows();
    return rows;
}

size_t Segment::updatesInDeltaTree() const
{
    return delta_tree->numInserts() + delta_tree->numDeletes();
}

//==========================================================================================
// Segment private methods.
//==========================================================================================

template <bool add_tag_column>
Segment::ReadInfo Segment::getReadInfo(const DMContext &       dm_context,
                                       const ColumnDefines &   read_columns,
                                       const SegmentSnapshot & segment_snap,
                                       const StorageSnapshot & storage_snap) const
{
    LOG_TRACE(log, "getReadInfo start");

    auto new_read_columns = arrangeReadColumns<add_tag_column>(dm_context.handle_column, read_columns);

    DeltaValueSpacePtr       delta_value_space;
    ChunkBlockInputStreamPtr stable_input_stream;
    DeltaIndexPtr            delta_index;

    auto delta_block  = segment_snap.delta->read(new_read_columns, storage_snap.log_reader, 0, segment_snap.delta_rows);
    delta_value_space = std::make_shared<DeltaValueSpace>(dm_context.handle_column, new_read_columns, delta_block);

    if (segment_snap.delta_index)
    {
        delta_index = segment_snap.delta_index;
    }
    else
    {
        delta_index = ensurePlace(dm_context, //
                                  storage_snap,
                                  *segment_snap.stable,
                                  *segment_snap.delta,
                                  segment_snap.delta_rows,
                                  segment_snap.delta_deletes,
                                  delta_value_space);
    }

    auto index_begin = DeltaIndex::begin(delta_index);
    auto index_end   = DeltaIndex::end(delta_index);

    LOG_TRACE(log, "getReadInfo end");

    return {
        .storage_snap      = storage_snap,
        .segment_snap      = segment_snap,
        .delta_value_space = delta_value_space,
        .index_begin       = index_begin,
        .index_end         = index_end,
        .read_columns      = new_read_columns,
    };
}

template <bool add_tag_column>
ColumnDefines Segment::arrangeReadColumns(const ColumnDefine & handle, const ColumnDefines & columns_to_read)
{
    // We always put handle, version and tag column at the beginning of columns.
    ColumnDefines new_columns_to_read;

    new_columns_to_read.push_back(handle);
    new_columns_to_read.push_back(getVersionColumnDefine());
    if constexpr (add_tag_column)
        new_columns_to_read.push_back(getTagColumnDefine());

    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        auto & c  = columns_to_read[i];
        bool   ok = c.id != handle.id && c.id != VERSION_COLUMN_ID;
        if constexpr (add_tag_column)
            ok = ok && c.id != TAG_COLUMN_ID;

        if (ok)
            new_columns_to_read.push_back(c);
    }

    return new_columns_to_read;
}

template <class IndexIterator>
BlockInputStreamPtr Segment::getPlacedStream(const PageReader &         data_page_reader,
                                             const ColumnDefines &      read_columns,
                                             const RSOperatorPtr &      filter,
                                             const DiskValueSpace &     stable_snap,
                                             const DeltaValueSpacePtr & delta_value_space,
                                             const IndexIterator &      delta_index_begin,
                                             const IndexIterator &      delta_index_end,
                                             size_t                     expected_block_size) const
{
    auto stable_input_stream = std::make_shared<ChunkBlockInputStream>(stable_snap.getChunks(), read_columns, data_page_reader, filter);
    return std::make_shared<DeltaMergeBlockInputStream<DeltaValueSpace, IndexIterator>>( //
        stable_input_stream,
        delta_value_space,
        delta_index_begin,
        delta_index_end,
        expected_block_size);
}

Handle Segment::getSplitPointFast(DMContext & dm_context, const PageReader & data_page_reader, const DiskValueSpace & stable_snap) const
{
    EventRecorder recorder(ProfileEvents::DMSegmentGetSplitPoint, ProfileEvents::DMSegmentGetSplitPointNS);

    auto & chunks = stable_snap.getChunks();
    if (unlikely(chunks.empty()))
        throw Exception("getSplitPointFast can only works on stable value space");
    size_t split_row_index = stable_snap.num_rows() / 2;
    auto   block           = stable_snap.read({dm_context.handle_column}, data_page_reader, split_row_index, 1);
    return block.getByPosition(0).column->getInt(0);
}

Handle Segment::getSplitPointSlow(DMContext & dm_context, const ReadInfo & read_info) const
{
    EventRecorder recorder(ProfileEvents::DMSegmentGetSplitPoint, ProfileEvents::DMSegmentGetSplitPointNS);

    auto & handle     = dm_context.handle_column;
    size_t exact_rows = 0;

    {
        auto stream = getPlacedStream(read_info.storage_snap.data_reader,
                                      {dm_context.handle_column},
                                      EMPTY_FILTER,
                                      *read_info.segment_snap.stable,
                                      read_info.delta_value_space,
                                      read_info.index_begin,
                                      read_info.index_end,
                                      DEFAULT_BLOCK_SIZE);
        stream->readPrefix();
        Block block;
        while ((block = stream->read()))
            exact_rows += block.rows();
        stream->readSuffix();
    }

    auto stream = getPlacedStream(read_info.storage_snap.data_reader,
                                  {dm_context.handle_column},
                                  EMPTY_FILTER,
                                  *read_info.segment_snap.stable,
                                  read_info.delta_value_space,
                                  read_info.index_begin,
                                  read_info.index_end,
                                  DEFAULT_BLOCK_SIZE);

    size_t split_row_index = exact_rows / 2;
    Handle split_handle    = 0;
    size_t count           = 0;

    stream->readPrefix();
    while (true)
    {
        Block block = stream->read();
        if (!block)
            break;
        count += block.rows();
        if (count > split_row_index)
        {
            size_t offset_in_block = block.rows() - (count - split_row_index);
            split_handle           = block.getByName(handle.name).column->getInt(offset_in_block);
            break;
        }
    }
    stream->readSuffix();

    return split_handle;
}

SegmentPair
Segment::doSplitLogical(DMContext & dm_context, const SegmentSnapshot & segment_snap, Handle split_point, WriteBatches & wbs) const
{
    LOG_TRACE(log, "Segment [" << segment_id << "] split logical");

    EventRecorder recorder(ProfileEvents::DMSegmentSplit, ProfileEvents::DMSegmentSplitNS);

    auto & storage_pool = dm_context.storage_pool;

    HandleRange my_range    = {range.start, split_point};
    HandleRange other_range = {split_point, range.end};

    auto other_segment_id = storage_pool.newMetaPageId();
    auto other_delta_id   = storage_pool.newMetaPageId();
    auto other_stable_id  = storage_pool.newMetaPageId();

    auto new_me = std::make_shared<Segment>(this->epoch + 1, //
                                            my_range,
                                            this->segment_id,
                                            other_segment_id,
                                            std::make_shared<DiskValueSpace>(*segment_snap.delta),
                                            std::make_shared<DiskValueSpace>(*segment_snap.stable));

    auto other = std::make_shared<Segment>(INITIAL_EPOCH, //
                                           other_range,
                                           other_segment_id,
                                           this->next_segment_id,
                                           other_delta_id,
                                           other_stable_id);

    GenPageId log_gen_page_id = std::bind(&StoragePool::newLogPageId, &storage_pool);

    Chunks my_delta_chunks = createRefChunks(segment_snap.delta->getChunks(), log_gen_page_id, wbs.log);
    Chunks my_stable_chunks;

    Chunks other_delta_chunks = createRefChunks(segment_snap.delta->getChunks(), log_gen_page_id, wbs.log);
    Chunks other_stable_chunks;

    GenPageId data_gen_page_id = std::bind(&StoragePool::newDataPageId, &storage_pool);
    for (auto & chunk : segment_snap.stable->getChunks())
    {
        auto [handle_first, handle_last] = chunk.getHandleFirstLast();
        if (my_range.intersect(handle_first, handle_last))
            my_stable_chunks.push_back(createRefChunk(chunk, data_gen_page_id, wbs.data));
        if (other_range.intersect(handle_first, handle_last))
            other_stable_chunks.push_back(createRefChunk(chunk, data_gen_page_id, wbs.data));
    }

    new_me->serialize(wbs.meta);
    new_me->delta->replaceChunks(wbs.meta, //
                                 wbs.removed_log,
                                 std::move(my_delta_chunks),
                                 segment_snap.delta->cloneCache(),
                                 segment_snap.delta->cacheChunks());
    new_me->stable->replaceChunks(wbs.meta, wbs.removed_data, std::move(my_stable_chunks));

    other->serialize(wbs.meta);
    other->delta->replaceChunks(wbs.meta, //
                                wbs.removed_log,
                                std::move(other_delta_chunks),
                                segment_snap.delta->cloneCache(),
                                segment_snap.delta->cacheChunks());
    other->stable->replaceChunks(wbs.meta, wbs.removed_data, std::move(other_stable_chunks));

    return {new_me, other};
}
SegmentPair Segment::doSplitPhysical(DMContext &             dm_context,
                                     const SegmentSnapshot & segment_snap,
                                     const StorageSnapshot & storage_snap,
                                     WriteBatches &          wbs) const
{
    LOG_TRACE(log, "Segment [" << segment_id << "] split physical");

    EventRecorder recorder(ProfileEvents::DMSegmentSplit, ProfileEvents::DMSegmentSplitNS);

    auto & handle       = dm_context.handle_column;
    auto & storage_pool = dm_context.storage_pool;
    auto   min_version  = dm_context.min_version;

    auto & columns   = dm_context.store_columns;
    auto   read_info = getReadInfo<false>(dm_context, columns, segment_snap, storage_snap);

    auto split_point = getSplitPointSlow(dm_context, read_info);

    HandleRange my_range    = {range.start, split_point};
    HandleRange other_range = {split_point, range.end};

    Chunks my_new_stable_chunks;
    Chunks other_new_stable_chunks;

    OpContext opc = OpContext::createForDataStorage(dm_context);
    {
        // Write my data
        BlockInputStreamPtr my_data = getPlacedStream(storage_snap.data_reader,
                                                      read_info.read_columns,
                                                      EMPTY_FILTER,
                                                      *segment_snap.stable,
                                                      read_info.delta_value_space,
                                                      read_info.index_begin,
                                                      read_info.index_end,
                                                      STABLE_CHUNK_ROWS);
        my_data                     = std::make_shared<DMHandleFilterBlockInputStream<true>>(my_data, my_range, 0);
        my_data  = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(my_data, handle, min_version);
        auto tmp = DiskValueSpace::writeChunks(opc, my_data, wbs.data);
        my_new_stable_chunks.swap(tmp);
    }

    {
        // Write new segment's data
        BlockInputStreamPtr other_data = getPlacedStream(storage_snap.data_reader,
                                                         read_info.read_columns,
                                                         EMPTY_FILTER,
                                                         *segment_snap.stable,
                                                         read_info.delta_value_space,
                                                         read_info.index_begin,
                                                         read_info.index_end,
                                                         STABLE_CHUNK_ROWS);
        other_data                     = std::make_shared<DMHandleFilterBlockInputStream<true>>(other_data, other_range, 0);
        other_data = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(other_data, handle, min_version);
        auto tmp   = DiskValueSpace::writeChunks(opc, other_data, wbs.data);
        other_new_stable_chunks.swap(tmp);
    }

    auto other_segment_id = storage_pool.newMetaPageId();
    auto other_delta_id   = storage_pool.newMetaPageId();
    auto other_stable_id  = storage_pool.newMetaPageId();

    auto new_me = std::make_shared<Segment>(this->epoch + 1, //
                                            my_range,
                                            this->segment_id,
                                            other_segment_id,
                                            std::make_shared<DiskValueSpace>(*segment_snap.delta),
                                            std::make_shared<DiskValueSpace>(*segment_snap.stable));

    auto other = std::make_shared<Segment>(INITIAL_EPOCH, //
                                           other_range,
                                           other_segment_id,
                                           this->next_segment_id,
                                           other_delta_id,
                                           other_stable_id);

    new_me->serialize(wbs.meta);
    new_me->delta->replaceChunks(wbs.meta, wbs.removed_log, {});
    new_me->stable->replaceChunks(wbs.meta, wbs.removed_data, std::move(my_new_stable_chunks));

    other->serialize(wbs.meta);
    other->delta->replaceChunks(wbs.meta, wbs.removed_log, {});
    other->stable->replaceChunks(wbs.meta, wbs.removed_data, std::move(other_new_stable_chunks));

    return {new_me, other};
}

SegmentPtr Segment::doMergeLogical(DMContext & /*dm_context*/, const SegmentPtr & left, const SegmentPtr & right, WriteBatches & wbs)
{
    if (unlikely(left->range.end != right->range.start || left->next_segment_id != right->segment_id))
        throw Exception("The ranges of merge segments are not consecutive: first end: " + DB::toString(left->range.end)
                        + ", second start: " + DB::toString(right->range.start));

    EventRecorder recorder(ProfileEvents::DMSegmentMerge, ProfileEvents::DMSegmentMergeNS);

    HandleRange merge_range = {left->range.start, right->range.end};

    Chunks merged_delta_chunks = left->delta->getChunks();
    merged_delta_chunks.insert(merged_delta_chunks.end(), right->delta->getChunks().begin(), right->delta->getChunks().end());
    Chunks merged_stable_chunks = left->stable->getChunks();
    merged_stable_chunks.insert(merged_stable_chunks.end(), right->stable->getChunks().begin(), right->stable->getChunks().end());

    auto merged = std::make_shared<Segment>(left->epoch + 1, //
                                            merge_range,
                                            left->segment_id,
                                            right->next_segment_id,
                                            left->delta->pageId(),
                                            left->stable->pageId());

    merged->serialize(wbs.meta);
    merged->delta->replaceChunks(wbs.meta, wbs.removed_log, std::move(merged_delta_chunks));
    merged->stable->replaceChunks(wbs.meta, wbs.removed_data, std::move(merged_stable_chunks));

    return merged;
}

void Segment::doFlushCache(DMContext & dm_context, WriteBatch & remove_log_wb)
{
    std::unique_lock lock(read_write_mutex);

    auto new_delta = delta->tryFlushCache(OpContext::createForLogStorage(dm_context), remove_log_wb, /* force= */ true);
    if (new_delta)
    {
        LOG_DEBUG(log, "Segment [" << DB::toString(segment_id) << "] update delta instance");

        delta = new_delta;
    }
}

DeltaIndexPtr Segment::ensurePlace(const DMContext &          dm_context,
                                   const StorageSnapshot &    storage_snapshot,
                                   const DiskValueSpace &     stable_snap,
                                   const DiskValueSpace &     to_place_delta,
                                   size_t                     delta_rows_limit,
                                   size_t                     delta_deletes_limit,
                                   const DeltaValueSpacePtr & delta_value_space) const
{
    // Synchronize between read/read threads.
    std::scoped_lock lock(read_read_mutex);

    DeltaTreePtr update_delta_tree;
    size_t       my_placed_delta_rows;
    size_t       my_placed_delta_deletes;
    bool         is_update_local_delta_tree;

    // Already placed.
    if (placed_delta_rows == delta_rows_limit && placed_delta_deletes == delta_deletes_limit)
        return delta_tree->getEntriesCopy<Allocator<false>>();

    if (placed_delta_rows > delta_rows_limit || placed_delta_deletes > delta_deletes_limit)
    {
        // Current delta_tree in Segment is too new for those delta_rows_limit and delta_deletes_limit, we must recreate another one.
        update_delta_tree          = std::make_shared<DefaultDeltaTree>();
        my_placed_delta_rows       = 0;
        my_placed_delta_deletes    = 0;
        is_update_local_delta_tree = false;
    }
    else
    {
        update_delta_tree          = delta_tree;
        my_placed_delta_rows       = placed_delta_rows;
        my_placed_delta_deletes    = placed_delta_deletes;
        is_update_local_delta_tree = true;
    }

    EventRecorder recorder(ProfileEvents::DMPlace, ProfileEvents::DMPlaceNS);

    auto blocks = to_place_delta.getMergeBlocks(dm_context.handle_column,
                                                storage_snapshot.log_reader,
                                                my_placed_delta_rows,
                                                my_placed_delta_deletes,
                                                delta_rows_limit,
                                                delta_deletes_limit);

    for (auto & v : blocks)
    {
        if (!v.delete_range.none())
        {
            placeDelete(dm_context, storage_snapshot.data_reader, stable_snap, delta_value_space, v.delete_range, *update_delta_tree);
            ++my_placed_delta_deletes;
        }
        else if (v.block)
        {
            auto rows = v.block.rows();
            placeUpsert(dm_context,
                        storage_snapshot.data_reader,
                        stable_snap,
                        delta_value_space,
                        my_placed_delta_rows,
                        std::move(v.block),
                        *update_delta_tree);
            my_placed_delta_rows += rows;
        }
    }

    if (unlikely(my_placed_delta_rows != delta_rows_limit || my_placed_delta_deletes != delta_deletes_limit))
        throw Exception("Illegal status: place delta rows and deletes are not equal to requested limit");

    if (is_update_local_delta_tree)
    {
        placed_delta_rows    = my_placed_delta_rows;
        placed_delta_deletes = my_placed_delta_deletes;
    }

    return update_delta_tree->getEntriesCopy<Allocator<false>>();
}

void Segment::placeUpsert(const DMContext &          dm_context,
                          const PageReader &         data_page_reader,
                          const DiskValueSpace &     stable_snap,
                          const DeltaValueSpacePtr & delta_value_space,
                          size_t                     delta_value_space_offset,
                          Block &&                   block,
                          DeltaTree &                update_delta_tree) const
{
    EventRecorder recorder(ProfileEvents::DMPlaceUpsert, ProfileEvents::DMPlaceUpsertNS);

    auto & handle            = dm_context.handle_column;
    auto   delta_index_begin = update_delta_tree.begin();
    auto   delta_index_end   = update_delta_tree.end();

    BlockInputStreamPtr merged_stream = getPlacedStream<DefaultDeltaTree::EntryIterator>( //
        data_page_reader,
        {handle, getVersionColumnDefine()},
        EMPTY_FILTER,
        stable_snap,
        delta_value_space,
        delta_index_begin,
        delta_index_end,
        DEFAULT_BLOCK_SIZE);

    IColumn::Permutation perm;
    if (sortBlockByPk(handle, block, perm))
        DM::placeInsert<true>(merged_stream, block, update_delta_tree, delta_value_space_offset, perm, getPkSort(handle));
    else
        DM::placeInsert<false>(merged_stream, block, update_delta_tree, delta_value_space_offset, perm, getPkSort(handle));
}

void Segment::placeDelete(const DMContext &          dm_context,
                          const PageReader &         data_page_reader,
                          const DiskValueSpace &     stable_snap,
                          const DeltaValueSpacePtr & delta_value_space,
                          const HandleRange &        delete_range,
                          DeltaTree &                update_delta_tree) const
{
    EventRecorder recorder(ProfileEvents::DMPlaceDeleteRange, ProfileEvents::DMPlaceDeleteRangeNS);

    auto & handle            = dm_context.handle_column;
    auto   delta_index_begin = update_delta_tree.begin();
    auto   delta_index_end   = update_delta_tree.end();

    Blocks delete_data;
    {
        BlockInputStreamPtr delete_stream = getPlacedStream<DefaultDeltaTree::EntryIterator>( //
            data_page_reader,
            {handle, getVersionColumnDefine()},
            withHanleRange(EMPTY_FILTER, delete_range),
            stable_snap,
            delta_value_space,
            delta_index_begin,
            delta_index_end,
            DEFAULT_BLOCK_SIZE);

        delete_stream = std::make_shared<DMHandleFilterBlockInputStream<true>>(delete_stream, delete_range, 0);

        // Try to merge into big block. 128 MB should be enough.
        SquashingBlockInputStream squashed_delete_stream(delete_stream, 0, 128 * (1UL << 20));

        while (true)
        {
            Block block = squashed_delete_stream.read();
            if (!block)
                break;
            delete_data.emplace_back(std::move(block));
        }
    }

    // Note that we can not do read and place at the same time.
    for (const auto & block : delete_data)
    {
        BlockInputStreamPtr merged_stream = getPlacedStream<DefaultDeltaTree::EntryIterator>( //
            data_page_reader,
            {handle, getVersionColumnDefine()},
            EMPTY_FILTER,
            stable_snap,
            delta_value_space,
            delta_index_begin,
            delta_index_end,
            DEFAULT_BLOCK_SIZE);
        DM::placeDelete(merged_stream, block, update_delta_tree, getPkSort(handle));
    }
}

MinMaxIndexPtr Segment::getMinMax(const ColumnDefine & column_define) const
{
    auto minmax = std::make_shared<MinMaxIndex>();

    auto merge_minmax = [&](const DiskValueSpacePtr & value_space) -> bool {
        for (auto & chunk : value_space->getChunks())
        {
            auto * col = chunk.tryGetColumn(column_define.id);
            // TODO: Handle the case of after DDL
            if (!col || !col->type->equals(*column_define.type))
                return false;
            minmax->merge(*(col->minmax));
        }
        return true;
    };

    bool ok = merge_minmax(delta) && merge_minmax(stable);
    if (ok)
        return minmax;
    else
        return {};
}

size_t Segment::estimatedRows() const
{
    // Not 100% accurate.
    ssize_t rows
        = ((ssize_t)stable->num_rows()) + delta_tree->numInserts() - delta_tree->numDeletes() + (delta->num_rows() - placed_delta_rows);
    return std::max(0, rows);
}

size_t Segment::estimatedBytes() const
{
    size_t stable_bytes = stable->num_bytes();
    if (stable->num_rows() == 0)
    {
        return stable_bytes + delta->num_bytes();
    }
    else
    {
        return stable_bytes + delta->num_bytes() - (stable_bytes / stable->num_rows()) * delta_tree->numDeletes();
    }
}

} // namespace DM
} // namespace DB