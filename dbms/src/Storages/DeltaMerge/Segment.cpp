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
#include <Storages/DeltaMerge/Segment.h>

namespace ProfileEvents
{
extern const Event DMWriteBlock;
extern const Event DMWriteBlockNS;
extern const Event DMAppendDelta;
extern const Event DMAppendDeltaNS;
extern const Event DMPlace;
extern const Event DMPlaceNS;
extern const Event DMPlaceUpsert;
extern const Event DMPlaceUpsertNS;
extern const Event DMPlaceDeleteRange;
extern const Event DMPlaceDeleteRangeNS;
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

using OpContext = DiskValueSpace::OpContext;

//==========================================================================================
// Segment ser/deser
//==========================================================================================

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
    segment->delta.setChunks({}, meta_wb, log_wb);
    segment->stable.setChunks({}, meta_wb, data_wb);

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

    segment->delta.restore(OpContext::createForLogStorage(context));
    segment->stable.restore(OpContext::createForDataStorage(context));

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
    writeIntBinary(delta.pageId(), buf);
    writeIntBinary(stable.pageId(), buf);

    auto data_size = buf.count(); // Must be called before tryGetReadBuffer.
    wb.putPage(segment_id, 0, buf.tryGetReadBuffer(), data_size);
}

//==========================================================================================
// Segment public APIs.
//==========================================================================================

SegmentPtr Segment::write(DMContext & dm_context, BlockOrDelete && block_or_delete)
{
    if (block_or_delete.block)
        LOG_DEBUG(log, "Segment[" + DB::toString(segment_id) + "] write rows: " + DB::toString(block_or_delete.block.rows()));
    else
        LOG_DEBUG(log, "Segment[" + DB::toString(segment_id) + "] delete range: " + block_or_delete.delete_range.toString());

    OpContext opc = OpContext::createForLogStorage(dm_context);

    EventRecorder recorder(ProfileEvents::DMAppendDelta, ProfileEvents::DMAppendDeltaNS);

    DiskValueSpace::AppendTaskPtr task;
    {
        // Create everything we need to do the update.
        // We only need a shared lock because this operation won't do any modifications.
        std::shared_lock lock(read_write_mutex);
        task = delta.createAppendTask(opc, block_or_delete);
    }

    // Write data to disk.
    dm_context.storage_pool.log().write(task->data_write_batch);
    dm_context.storage_pool.meta().write(task->meta_write_batch);
    dm_context.storage_pool.log().write(task->data_remove_write_batch);

    {
        // Update metadata in memory.
        // Here we need a unique lock to do modifications in memory.
        std::unique_lock lock(read_write_mutex);

        delta.applyAppendTask(opc, task, block_or_delete);
    }

    recorder.submit();

#ifndef NDEBUG
    check(dm_context, "After write");
#endif

    if (shouldFlush(dm_context))
    {
        auto res = flush(dm_context);
#ifndef NDEBUG
        res->check(dm_context, "After delta merge");
#endif
        return res;
    }
    return {};
}

void Segment::check(DMContext & dm_context, const String & when)
{
    auto & handle = dm_context.table_handle_define;

    size_t stable_rows = stable.num_rows();
    size_t delta_rows  = delta.num_rows();

    LOG_DEBUG(log, when + ": stable_rows:" + DB::toString(stable_rows) + ", delta_rows:" + DB::toString(delta_rows));

    auto snapshot = getReadSnapshot<false>(dm_context, {handle});

    LOG_DEBUG(log,
              when + ": entries:" + DB::toString(delta_tree->numEntries()) + ", inserts:" + DB::toString(delta_tree->numInserts())
                  + ", deletes:" + DB::toString(delta_tree->numDeletes()));

    auto stream = getPlacedStream(dm_context,
                                  {range},
                                  snapshot.read_columns,
                                  snapshot.delta_value_space,
                                  snapshot.index_begin,
                                  snapshot.index_end,
                                  DEFAULT_BLOCK_SIZE);

    size_t total_rows = 0;
    while (true)
    {
        Block block = stream->read();
        if (!block)
            break;
        if (!block.rows())
            continue;
        const auto & handle_data = getColumnVectorData<Handle>(block, block.getPositionByName(handle.name));
        auto         rows        = block.rows();
        for (size_t i = 0; i < rows; ++i)
        {
            if (!range.check(handle_data[i]))
                throw Exception(when + ": Segment contains illegal rows(raw)");
        }
        total_rows += rows;
    }

    LOG_DEBUG(log, when + ": rows(raw): " + DB::toString(total_rows));
}

BlockInputStreamPtr Segment::getInputStream(const DMContext &     dm_context,
                                            const ColumnDefines & columns_to_read,
                                            const HandleRanges &  read_ranges,
                                            UInt64                max_version,
                                            size_t                expected_block_size)
{
    auto & handle   = dm_context.table_handle_define;
    auto   snapshot = getReadSnapshot<true>(dm_context, columns_to_read);
    auto   stream   = getPlacedStream(dm_context,
                                  read_ranges,
                                  snapshot.read_columns,
                                  snapshot.delta_value_space,
                                  snapshot.index_begin,
                                  snapshot.index_end,
                                  expected_block_size);
    stream          = std::make_shared<DMVersionFilterBlockInputStream<DM_VESION_FILTER_MODE_MVCC>>(stream, handle, max_version);
    return stream;
}

BlockInputStreamPtr Segment::getInputStreamRaw(const DMContext & dm_context, const ColumnDefines & columns_to_read)
{
    auto & handle  = dm_context.table_handle_define;
    auto & storage = dm_context.storage_pool;

    ColumnDefines new_columns_to_read;
    new_columns_to_read.push_back(handle);

    for (const auto & c : columns_to_read)
    {
        if (c.id != handle.id)
            new_columns_to_read.push_back(c);
    }


    DiskValueSpacePtr delta_snap;
    {
        // Create a new delta vs, so that later read operations won't block write thread.
        std::unique_lock lock(read_write_mutex);
        delta_snap = std::make_shared<DiskValueSpace>(delta);
    }

    //    auto                delta_block  = delta_snap->read(new_columns_to_read, storage.log(), 0, delta_snap->num_rows());
    //    BlockInputStreamPtr delta_stream = std::make_shared<OneBlockInputStream>(delta_block);
    BlockInputStreamPtr delta_stream = delta_snap->getInputStream(new_columns_to_read, storage.log());
    delta_stream                     = std::make_shared<DMHandleFilterBlockInputStream>(delta_stream, range, 0, false);

    BlockInputStreamPtr stable_stream = stable.getInputStream(new_columns_to_read, storage.data());
    stable_stream                     = std::make_shared<DMHandleFilterBlockInputStream>(stable_stream, range, 0, true);

    BlockInputStreams streams;
    streams.push_back(delta_stream);
    streams.push_back(stable_stream);
    return std::make_shared<ConcatBlockInputStream>(streams);
}

SegmentPair Segment::split(DMContext & dm_context)
{
    LOG_DEBUG(log, "Segment [" + DB::toString(segment_id) + "] start to split.");

    auto   snapshot    = getReadSnapshot<false>(dm_context, dm_context.table_columns);
    Handle split_point = getSplitPoint(dm_context, snapshot);
    auto   res         = doSplit(dm_context, snapshot, split_point);

    LOG_DEBUG(log, "Segment [" + DB::toString(segment_id) + "] done split.");

    return res;
}

SegmentPtr Segment::merge(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right)
{
    LOG_DEBUG(left->log, "Merge segment [" + DB::toString(left->segment_id) + "] with [" + DB::toString(right->segment_id) + "]");

    auto left_snapshot  = left->getReadSnapshot<false>(dm_context, dm_context.table_columns);
    auto right_snapshot = right->getReadSnapshot<false>(dm_context, dm_context.table_columns);

    auto res = doMerge(dm_context, left, left_snapshot, right, right_snapshot);

    LOG_DEBUG(left->log, "Done merge segment [" + DB::toString(left->segment_id) + "] with [" + DB::toString(right->segment_id) + "]");

    return res;
}


Segment::Segment(UInt64 epoch_, const HandleRange & range_, PageId segment_id_, PageId next_segment_id_, PageId delta_id, PageId stable_id)
    : epoch(epoch_),
      range(range_),
      segment_id(segment_id_),
      next_segment_id(next_segment_id_),
      delta(true, delta_id),
      stable(false, stable_id),
      delta_tree(std::make_shared<DefaultDeltaTree>()),
      log(&Logger::get("Segment"))
{
}

Segment::Segment(UInt64              epoch_,
                 const HandleRange & range_,
                 PageId              segment_id_,
                 PageId              next_segment_id_,
                 PageId              delta_id,
                 const Chunks &      delta_chunks_,
                 PageId              stable_id,
                 const Chunks &      stable_chunks_)
    : epoch(epoch_),
      range(range_),
      segment_id(segment_id_),
      next_segment_id(next_segment_id_),
      delta(true, delta_id, delta_chunks_),
      stable(false, stable_id, stable_chunks_),
      delta_tree(std::make_shared<DefaultDeltaTree>()),
      log(&Logger::get("Segment"))
{
}

//void Segment::swap(Segment & other)
//{
//    std::swap(epoch, other.epoch);
//    range.swap(other.range);
//    std::swap(segment_id, other.segment_id);
//    std::swap(next_segment_id, other.next_segment_id);
//
//    delta.swap(other.delta);
//    stable.swap(other.stable);
//
//    delta_tree.swap(other.delta_tree);
//    std::swap(placed_delta_rows, other.placed_delta_rows);
//    std::swap(placed_delta_deletes, other.placed_delta_deletes);
//}

size_t Segment::getEstimatedRows()
{
    std::shared_lock lock(read_write_mutex);
    return estimatedRows();
}

size_t Segment::getEstimatedBytes()
{
    std::shared_lock lock(read_write_mutex);
    return estimatedBytes();
}

size_t Segment::delta_rows()
{
    std::shared_lock lock(read_write_mutex);
    return delta.num_rows();
}

size_t Segment::delta_deletes()
{
    std::shared_lock lock(read_write_mutex);
    return delta.num_deletes();
}

//==========================================================================================
// Segment private methods.
//==========================================================================================

template <bool add_tag_column>
ReadSnapshot Segment::getReadSnapshot(const DMContext & dm_context, const ColumnDefines & columns_to_read)
{
    auto new_columns_to_read = arrangeReadColumns<add_tag_column>(dm_context.table_handle_define, columns_to_read);

    // Create a new delta vs and delta index snapshot, so that later read/write operations won't block write thread.
    DiskValueSpacePtr delta_snap;
    {
        // Synchronize between read/write threads.
        std::shared_lock lock(read_write_mutex);
        delta_snap = std::make_shared<DiskValueSpace>(delta);
    }

    auto & handle  = dm_context.table_handle_define;
    auto & storage = dm_context.storage_pool;

    const auto delta_block            = delta_snap->read(new_columns_to_read, storage.log(), 0, delta_snap->num_rows());
    auto       delta_snap_value_space = std::make_shared<DeltaValueSpace>(handle, new_columns_to_read, delta_block);

    DeltaIndexPtr delta_index;
    {
        // Synchronize between read/read threads.
        std::unique_lock lock(read_read_mutex);
        delta_index = ensurePlace(dm_context, delta_snap, delta_snap_value_space);
    }

    auto index_begin = DeltaIndex::begin(delta_index);
    auto index_end   = DeltaIndex::end(delta_index);

    return {
        .delta_value_space = delta_snap_value_space,
        .index_begin       = index_begin,
        .index_end         = index_end,
        .read_columns      = new_columns_to_read,
    };
}

template <bool add_tag_column>
ColumnDefines Segment::arrangeReadColumns(const ColumnDefine & handle, const ColumnDefines & columns_to_read)
{
    // We always put handle, version and tag column at the beginning of columns.
    ColumnDefines new_columns_to_read;

    new_columns_to_read.push_back(handle);
    new_columns_to_read.push_back(VERSION_COLUMN_DEFINE);
    if constexpr (add_tag_column)
        new_columns_to_read.push_back(TAG_COLUMN_DEFINE);

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
BlockInputStreamPtr Segment::getPlacedStream(const DMContext &          dm_context,
                                             const HandleRanges &       read_ranges,
                                             const ColumnDefines &      read_columns,
                                             const DeltaValueSpacePtr & delta_value_space,
                                             const IndexIterator &      delta_index_begin,
                                             const IndexIterator &      delta_index_end,
                                             size_t                     expected_block_size) const
{
    auto placed_stream_creator = [&](const HandleRange & read_range) {
        auto stable_input_stream = stable.getInputStream(read_columns, dm_context.storage_pool.data());
        return std::make_shared<DeltaMergeBlockInputStream<DeltaValueSpace, IndexIterator>>( //
            0,
            read_range,
            stable_input_stream,
            delta_value_space,
            delta_index_begin,
            delta_index_end,
            expected_block_size);
    };


    if (read_ranges.size() == 1)
    {
        LOG_DEBUG(log, "Segment[" + DB::toString(segment_id) + "] is read by " + DB::toString(1) + " ranges");
        return placed_stream_creator(read_ranges[0]);
    }
    else
    {
        BlockInputStreams streams;
        for (auto & read_range : read_ranges)
        {
            HandleRange real_range = range.shrink(read_range);
            if (!real_range.none())
                streams.push_back(placed_stream_creator(real_range));
        }

        LOG_DEBUG(log, "Segment[" + DB::toString(segment_id) + "] is read by " + DB::toString(streams.size()) + " ranges");

        return std::make_shared<ConcatBlockInputStream>(streams);
    }
}


SegmentPair Segment::doSplit(DMContext & dm_context, const ReadSnapshot & snapshot, Handle split_point) const
{
    EventRecorder recorder(ProfileEvents::DMSegmentSplit, ProfileEvents::DMSegmentSplitNS);

    auto & handle       = dm_context.table_handle_define;
    auto & storage_pool = dm_context.storage_pool;
    auto   min_version  = dm_context.min_version;

    HandleRange my_range    = {range.start, split_point};
    HandleRange other_range = {split_point, range.end};

    Chunks my_new_stable_chunks;
    Chunks other_new_stable_chunks;

    OpContext opc = OpContext::createForDataStorage(dm_context);
    {
        // Write my data
        BlockInputStreamPtr my_data = getPlacedStream(dm_context,
                                                      {my_range},
                                                      snapshot.read_columns,
                                                      snapshot.delta_value_space,
                                                      snapshot.index_begin,
                                                      snapshot.index_end,
                                                      STABLE_CHUNK_ROWS);
        my_data  = std::make_shared<DMVersionFilterBlockInputStream<DM_VESION_FILTER_MODE_COMPACT>>(my_data, handle, min_version);
        auto tmp = DiskValueSpace::writeChunks(opc, my_data);
        my_new_stable_chunks.swap(tmp);
    }

    {
        // Write new segment's data
        BlockInputStreamPtr other_data = getPlacedStream(dm_context,
                                                         {other_range},
                                                         snapshot.read_columns,
                                                         snapshot.delta_value_space,
                                                         snapshot.index_begin,
                                                         snapshot.index_end,
                                                         STABLE_CHUNK_ROWS);
        other_data = std::make_shared<DMVersionFilterBlockInputStream<DM_VESION_FILTER_MODE_COMPACT>>(other_data, handle, min_version);
        auto tmp   = DiskValueSpace::writeChunks(opc, other_data);
        other_new_stable_chunks.swap(tmp);
    }

    auto other_segment_id = storage_pool.newMetaPageId();
    auto other_delta_id   = storage_pool.newMetaPageId();
    auto other_stable_id  = storage_pool.newMetaPageId();

    auto new_me = std::make_shared<Segment>(this->epoch + 1, //
                                            my_range,
                                            this->segment_id,
                                            other_segment_id,
                                            this->delta.pageId(),
                                            this->delta.getChunks(),
                                            this->stable.pageId(),
                                            this->stable.getChunks());

    auto other = std::make_shared<Segment>(INITIAL_EPOCH, //
                                           other_range,
                                           other_segment_id,
                                           this->next_segment_id,
                                           other_delta_id,
                                           other_stable_id);

    WriteBatch meta_wb;
    WriteBatch log_wb;
    WriteBatch data_wb;

    new_me->serialize(meta_wb);
    new_me->delta.setChunks({}, meta_wb, log_wb);
    new_me->stable.setChunks(std::move(my_new_stable_chunks), meta_wb, data_wb);

    other->serialize(meta_wb);
    other->delta.setChunks({}, meta_wb, log_wb);
    other->stable.setChunks(std::move(other_new_stable_chunks), meta_wb, data_wb);

    // Commit meta.
    storage_pool.meta().write(meta_wb);

    // Remove old chunks.
    storage_pool.log().write(log_wb);
    storage_pool.data().write(data_wb);

    recorder.submit();

    return {new_me, other};
}

SegmentPtr Segment::doMerge(DMContext &          dm_context,
                            const SegmentPtr &   left,
                            const ReadSnapshot & left_snapshot,
                            const SegmentPtr &   right,
                            const ReadSnapshot & right_snapshot)
{
    if (left->range.end != right->range.start || left->next_segment_id != right->segment_id)
        throw Exception("The ranges of merge segments are not consecutive: first end: " + DB::toString(left->range.end)
                        + ", second start: " + DB::toString(right->range.start));

    EventRecorder recorder(ProfileEvents::DMSegmentMerge, ProfileEvents::DMSegmentMergeNS);

    auto & handle       = dm_context.table_handle_define;
    auto & storage_pool = dm_context.storage_pool;
    auto   min_version  = dm_context.min_version;

    Chunks new_stable_chunks;
    {
        BlockInputStreamPtr left_data = left->getPlacedStream(dm_context,
                                                              {left->range},
                                                              left_snapshot.read_columns,
                                                              left_snapshot.delta_value_space,
                                                              left_snapshot.index_begin,
                                                              left_snapshot.index_end,
                                                              STABLE_CHUNK_ROWS);
        left_data = std::make_shared<DMVersionFilterBlockInputStream<DM_VESION_FILTER_MODE_COMPACT>>(left_data, handle, min_version);

        BlockInputStreamPtr right_data = right->getPlacedStream(dm_context,
                                                                {right->range},
                                                                right_snapshot.read_columns,
                                                                right_snapshot.delta_value_space,
                                                                right_snapshot.index_begin,
                                                                right_snapshot.index_end,
                                                                STABLE_CHUNK_ROWS);
        right_data = std::make_shared<DMVersionFilterBlockInputStream<DM_VESION_FILTER_MODE_COMPACT>>(right_data, handle, min_version);

        BlockInputStreamPtr merged_stream = std::make_shared<ConcatBlockInputStream>(BlockInputStreams({left_data, right_data}));

        OpContext opc = OpContext::createForDataStorage(dm_context);
        auto      tmp = DiskValueSpace::writeChunks(opc, merged_stream);
        new_stable_chunks.swap(tmp);
    }

    HandleRange merge_range = {left->range.start, right->range.end};


    auto merged = std::make_shared<Segment>(left->epoch + 1, //
                                            merge_range,
                                            left->segment_id,
                                            right->next_segment_id,
                                            left->delta.pageId(),
                                            left->delta.getChunks(),
                                            left->stable.pageId(),
                                            left->stable.getChunks());

    // right_copy is used to generate write batch. Because we cannot modify the content of original object.
    Segment right_copy(right->epoch,
                       right->range,
                       right->segment_id,
                       right->next_segment_id,
                       right->delta.pageId(),
                       right->delta.getChunks(),
                       right->stable.pageId(),
                       right->stable.getChunks());

    WriteBatch meta_wb;
    WriteBatch log_wb;
    WriteBatch data_wb;

    merged->serialize(meta_wb);
    merged->delta.setChunks({}, meta_wb, log_wb);
    merged->stable.setChunks(std::move(new_stable_chunks), meta_wb, data_wb);

    right_copy.delta.setChunks({}, meta_wb, log_wb);
    right_copy.stable.setChunks({}, meta_wb, data_wb);

    // Remove other's meta data.
    meta_wb.delPage(left->segment_id);
    meta_wb.delPage(left->delta.pageId());
    meta_wb.delPage(left->stable.pageId());

    // Commit meta updates.
    storage_pool.meta().write(meta_wb);

    // Remove old chunks.
    storage_pool.log().write(log_wb);
    storage_pool.data().write(data_wb);

    recorder.submit();

    return merged;
}

SegmentPtr Segment::reset(DMContext & dm_context, BlockInputStreamPtr & input_stream) const
{
    auto & storage_pool = dm_context.storage_pool;

    // Write new chunks.
    OpContext opc               = OpContext::createForDataStorage(dm_context);
    Chunks    new_stable_chunks = DiskValueSpace::writeChunks(opc, input_stream);

    auto new_me = std::make_shared<Segment>(epoch, //
                                            range,
                                            segment_id,
                                            next_segment_id,
                                            delta.pageId(),
                                            delta.getChunks(),
                                            stable.pageId(),
                                            stable.getChunks());

    WriteBatch meta_wb;
    WriteBatch log_wb;
    WriteBatch data_wb;

    // The order of following code is critical.

    new_me->delta.setChunks({}, meta_wb, log_wb);
    new_me->stable.setChunks(std::move(new_stable_chunks), meta_wb, data_wb);

    // Commit meta updates.
    storage_pool.meta().write(meta_wb);

    // Remove old chunks.
    storage_pool.log().write(log_wb);
    storage_pool.data().write(data_wb);

    return new_me;
}

bool Segment::shouldFlush(DMContext & dm_context, bool force)
{
    return force || delta.num_rows() >= dm_context.delta_limit_rows || delta.num_bytes() >= dm_context.delta_limit_bytes;
}

SegmentPtr Segment::flush(DMContext & dm_context)
{
    EventRecorder recorder(ProfileEvents::DMDeltaMerge, ProfileEvents::DMDeltaMergeNS);

    LOG_DEBUG(log, "Segment [" + DB::toString(segment_id) + "] start to merge delta.");

    auto & handle      = dm_context.table_handle_define;
    auto & columns     = dm_context.table_columns;
    auto   min_version = dm_context.min_version;

    auto snapshot    = getReadSnapshot<false>(dm_context, columns);
    auto data_stream = getPlacedStream(dm_context,
                                       {range},
                                       snapshot.read_columns,
                                       snapshot.delta_value_space,
                                       snapshot.index_begin,
                                       snapshot.index_end,
                                       STABLE_CHUNK_ROWS);
    data_stream      = std::make_shared<DMVersionFilterBlockInputStream<DM_VESION_FILTER_MODE_COMPACT>>(data_stream, handle, min_version);

    SegmentPtr new_me = reset(dm_context, data_stream);

    // Force tcmalloc to return memory back to system.
    // https://internal.pingcap.net/jira/browse/FLASH-41
    // TODO: Evaluate the cost of this.
    //    MallocExtension::instance()->ReleaseFreeMemory();

    LOG_DEBUG(log, "Segment [" + DB::toString(segment_id) + "] done merge delta.");

    recorder.submit();

    return new_me;
}

DeltaIndexPtr
Segment::ensurePlace(const DMContext & dm_context, const DiskValueSpacePtr & to_place_delta, const DeltaValueSpacePtr & delta_value_space)
{
    EventRecorder recorder(ProfileEvents::DMPlace, ProfileEvents::DMPlaceNS);

    auto & handle  = dm_context.table_handle_define;
    auto & storage = dm_context.storage_pool;

    size_t delta_rows    = to_place_delta->num_rows();
    size_t delta_deletes = to_place_delta->num_deletes();
    if (placed_delta_rows == delta_rows && placed_delta_deletes == delta_deletes)
        return delta_tree->getEntriesCopy<Allocator<false>>();

    auto blocks = to_place_delta->getMergeBlocks(handle, storage.log(), placed_delta_rows, placed_delta_deletes, delta_rows, delta_deletes);

    for (auto & v : blocks)
    {
        if (!v.delete_range.none())
            placeDelete(dm_context, delta_value_space, v.delete_range);
        else if (v.block)
            placeUpsert(dm_context, delta_value_space, std::move(v.block));
    }

    recorder.submit();

    return delta_tree->getEntriesCopy<Allocator<false>>();
}

void Segment::placeUpsert(const DMContext & dm_context, const DeltaValueSpacePtr & delta_value_space, Block && block)
{
    EventRecorder recorder(ProfileEvents::DMPlaceUpsert, ProfileEvents::DMPlaceUpsertNS);

    auto & handle            = dm_context.table_handle_define;
    auto   delta_index_begin = delta_tree->begin();
    auto   delta_index_end   = delta_tree->end();

    BlockInputStreamPtr merged_stream = getPlacedStream<DefaultDeltaTree::EntryIterator>( //
        dm_context,
        {range},
        {handle, VERSION_COLUMN_DEFINE},
        delta_value_space,
        delta_index_begin,
        delta_index_end,
        DEFAULT_BLOCK_SIZE);

    auto perm = sortBlockByPk(handle, block);
    DM::placeInsert(merged_stream, block, *delta_tree, placed_delta_rows, perm, getPkSort(handle));
    placed_delta_rows += block.rows();

    recorder.submit();
}

void Segment::placeDelete(const DMContext & dm_context, const DeltaValueSpacePtr & delta_value_space, const HandleRange & delete_range)
{
    EventRecorder recorder(ProfileEvents::DMPlaceDeleteRange, ProfileEvents::DMPlaceDeleteRangeNS);

    auto & handle            = dm_context.table_handle_define;
    auto   delta_index_begin = delta_tree->begin();
    auto   delta_index_end   = delta_tree->end();

    Blocks delete_data;
    {
        BlockInputStreamPtr delete_stream = getPlacedStream<DefaultDeltaTree::EntryIterator>( //
            dm_context,
            {delete_range},
            {handle, VERSION_COLUMN_DEFINE},
            delta_value_space,
            delta_index_begin,
            delta_index_end,
            DEFAULT_BLOCK_SIZE);
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
            dm_context,
            {range},
            {handle, VERSION_COLUMN_DEFINE},
            delta_value_space,
            delta_index_begin,
            delta_index_end,
            DEFAULT_BLOCK_SIZE);
        DM::placeDelete(merged_stream, block, *delta_tree, getPkSort(handle));
    }
    ++placed_delta_deletes;

    recorder.submit();
}

Handle Segment::getSplitPoint(DMContext & dm_context, const ReadSnapshot & snapshot)
{
    EventRecorder recorder(ProfileEvents::DMSegmentGetSplitPoint, ProfileEvents::DMSegmentGetSplitPointNS);

    auto & handle = dm_context.table_handle_define;
    auto   stream = getPlacedStream(dm_context,
                                  {range},
                                  {dm_context.table_handle_define},
                                  snapshot.delta_value_space,
                                  snapshot.index_begin,
                                  snapshot.index_end,
                                  DEFAULT_BLOCK_SIZE);

    stream->readPrefix();
    size_t split_row_index = estimatedRows() / 2;
    Handle split_handle    = 0;
    size_t count           = 0;
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

    recorder.submit();

    return split_handle;
}

size_t Segment::estimatedRows()
{
    // Not 100% accurate.
    return stable.num_rows() + delta_tree->numInserts() - delta_tree->numDeletes() + (delta.num_rows() - placed_delta_rows);
}

size_t Segment::estimatedBytes()
{
    size_t stable_bytes = stable.num_bytes();
    return stable_bytes + delta.num_bytes() - (stable_bytes / stable.num_rows()) * delta_tree->numDeletes();
}

} // namespace DM
} // namespace DB