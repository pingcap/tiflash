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
      delta(std::make_shared<DiskValueSpace>(true, delta_id, delta_chunks_)),
      stable(std::make_shared<DiskValueSpace>(false, stable_id, stable_chunks_)),
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
    segment->delta->setChunks({}, meta_wb, log_wb);
    segment->stable->setChunks({}, meta_wb, data_wb);

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
    auto               op_context = OpContext::createForLogStorage(dm_context);
    AppendWriteBatches wbs;

    auto task = createAppendTask(op_context, wbs, update);
    {
        EventRecorder recorder(ProfileEvents::DMAppendDeltaCommitDisk, ProfileEvents::DMAppendDeltaCommitDiskNS);
        dm_context.storage_pool.log().write(wbs.data);
        dm_context.storage_pool.meta().write(wbs.meta);
    }

    applyAppendTask(op_context, task, update);

    {
        EventRecorder recorder(ProfileEvents::DMAppendDeltaCleanUp, ProfileEvents::DMAppendDeltaCleanUpNS);
        dm_context.storage_pool.log().write(wbs.removed_data);
    }
}

AppendTaskPtr Segment::createAppendTask(const OpContext & opc, AppendWriteBatches & wbs, const BlockOrDelete & update)
{
    if (update.block)
        LOG_DEBUG(log, "Segment [" << DB::toString(segment_id) << "] write rows: " << DB::toString(update.block.rows()));
    else
        LOG_DEBUG(log, "Segment [" << DB::toString(segment_id) << "] delete range: " << update.delete_range.toString());

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
    if (new_delta)
    {
        LOG_DEBUG(log, "Segment [" << DB::toString(segment_id) << "] update delta instance");
        delta = new_delta;
    }
}

void Segment::check(DMContext & dm_context, const String & when) const
{
    // This method is broken.
    if (true)
        return;
    auto & handle = dm_context.handle_column;

    size_t stable_rows = stable->num_rows();
    size_t delta_rows  = delta->num_rows();

    LOG_DEBUG(log, when << ": stable_rows:" << DB::toString(stable_rows) << ", delta_rows:" << DB::toString(delta_rows));

    StorageSnapshot storage_snapshot(dm_context.storage_pool);
    auto            read_info = getReadInfo<false>(dm_context, {delta, delta_rows, delta->num_deletes()}, storage_snapshot, {handle});

    LOG_DEBUG(log,
              when + ": entries:" << DB::toString(delta_tree->numEntries()) << ", inserts:"
                                  << DB::toString(delta_tree->numInserts()) + ", deletes:" << DB::toString(delta_tree->numDeletes()));

    auto stream = getPlacedStream(storage_snapshot.data_reader,
                                  read_info.read_columns,
                                  EMPTY_FILTER,
                                  read_info.delta_value_space,
                                  read_info.index_begin,
                                  read_info.index_end,
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

    LOG_DEBUG(log, when << ": rows(raw): " << DB::toString(total_rows));
}

SegmentSnapshot Segment::getReadSnapshot() const
{
    std::shared_lock lock(read_write_mutex);
    return {delta, delta->num_rows(), delta->num_deletes()};
}

BlockInputStreamPtr Segment::getInputStream(const DMContext &       dm_context,
                                            const SegmentSnapshot & segment_snap,
                                            const StorageSnapshot & storage_snaps,
                                            const ColumnDefines &   columns_to_read,
                                            const HandleRanges &    read_ranges,
                                            const RSOperatorPtr &   filter,
                                            UInt64                  max_version,
                                            size_t                  expected_block_size)
{
    auto & handle    = dm_context.handle_column;
    auto   read_info = getReadInfo<true>(dm_context, segment_snap, storage_snaps, columns_to_read);

    auto create_stream = [&](const HandleRange & read_range) {
        auto stream = getPlacedStream(storage_snaps.data_reader,
                                      read_info.read_columns,
                                      filter,
                                      read_info.delta_value_space,
                                      read_info.index_begin,
                                      read_info.index_end,
                                      expected_block_size);
        stream      = std::make_shared<DMHandleFilterBlockInputStream<true>>(stream, read_range, 0);
        return std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_MVCC>>(stream, handle, max_version);
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

BlockInputStreamPtr Segment::getInputStreamRaw(const DMContext &       dm_context,
                                               const SegmentSnapshot & segment_snap,
                                               const StorageSnapshot & storage_snaps,
                                               const ColumnDefines &   columns_to_read)
{
    auto & handle = dm_context.handle_column;

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
        std::shared_lock lock(read_write_mutex);
        delta_snap = std::make_shared<DiskValueSpace>(*segment_snap.delta);
    }

    BlockInputStreamPtr delta_stream = std::make_shared<ChunkBlockInputStream>(delta_snap->getChunks(), //
                                                                               new_columns_to_read,
                                                                               storage_snaps.log_reader,
                                                                               EMPTY_FILTER);
    delta_stream                     = std::make_shared<DMHandleFilterBlockInputStream<false>>(delta_stream, range, 0);

    BlockInputStreamPtr stable_stream = std::make_shared<ChunkBlockInputStream>(stable->getChunks(), //
                                                                                new_columns_to_read,
                                                                                storage_snaps.data_reader,
                                                                                EMPTY_FILTER);
    stable_stream                     = std::make_shared<DMHandleFilterBlockInputStream<true>>(stable_stream, range, 0);

    BlockInputStreams streams;
    streams.push_back(delta_stream);
    streams.push_back(stable_stream);
    return std::make_shared<ConcatBlockInputStream>(streams);
}

SegmentPair Segment::split(DMContext & dm_context, RemoveWriteBatches & remove_wbs) const
{
    /// Currently split & merge are done after update segment, so snapshot is not needed.
    LOG_DEBUG(log, "Segment [" << DB::toString(segment_id) << "] start to split, estimated rows[" << DB::toString(estimatedRows()) << "]");

    StorageSnapshot storage_snapshot(dm_context.storage_pool);
    SegmentPair     res;

    Handle            split_point = MIN_INT64;
    Segment::ReadInfo read_info;

    // Otherwise the split could become too unequal.
    if (stable->num_rows() > delta->num_rows())
        split_point = getSplitPointFast(dm_context, storage_snapshot.data_reader);

    if (split_point == MIN_INT64 || split_point == range.start || split_point == range.end)
    {
        // Fallback to slow version.
        read_info   = getReadInfo<false>(dm_context, //
                                       {delta, delta->num_rows(), delta->num_deletes()},
                                       storage_snapshot,
                                       dm_context.store_columns);
        split_point = getSplitPointSlow(dm_context, storage_snapshot.data_reader, read_info);
    }

    if (stable->num_rows() > 0)
    {
        res = doRefSplit(dm_context, split_point, remove_wbs);
    }
    else
    {
        if (!read_info)
        {
            read_info = getReadInfo<false>(dm_context, //
                                           {delta, delta->num_rows(), delta->num_deletes()},
                                           storage_snapshot,
                                           dm_context.store_columns);
        }
        res = doSplit(dm_context, storage_snapshot.data_reader, read_info, split_point, remove_wbs);
    }
    LOG_DEBUG(log, "Segment [" << segment_id << "] split into " << res.first->info() << " and " << res.second->info());

    return res;
}

SegmentPtr Segment::merge(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right, RemoveWriteBatches & remove_wbs)
{
    LOG_DEBUG(left->log, "Merge segment [" << DB::toString(left->segment_id) << "] with [" << DB::toString(right->segment_id) << "]");

    //    StorageSnapshot storage_snapshot(dm_context.storage_pool);
    //
    //    auto left_read_info = left->getReadInfo<false>(
    //        dm_context, {left->delta, left->delta->num_rows(), left->delta->num_deletes()}, storage_snapshot, dm_context.store_columns);
    //    auto right_read_info = right->getReadInfo<false>(
    //        dm_context, {right->delta, right->delta->num_rows(), left->delta->num_deletes()}, storage_snapshot, dm_context.store_columns);
    //
    //    auto res = doMerge(dm_context, storage_snapshot.data_reader, left, left_read_info, right, right_read_info, remove_wbs);

    auto res = doMergeFast(dm_context, left, right, remove_wbs);

    LOG_DEBUG(left->log, "Done merge segment [" << DB::toString(left->segment_id) << "] with [" << DB::toString(right->segment_id) << "]");

    return res;
}


bool Segment::shouldFlushDelta(DMContext & dm_context) const
{
    return delta->num_rows() >= dm_context.delta_limit_rows || delta->num_bytes() >= dm_context.delta_limit_bytes;
}

SegmentPtr Segment::flushDelta(DMContext & dm_context, RemoveWriteBatches & remove_wbs) const
{
    EventRecorder recorder(ProfileEvents::DMDeltaMerge, ProfileEvents::DMDeltaMergeNS);

    LOG_DEBUG(log,
              "Segment [" << DB::toString(segment_id) << "] start to merge delta. delta chunks: " << DB::toString(delta->num_chunks())
                          << ", rows: " << DB::toString(delta->num_rows()));

    auto & handle      = dm_context.handle_column;
    auto & columns     = dm_context.store_columns;
    auto   min_version = dm_context.min_version;

    StorageSnapshot storage_snapshot(dm_context.storage_pool);

    auto read_info   = getReadInfo<false>(dm_context, {delta, delta->num_rows(), delta->num_deletes()}, storage_snapshot, columns);
    auto data_stream = getPlacedStream(storage_snapshot.data_reader,
                                       read_info.read_columns,
                                       EMPTY_FILTER,
                                       read_info.delta_value_space,
                                       read_info.index_begin,
                                       read_info.index_end,
                                       STABLE_CHUNK_ROWS);
    data_stream      = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(data_stream, handle, min_version);

    SegmentPtr new_me = reset(dm_context, data_stream, remove_wbs);

    // Force tcmalloc to return memory back to system.
    // https://internal.pingcap.net/jira/browse/FLASH-41
    // TODO: Evaluate the cost of this.
    //    MallocExtension::instance()->ReleaseFreeMemory();

    LOG_DEBUG(log, "Segment [" << DB::toString(segment_id) << "] done merge delta.");

    recorder.submit();

    return new_me;
}

void Segment::flushCache(DMContext & dm_context, WriteBatch & remove_log_wb)
{
    std::unique_lock lock(read_write_mutex);

    auto new_delta = delta->tryFlushCache(OpContext::createForLogStorage(dm_context), remove_log_wb, /* force= */ true);
    if (new_delta)
    {
        LOG_DEBUG(log, "Segment [" << DB::toString(segment_id) << "] update delta instance");

        delta = new_delta;
    }
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

size_t Segment::delta_rows() const
{
    std::shared_lock lock(read_write_mutex);
    return delta->num_rows();
}

size_t Segment::delta_deletes() const
{
    std::shared_lock lock(read_write_mutex);
    return delta->num_deletes();
}

//==========================================================================================
// Segment private methods.
//==========================================================================================

template <bool add_tag_column>
Segment::ReadInfo Segment::getReadInfo(const DMContext &       dm_context,
                                       const SegmentSnapshot & segment_snap,
                                       const StorageSnapshot & storage_snaps,
                                       const ColumnDefines &   columns_to_read) const
{
    auto new_columns_to_read = arrangeReadColumns<add_tag_column>(dm_context.handle_column, columns_to_read);

    // Create a new delta vs and delta index snapshot, so that later read/write operations won't block write thread.
    // TODO: We don't need to do copy if chunks in DiskValueSpace is a list.
    DiskValueSpacePtr delta_snap;
    {
        // Synchronize between read/write threads.
        std::shared_lock lock(read_write_mutex);
        delta_snap = std::make_shared<DiskValueSpace>(*segment_snap.delta);
    }

    auto & handle = dm_context.handle_column;

    const auto delta_block       = delta_snap->read(new_columns_to_read, storage_snaps.log_reader, 0, segment_snap.delta_rows);
    auto       delta_value_space = std::make_shared<DeltaValueSpace>(handle, new_columns_to_read, delta_block);

    DeltaIndexPtr delta_index = ensurePlace(dm_context, //
                                            storage_snaps,
                                            delta_snap,
                                            segment_snap.delta_rows,
                                            segment_snap.delta_deletes,
                                            delta_value_space);

    auto index_begin = DeltaIndex::begin(delta_index);
    auto index_end   = DeltaIndex::end(delta_index);

    return {
        .delta_value_space = delta_value_space,
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
                                             const DeltaValueSpacePtr & delta_value_space,
                                             const IndexIterator &      delta_index_begin,
                                             const IndexIterator &      delta_index_end,
                                             size_t                     expected_block_size) const
{

    auto stable_input_stream = std::make_shared<ChunkBlockInputStream>(stable->getChunks(), read_columns, data_page_reader, filter);
    return std::make_shared<DeltaMergeBlockInputStream<DeltaValueSpace, IndexIterator>>( //
        stable_input_stream,
        delta_value_space,
        delta_index_begin,
        delta_index_end,
        expected_block_size);
}

Handle Segment::getSplitPointFast(DMContext & dm_context, const PageReader & data_page_reader) const
{
    EventRecorder recorder(ProfileEvents::DMSegmentGetSplitPoint, ProfileEvents::DMSegmentGetSplitPointNS);

    auto & chunks = stable->getChunks();
    if (unlikely(chunks.empty()))
        throw Exception("getSplitPointFast can only works on stable value space");
    size_t split_row_index = stable->num_rows() / 2;
    auto   block           = stable->read({dm_context.handle_column}, data_page_reader, split_row_index, 1);
    return block.getByPosition(0).column->getInt(0);
}

Handle Segment::getSplitPointSlow(DMContext & dm_context, const PageReader & data_page_reader, const ReadInfo & read_info) const
{
    EventRecorder recorder(ProfileEvents::DMSegmentGetSplitPoint, ProfileEvents::DMSegmentGetSplitPointNS);

    auto & handle = dm_context.handle_column;
    auto   stream = getPlacedStream(data_page_reader,
                                  {dm_context.handle_column},
                                  EMPTY_FILTER,
                                  read_info.delta_value_space,
                                  read_info.index_begin,
                                  read_info.index_end,
                                  DEFAULT_BLOCK_SIZE);

    size_t split_row_index = estimatedRows() / 2;
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

    recorder.submit();

    return split_handle;
}

SegmentPair Segment::doRefSplit(DMContext & dm_context, Handle split_point, RemoveWriteBatches & remove_wbs) const
{
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
                                            this->delta->pageId(),
                                            this->delta->getChunks(),
                                            this->stable->pageId(),
                                            this->stable->getChunks());

    auto other = std::make_shared<Segment>(INITIAL_EPOCH, //
                                           other_range,
                                           other_segment_id,
                                           this->next_segment_id,
                                           other_delta_id,
                                           other_stable_id);

    WriteBatch meta_wb;
    WriteBatch log_wb;
    WriteBatch data_wb;


    GenPageId log_gen_page_id = std::bind(&StoragePool::newLogPageId, &storage_pool);

    Chunks my_delta_chunks = createRefChunks(this->delta->getChunks(), log_gen_page_id, log_wb);
    Chunks my_stable_chunks;

    Chunks other_delta_chunks = createRefChunks(this->delta->getChunks(), log_gen_page_id, log_wb);
    Chunks other_stable_chunks;

    GenPageId data_gen_page_id = std::bind(&StoragePool::newDataPageId, &storage_pool);
    for (auto & chunk : this->stable->getChunks())
    {
        auto [handle_first, handle_last] = chunk.getHandleFirstLast();
        if (my_range.intersect(handle_first, handle_last))
            my_stable_chunks.push_back(createRefChunk(chunk, data_gen_page_id, data_wb));
        if (other_range.intersect(handle_first, handle_last))
            other_stable_chunks.push_back(createRefChunk(chunk, data_gen_page_id, data_wb));
    }

    new_me->serialize(meta_wb);
    new_me->delta->setChunks(std::move(my_delta_chunks), meta_wb, remove_wbs.log);
    new_me->stable->setChunks(std::move(my_stable_chunks), meta_wb, remove_wbs.data);

    other->serialize(meta_wb);
    other->delta->setChunks(std::move(other_delta_chunks), meta_wb, remove_wbs.log);
    other->stable->setChunks(std::move(other_stable_chunks), meta_wb, remove_wbs.data);

    // Commit.
    storage_pool.log().write(log_wb);
    storage_pool.data().write(data_wb);
    storage_pool.meta().write(meta_wb);

    return {new_me, other};
}

SegmentPair Segment::doSplit(DMContext &          dm_context,
                             const PageReader &   data_page_reader,
                             const ReadInfo &     read_info,
                             Handle               split_point,
                             RemoveWriteBatches & remove_wbs) const
{
    EventRecorder recorder(ProfileEvents::DMSegmentSplit, ProfileEvents::DMSegmentSplitNS);

    auto & handle       = dm_context.handle_column;
    auto & storage_pool = dm_context.storage_pool;
    auto   min_version  = dm_context.min_version;

    HandleRange my_range    = {range.start, split_point};
    HandleRange other_range = {split_point, range.end};

    Chunks my_new_stable_chunks;
    Chunks other_new_stable_chunks;

    OpContext opc = OpContext::createForDataStorage(dm_context);
    {
        // Write my data
        BlockInputStreamPtr my_data = getPlacedStream(data_page_reader,
                                                      read_info.read_columns,
                                                      EMPTY_FILTER,
                                                      read_info.delta_value_space,
                                                      read_info.index_begin,
                                                      read_info.index_end,
                                                      STABLE_CHUNK_ROWS);
        my_data                     = std::make_shared<DMHandleFilterBlockInputStream<true>>(my_data, my_range, 0);
        my_data  = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(my_data, handle, min_version);
        auto tmp = DiskValueSpace::writeChunks(opc, my_data);
        my_new_stable_chunks.swap(tmp);
    }

    {
        // Write new segment's data
        BlockInputStreamPtr other_data = getPlacedStream(data_page_reader,
                                                         read_info.read_columns,
                                                         EMPTY_FILTER,
                                                         read_info.delta_value_space,
                                                         read_info.index_begin,
                                                         read_info.index_end,
                                                         STABLE_CHUNK_ROWS);
        other_data                     = std::make_shared<DMHandleFilterBlockInputStream<true>>(other_data, other_range, 0);
        other_data = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(other_data, handle, min_version);
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
                                            this->delta->pageId(),
                                            this->delta->getChunks(),
                                            this->stable->pageId(),
                                            this->stable->getChunks());

    auto other = std::make_shared<Segment>(INITIAL_EPOCH, //
                                           other_range,
                                           other_segment_id,
                                           this->next_segment_id,
                                           other_delta_id,
                                           other_stable_id);

    WriteBatch meta_wb;

    new_me->serialize(meta_wb);
    new_me->delta->setChunks({}, meta_wb, remove_wbs.log);
    new_me->stable->setChunks(std::move(my_new_stable_chunks), meta_wb, remove_wbs.data);

    other->serialize(meta_wb);
    other->delta->setChunks({}, meta_wb, remove_wbs.log);
    other->stable->setChunks(std::move(other_new_stable_chunks), meta_wb, remove_wbs.data);

    // Commit meta.
    storage_pool.meta().write(meta_wb);

    return {new_me, other};
}

SegmentPtr Segment::doMergeFast(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right, RemoveWriteBatches & remove_wbs)
{
    if (unlikely(left->range.end != right->range.start || left->next_segment_id != right->segment_id))
        throw Exception("The ranges of merge segments are not consecutive: first end: " + DB::toString(left->range.end)
                        + ", second start: " + DB::toString(right->range.start));

    EventRecorder recorder(ProfileEvents::DMSegmentMerge, ProfileEvents::DMSegmentMergeNS);

    auto &      storage_pool = dm_context.storage_pool;
    HandleRange merge_range  = {left->range.start, right->range.end};

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

    WriteBatch meta_wb;

    merged->serialize(meta_wb);
    merged->delta->setChunks(std::move(merged_delta_chunks), meta_wb, remove_wbs.log);
    merged->stable->setChunks(std::move(merged_stable_chunks), meta_wb, remove_wbs.data);

    // Remove right's meta data.
    remove_wbs.meta.delPage(right->segment_id);
    remove_wbs.meta.delPage(right->delta->pageId());
    remove_wbs.meta.delPage(right->stable->pageId());

    // Commit meta updates.
    storage_pool.meta().write(meta_wb);

    return merged;
}

SegmentPtr Segment::doMerge(DMContext &          dm_context,
                            const PageReader &   data_page_reader,
                            const SegmentPtr &   left,
                            const ReadInfo &     left_read_info,
                            const SegmentPtr &   right,
                            const ReadInfo &     right_read_info,
                            RemoveWriteBatches & remove_wbs)
{
    if (left->range.end != right->range.start || left->next_segment_id != right->segment_id)
        throw Exception("The ranges of merge segments are not consecutive: first end: " + DB::toString(left->range.end)
                        + ", second start: " + DB::toString(right->range.start));

    EventRecorder recorder(ProfileEvents::DMSegmentMerge, ProfileEvents::DMSegmentMergeNS);

    auto & handle       = dm_context.handle_column;
    auto & storage_pool = dm_context.storage_pool;
    auto   min_version  = dm_context.min_version;

    Chunks new_stable_chunks;
    {
        BlockInputStreamPtr left_data = left->getPlacedStream(data_page_reader,
                                                              left_read_info.read_columns,
                                                              EMPTY_FILTER,
                                                              left_read_info.delta_value_space,
                                                              left_read_info.index_begin,
                                                              left_read_info.index_end,
                                                              STABLE_CHUNK_ROWS);
        left_data                     = std::make_shared<DMHandleFilterBlockInputStream<true>>(left_data, left->range, 0);
        left_data = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(left_data, handle, min_version);

        BlockInputStreamPtr right_data = right->getPlacedStream(data_page_reader,
                                                                right_read_info.read_columns,
                                                                EMPTY_FILTER,
                                                                right_read_info.delta_value_space,
                                                                right_read_info.index_begin,
                                                                right_read_info.index_end,
                                                                STABLE_CHUNK_ROWS);
        right_data                     = std::make_shared<DMHandleFilterBlockInputStream<true>>(right_data, right->range, 0);
        right_data = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(right_data, handle, min_version);

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
                                            left->delta->pageId(),
                                            left->delta->getChunks(),
                                            left->stable->pageId(),
                                            left->stable->getChunks());

    // right_copy is used to generate write batch. Because we cannot modify the content of original object.
    Segment right_copy(right->epoch,
                       right->range,
                       right->segment_id,
                       right->next_segment_id,
                       right->delta->pageId(),
                       right->delta->getChunks(),
                       right->stable->pageId(),
                       right->stable->getChunks());

    WriteBatch meta_wb;

    merged->serialize(meta_wb);
    merged->delta->setChunks({}, meta_wb, remove_wbs.log);
    merged->stable->setChunks(std::move(new_stable_chunks), meta_wb, remove_wbs.data);

    right_copy.delta->setChunks({}, meta_wb, remove_wbs.log);
    right_copy.stable->setChunks({}, meta_wb, remove_wbs.data);

    // Remove right's meta data.
    remove_wbs.meta.delPage(right->segment_id);
    remove_wbs.meta.delPage(right->delta->pageId());
    remove_wbs.meta.delPage(right->stable->pageId());

    // Commit meta updates.
    storage_pool.meta().write(meta_wb);

    recorder.submit();

    return merged;
}

SegmentPtr Segment::reset(DMContext & dm_context, BlockInputStreamPtr & input_stream, RemoveWriteBatches & remove_wbs) const
{
    auto & storage_pool = dm_context.storage_pool;

    // Write new chunks.
    OpContext opc               = OpContext::createForDataStorage(dm_context);
    Chunks    new_stable_chunks = DiskValueSpace::writeChunks(opc, input_stream);

    auto new_me = std::make_shared<Segment>(epoch, //
                                            range,
                                            segment_id,
                                            next_segment_id,
                                            delta->pageId(),
                                            delta->getChunks(),
                                            stable->pageId(),
                                            stable->getChunks());

    WriteBatch meta_wb;

    new_me->delta->setChunks({}, meta_wb, remove_wbs.log);
    new_me->stable->setChunks(std::move(new_stable_chunks), meta_wb, remove_wbs.data);

    // Commit updates.
    storage_pool.meta().write(meta_wb);

    return new_me;
}

DeltaIndexPtr Segment::ensurePlace(const DMContext &          dm_context,
                                   const StorageSnapshot &    storage_snapshot,
                                   const DiskValueSpacePtr &  to_place_delta,
                                   size_t                     delta_rows_limit,
                                   size_t                     delta_deletes_limit,
                                   const DeltaValueSpacePtr & delta_value_space) const
{
    // Synchronize between read/read threads.
    std::unique_lock lock(read_read_mutex);

    // Other read threads could already done place.
    if (placed_delta_rows >= delta_rows_limit && placed_delta_deletes >= delta_deletes_limit)
        return delta_tree->getEntriesCopy<Allocator<false>>(delta_rows_limit, delta_deletes_limit);

    EventRecorder recorder(ProfileEvents::DMPlace, ProfileEvents::DMPlaceNS);

    auto blocks = to_place_delta->getMergeBlocks(dm_context.handle_column,
                                                 storage_snapshot.log_reader,
                                                 placed_delta_rows,
                                                 placed_delta_deletes,
                                                 delta_rows_limit,
                                                 delta_deletes_limit);

    for (auto & v : blocks)
    {
        if (!v.delete_range.none())
            placeDelete(dm_context, storage_snapshot.data_reader, delta_value_space, v.delete_range);
        else if (v.block)
            placeUpsert(dm_context, storage_snapshot.data_reader, delta_value_space, std::move(v.block));
    }

    return delta_tree->getEntriesCopy<Allocator<false>>(delta_rows_limit, delta_deletes_limit);
}

void Segment::placeUpsert(const DMContext &          dm_context,
                          const PageReader &         data_page_reader,
                          const DeltaValueSpacePtr & delta_value_space,
                          Block &&                   block) const
{
    EventRecorder recorder(ProfileEvents::DMPlaceUpsert, ProfileEvents::DMPlaceUpsertNS);

    auto & handle            = dm_context.handle_column;
    auto   delta_index_begin = delta_tree->begin();
    auto   delta_index_end   = delta_tree->end();

    BlockInputStreamPtr merged_stream = getPlacedStream<DefaultDeltaTree::EntryIterator>( //
        data_page_reader,
        {handle, getVersionColumnDefine()},
        EMPTY_FILTER,
        delta_value_space,
        delta_index_begin,
        delta_index_end,
        DEFAULT_BLOCK_SIZE);

    IColumn::Permutation perm;
    if (sortBlockByPk(handle, block, perm))
        DM::placeInsert<true>(merged_stream, block, *delta_tree, placed_delta_rows, perm, getPkSort(handle));
    else
        DM::placeInsert<false>(merged_stream, block, *delta_tree, placed_delta_rows, perm, getPkSort(handle));
    placed_delta_rows += block.rows();
}

void Segment::placeDelete(const DMContext &          dm_context,
                          const PageReader &         data_page_reader,
                          const DeltaValueSpacePtr & delta_value_space,
                          const HandleRange &        delete_range) const
{
    EventRecorder recorder(ProfileEvents::DMPlaceDeleteRange, ProfileEvents::DMPlaceDeleteRangeNS);

    auto & handle            = dm_context.handle_column;
    auto   delta_index_begin = delta_tree->begin();
    auto   delta_index_end   = delta_tree->end();

    Blocks delete_data;
    {
        BlockInputStreamPtr delete_stream = getPlacedStream<DefaultDeltaTree::EntryIterator>( //
            data_page_reader,
            {handle, getVersionColumnDefine()},
            withHanleRange(EMPTY_FILTER, delete_range),
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
            delta_value_space,
            delta_index_begin,
            delta_index_end,
            DEFAULT_BLOCK_SIZE);
        DM::placeDelete(merged_stream, block, *delta_tree, getPkSort(handle), placed_delta_deletes);
    }
    ++placed_delta_deletes;
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
    return stable->num_rows() + delta_tree->numInserts() - delta_tree->numDeletes() + (delta->num_rows() - placed_delta_rows);
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