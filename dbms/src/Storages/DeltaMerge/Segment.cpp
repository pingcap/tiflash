#include <gperftools/malloc_extension.h>

#include <numeric>

#include <DataTypes/DataTypeFactory.h>

#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>

#include <Storages/DeltaMerge/DMHandleFilterBlockInputStream.h>
#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMerge.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaPlace.h>
#include <Storages/DeltaMerge/Segment.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_FORMAT_VERSION;
} // namespace ErrorCodes


const Segment::Version Segment::CURRENT_VERSION = 1;
const static size_t    SEGMENT_BUFFER_SIZE      = 128; // More than enough.
const static size_t    STABLE_CHUNK_ROWS        = DEFAULT_BLOCK_SIZE;

struct DeltaValueSpace
{
    DeltaValueSpace(const ColumnDefines & column_defines, const Block & block)
    {
        columns.reserve(column_defines.size());
        for (const auto & c : column_defines)
        {
            auto & col = block.getByName(c.name).column;
            columns.emplace_back(col);
            columns_ptr.emplace_back(col.get());
        }
    }

    inline void insertValue(IColumn & des, size_t column_index, UInt64 value_id) //
    {
        des.insertFrom(*(columns_ptr[column_index]), value_id);
    }

    Columns       columns;
    ColumnRawPtrs columns_ptr;
};

using MyDeltaMergeBlockInputStream = DeltaMergeBlockInputStream<DefaultDeltaTree, DeltaValueSpace>;
using OpContext                    = DiskValueSpace::OpContext;

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

void Segment::write(DMContext & dm_context, Block && block)
{
    std::unique_lock lock(mutex);

    OpContext opc = OpContext::createForLogStorage(dm_context);

    Chunks chunks = DiskValueSpace::writeChunks(opc, std::make_shared<OneBlockInputStream>(block));
    for (auto & chunk : chunks)
        delta.appendChunkWithCache(opc, std::move(chunk), block);

    tryFlush(dm_context);
    if (delta.tryFlushCache(opc))
        ensurePlace(dm_context.table_handle_define, dm_context.storage_pool);
}

void Segment::deleteRange(DMContext & dm_context, const HandleRange & delete_range)
{
    std::unique_lock lock(mutex);

    OpContext opc   = OpContext::createForLogStorage(dm_context);
    Chunk     chunk = DiskValueSpace::writeDelete(opc, delete_range);
    delta.appendChunkWithCache(opc, std::move(chunk), {});

    placeDelete(dm_context.table_handle_define, dm_context.storage_pool, delete_range);

    // After delete range, we place all remaining deltas and deletes.
    ensurePlace(dm_context.table_handle_define, dm_context.storage_pool);

    tryFlush(dm_context);
    delta.tryFlushCache(opc);
}

BlockInputStreamPtr Segment::getInputStream(const DMContext &     dm_context, //
                                            const ColumnDefines & columns_to_read,
                                            size_t                expected_block_size,
                                            UInt64                max_version,
                                            bool                  is_raw)
{
    std::shared_lock lock(mutex);

    auto & handle  = dm_context.table_handle_define;
    auto & storage = dm_context.storage_pool;
    ensurePlace(handle, storage);

    auto stream = getPlacedStream(handle, columns_to_read, storage, expected_block_size, std::move(lock));
    if (!is_raw)
        stream = std::make_shared<DMVersionFilterBlockInputStream<DM_VESION_FILTER_MODE_MVCC>>(stream, handle, max_version);
    return stream;
}

SegmentPtr Segment::split(DMContext & dm_context)
{
    std::unique_lock lock(mutex);

    ensurePlace(dm_context.table_handle_define, dm_context.storage_pool);

    Handle split_point = getSplitPoint(dm_context);

    auto new_segment = doSplit(dm_context, split_point);

    MallocExtension::instance()->ReleaseFreeMemory();

    return new_segment;
}

void Segment::merge(DMContext & dm_context, const SegmentPtr & other)
{
    std::unique_lock lock(mutex);
    std::unique_lock lock2(other->mutex);

    ensurePlace(dm_context.table_handle_define, dm_context.storage_pool);
    other->ensurePlace(dm_context.table_handle_define, dm_context.storage_pool);

    doMerge(dm_context, other);

    MallocExtension::instance()->ReleaseFreeMemory();
}


Segment::Segment(UInt64 epoch_, const HandleRange & range_, PageId segment_id_, PageId next_segment_id_, PageId delta_id, PageId stable_id)
    : epoch(epoch_),
      range(range_),
      segment_id(segment_id_),
      next_segment_id(next_segment_id_),
      delta(true, delta_id),
      stable(false, stable_id),
      delta_tree(std::make_shared<DefaultDeltaTree>())
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
      delta_tree(std::make_shared<DefaultDeltaTree>())
{
}

void Segment::swap(Segment & other)
{
    std::swap(epoch, other.epoch);
    range.swap(other.range);
    std::swap(segment_id, other.segment_id);
    std::swap(next_segment_id, other.next_segment_id);

    delta.swap(other.delta);
    stable.swap(other.stable);

    delta_tree.swap(other.delta_tree);
    std::swap(placed_delta_rows, other.placed_delta_rows);
    std::swap(placed_delta_deletes, other.placed_delta_deletes);
}

size_t Segment::getEstimatedRows()
{
    std::shared_lock lock(mutex);
    return estimatedRows();
}

size_t Segment::getEstimatedBytes()
{
    std::shared_lock lock(mutex);
    return estimatedBytes();
}

//==========================================================================================
// Segment private methods.
//==========================================================================================

BlockInputStreamPtr Segment::getPlacedStream(const ColumnDefine &  handle,
                                             const ColumnDefines & columns_to_read,
                                             StoragePool &         storage_pool,
                                             size_t                expected_block_size,
                                             SharedLock &&         lock)
{
    // We always put handle and version at the beginning of columns.
    ColumnDefines new_columns_to_read;
    ColumnDefines columns_without_pk;

    new_columns_to_read.push_back(handle);
    new_columns_to_read.push_back(VERSION_COLUMN_DEFINE);

    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        const auto & c = columns_to_read[i];
        if (c.id != handle.id && c.id != VERSION_COLUMN_ID)
        {
            columns_without_pk.push_back(c);
            new_columns_to_read.push_back(c);
        }
    }

    auto stable_input_stream = stable.getInputStream(new_columns_to_read, storage_pool.data());

    // handle and version column are cached, they can be fetched faster by  delta.getPKColumns().
    auto delta_block            = delta.read(columns_without_pk, storage_pool.log(), 0, delta.num_rows());
    auto delta_pk_columns_block = delta.getPKColumns(handle, storage_pool.log()).toBlock(handle);

    delta_block.insert(0, delta_pk_columns_block.getByPosition(0));
    delta_block.insert(1, delta_pk_columns_block.getByPosition(1));

    auto delta_value_space = std::make_shared<DeltaValueSpace>(new_columns_to_read, delta_block);

    return std::make_shared<MyDeltaMergeBlockInputStream>(
        stable_input_stream, *delta_tree, delta_value_space, expected_block_size, std::move(lock));
}


SegmentPtr Segment::doSplit(DMContext & dm_context, Handle split_point)
{
    auto & handle       = dm_context.table_handle_define;
    auto & columns      = dm_context.table_columns;
    auto & storage_pool = dm_context.storage_pool;
    auto   min_version  = dm_context.min_version;

    HandleRange my_range    = {range.start, split_point};
    HandleRange other_range = {split_point, range.end};

    Chunks my_new_stable_chunks;
    Chunks other_new_stable_chunks;

    OpContext opc = OpContext::createForDataStorage(dm_context);
    {
        // Write my data
        BlockInputStreamPtr my_data = getPlacedStream(handle, columns, storage_pool, STABLE_CHUNK_ROWS, {});
        my_data                     = std::make_shared<DMHandleFilterBlockInputStream>(my_data, handle, my_range);
        my_data  = std::make_shared<DMVersionFilterBlockInputStream<DM_VESION_FILTER_MODE_COMPACT>>(my_data, handle, min_version);
        auto tmp = DiskValueSpace::writeChunks(opc, my_data);
        my_new_stable_chunks.swap(tmp);
    }

    {
        // Write new segment's data
        BlockInputStreamPtr other_data = getPlacedStream(handle, columns, storage_pool, STABLE_CHUNK_ROWS, {});
        other_data                     = std::make_shared<DMHandleFilterBlockInputStream>(other_data, handle, other_range);
        other_data = std::make_shared<DMVersionFilterBlockInputStream<DM_VESION_FILTER_MODE_COMPACT>>(other_data, handle, min_version);
        auto tmp   = DiskValueSpace::writeChunks(opc, other_data);
        other_new_stable_chunks.swap(tmp);
    }

    auto other_segment_id = storage_pool.newMetaPageId();
    auto other_delta_id   = storage_pool.newMetaPageId();
    auto other_stable_id  = storage_pool.newMetaPageId();

    Segment new_me(this->epoch + 1, //
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

    new_me.serialize(meta_wb);
    new_me.delta.setChunks({}, meta_wb, log_wb);
    new_me.stable.setChunks(std::move(my_new_stable_chunks), meta_wb, data_wb);

    other->serialize(meta_wb);
    other->delta.setChunks({}, meta_wb, log_wb);
    other->stable.setChunks(std::move(other_new_stable_chunks), meta_wb, data_wb);

    // Commit meta.
    storage_pool.meta().write(meta_wb);

    // ============================================================
    // The following code are pure memory operations,
    // they are considered safe and won't fail.

    this->swap(new_me);

    // ============================================================

    // Remove old chunks.
    storage_pool.log().write(log_wb);
    storage_pool.data().write(data_wb);

    return other;
}

void Segment::doMerge(DMContext & dm_context, const SegmentPtr & other)
{
    if (this->range.end != other->range.start || this->next_segment_id != other->segment_id)
        throw Exception("The ranges of merge segments are not consecutive: first end: " + DB::toString(this->range.end)
                        + ", second start: " + DB::toString(other->range.start));

    auto & handle       = dm_context.table_handle_define;
    auto & columns      = dm_context.table_columns;
    auto & storage_pool = dm_context.storage_pool;
    auto   min_version  = dm_context.min_version;

    Chunks new_stable_chunks;
    {
        BlockInputStreamPtr my_data = this->getPlacedStream(handle, columns, storage_pool, STABLE_CHUNK_ROWS, {});
        my_data = std::make_shared<DMVersionFilterBlockInputStream<DM_VESION_FILTER_MODE_COMPACT>>(my_data, handle, min_version);

        BlockInputStreamPtr other_data = other->getPlacedStream(handle, columns, storage_pool, STABLE_CHUNK_ROWS, {});
        other_data = std::make_shared<DMVersionFilterBlockInputStream<DM_VESION_FILTER_MODE_COMPACT>>(other_data, handle, min_version);

        BlockInputStreamPtr merged_stream = std::make_shared<ConcatBlockInputStream>(BlockInputStreams({my_data, other_data}));

        OpContext opc = OpContext::createForDataStorage(dm_context);
        auto      tmp = DiskValueSpace::writeChunks(opc, merged_stream);
        new_stable_chunks.swap(tmp);
    }

    HandleRange my_new_range = {range.start, other->getRange().end};

    Segment new_me(epoch + 1, //
                   my_new_range,
                   segment_id,
                   other->next_segment_id,
                   delta.pageId(),
                   delta.getChunks(),
                   stable.pageId(),
                   stable.getChunks());
    Segment other_copy(other->epoch, //
                       other->range,
                       other->segment_id,
                       other->next_segment_id,
                       other->delta.pageId(),
                       other->delta.getChunks(),
                       other->stable.pageId(),
                       other->stable.getChunks());

    WriteBatch meta_wb;
    WriteBatch log_wb;
    WriteBatch data_wb;

    new_me.serialize(meta_wb);
    new_me.delta.setChunks({}, meta_wb, log_wb);
    new_me.stable.setChunks(std::move(new_stable_chunks), meta_wb, data_wb);

    other_copy.delta.setChunks({}, meta_wb, log_wb);
    other_copy.stable.setChunks({}, meta_wb, data_wb);

    // Remove other's meta data.
    meta_wb.delPage(other->segment_id);
    meta_wb.delPage(other->delta.pageId());
    meta_wb.delPage(other->stable.pageId());

    // Commit meta updates.
    storage_pool.meta().write(meta_wb);

    // ============================================================
    // The following code are pure memory operations,
    // they are considered safe and won't fail.

    this->swap(new_me);
    // Don't free the content of other segment here, especially the mutex!

    // ============================================================

    // Remove old chunks.
    storage_pool.log().write(log_wb);
    storage_pool.data().write(data_wb);
}

void Segment::reset(DMContext & dm_context, BlockInputStreamPtr & input_stream)
{
    auto & storage_pool = dm_context.storage_pool;

    // Write new chunks.
    OpContext opc               = OpContext::createForDataStorage(dm_context);
    Chunks    new_stable_chunks = DiskValueSpace::writeChunks(opc, input_stream);

    Segment new_me(epoch, //
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

    new_me.delta.setChunks({}, meta_wb, log_wb);
    new_me.stable.setChunks(std::move(new_stable_chunks), meta_wb, data_wb);

    // Commit meta updates.
    storage_pool.meta().write(meta_wb);

    // ============================================================
    // The following code are pure memory operations,
    // they are considered safe and won't fail.

    this->swap(new_me);

    // ============================================================

    // Remove old chunks.
    storage_pool.log().write(log_wb);
    storage_pool.data().write(data_wb);
}

bool Segment::tryFlush(DMContext & dm_context, bool force)
{
    if (force || delta.num_rows() >= dm_context.delta_limit_rows || delta.num_bytes() >= dm_context.delta_limit_bytes)
    {
        flush(dm_context);
        return true;
    }
    return false;
}

void Segment::flush(DMContext & dm_context)
{
    auto & handle       = dm_context.table_handle_define;
    auto & columns      = dm_context.table_columns;
    auto & storage_pool = dm_context.storage_pool;
    auto   min_version  = dm_context.min_version;

    ensurePlace(handle, storage_pool);

    auto data_stream = getPlacedStream(handle, columns, storage_pool, STABLE_CHUNK_ROWS, {});
    data_stream      = std::make_shared<DMVersionFilterBlockInputStream<DM_VESION_FILTER_MODE_COMPACT>>(data_stream, handle, min_version);

    reset(dm_context, data_stream);

    // Force tcmalloc to return memory back to system.
    // https://internal.pingcap.net/jira/browse/FLASH-41
    // TODO: Evaluate the cost of this.
    MallocExtension::instance()->ReleaseFreeMemory();
}

void Segment::ensurePlace(const ColumnDefine & handle, StoragePool & storage)
{
    size_t delta_rows    = delta.num_rows();
    size_t delta_deletes = delta.num_deletes();
    if (placed_delta_rows == delta_rows && placed_delta_deletes == delta_deletes)
        return;

    auto blocks = delta.getMergeBlocks(handle, storage.log(), placed_delta_rows, delta_deletes);

    for (auto & v : blocks)
    {
        if (!v.delete_range.none())
            placeDelete(handle, storage, v.delete_range);
        else if (v.block)
            placeUpsert(handle, storage, std::move(v.block));
    }
}

void Segment::placeUpsert(const ColumnDefine & handle, StoragePool & storage, Block && block)
{
    BlockInputStreamPtr merged_stream = getPlacedStream(handle, {handle, VERSION_COLUMN_DEFINE}, storage, DEFAULT_BLOCK_SIZE, {});

    auto perm = sortBlockByPk(handle, block);
    DB::placeInsert(merged_stream, block, *delta_tree, placed_delta_rows, perm, getPkSort(handle));
    placed_delta_rows += block.rows();
}

void Segment::placeDelete(const ColumnDefine & handle, StoragePool & storage, const HandleRange & delete_range)
{
    Blocks delete_data;
    {
        BlockInputStreamPtr merged_stream = getPlacedStream(handle, {handle, VERSION_COLUMN_DEFINE}, storage, DEFAULT_BLOCK_SIZE, {});
        BlockInputStreamPtr delete_stream = std::make_shared<DMHandleFilterBlockInputStream>(merged_stream, handle, delete_range);
        // Try to merge into big block. 128 MB should be enough.
        SquashingBlockInputStream squashed_delete_stream(delete_stream, 0, 128 * (1 << 20));

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
        BlockInputStreamPtr merged_stream = getPlacedStream(handle, {handle, VERSION_COLUMN_DEFINE}, storage, DEFAULT_BLOCK_SIZE, {});
        DB::placeDelete(merged_stream, block, *delta_tree, getPkSort(handle));
    }
    ++placed_delta_deletes;
}

Handle Segment::getSplitPoint(DMContext & dm_context)
{

    auto & handle  = dm_context.table_handle_define;
    auto & storage = dm_context.storage_pool;

    ensurePlace(handle, storage);
    auto stream = getPlacedStream(handle, {dm_context.table_handle_define}, storage, DEFAULT_BLOCK_SIZE, {});

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

} // namespace DB