#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/ChunkBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DiskValueSpace.h>
#include <Storages/Page/PageStorage.h>

namespace ProfileEvents
{
extern const Event DMFlushDeltaCache;
extern const Event DMFlushDeltaCacheNS;
} // namespace ProfileEvents

namespace DB
{
namespace DM
{
//==========================================================================================
// DiskValueSpace public methods.
//==========================================================================================

DiskValueSpace::DiskValueSpace(bool should_cache_, PageId page_id_)
    : should_cache(should_cache_), page_id(page_id_), log(&Logger::get("DiskValueSpace"))
{
}

DiskValueSpace::DiskValueSpace(bool should_cache_, PageId page_id_, const Chunks & chunks_)
    : should_cache(should_cache_), page_id(page_id_), chunks(chunks_), log(&Logger::get("DiskValueSpace"))
{
}

DiskValueSpace::DiskValueSpace(const DiskValueSpace & other)
    : should_cache(other.should_cache), page_id(other.page_id), chunks(other.chunks), cache_chunks(other.cache_chunks), log(other.log)
{
    for (auto & [col_id, col] : other.cache)
        cache.emplace(col_id, col->cloneResized(col->size()));
}

void DiskValueSpace::restore(const OpContext & context)
{
    // Deserialize.
    Page                 page = context.meta_storage.read(page_id);
    ReadBufferFromMemory buf(page.data.begin(), page.data.size());
    chunks = deserializeChunks(buf);

    // Restore cache.
    if (should_cache && !chunks.empty())
    {
        size_t chunks_to_cache = 0;
        for (ssize_t i = chunks.size() - 1; i >= 0; --i)
        {
            const auto & chunk = chunks[i];
            // We can not cache a chunk which is a delete range and not the latest one.
            if (chunk.isDeleteRange() && i != (ssize_t)chunks.size() - 1)
                break;
            if (chunk.getRows() >= context.dm_context.delta_cache_limit_rows
                || chunk.getBytes() >= context.dm_context.delta_cache_limit_bytes)
                break;
            ++chunks_to_cache;
        }

        // Load cache into memory.
        size_t total_rows = num_rows();
        size_t cache_rows = rowsFromBack(chunks_to_cache);
        Block  cache_data = read(context.dm_context.table_columns, context.data_storage, total_rows - cache_rows, cache_rows);
        if (unlikely(cache_data.rows() != cache_rows))
            throw Exception("The fragment rows from storage mismatch");

        for (const auto & col_define : context.dm_context.table_columns)
        {
            ColumnWithTypeAndName & col = cache_data.getByName(col_define.name);
            cache[col_define.id]        = (*std::move(col.column)).mutate();
        }
        // Note: cache_chunks must be set after #read() .
        cache_chunks = chunks_to_cache;
    }

    // Try flush now, in case of last flush failure recover.
    tryFlushCache(context);
}

DiskValueSpace::AppendTaskPtr DiskValueSpace::createAppendTask(const OpContext & context, const BlockOrDelete & block_or_delete) const
{
    auto task = std::make_unique<AppendTask>();

    auto & append_block = block_or_delete.block;
    auto & delete_range = block_or_delete.delete_range;

    const bool   is_delete       = !append_block;
    const size_t block_bytes     = is_delete ? 0 : blockBytes(append_block);
    const size_t append_rows     = is_delete ? 0 : append_block.rows();
    const size_t cache_rows      = cacheRows();
    const size_t cache_bytes     = cacheBytes();
    const size_t total_rows      = num_rows();
    const size_t in_storage_rows = total_rows - cache_rows;

    Chunk delete_chunk = is_delete ? Chunk(delete_range) : Chunk();

    MemoryWriteBuffer buf(0, CHUNK_SERIALIZE_BUFFER_SIZE);

    // If the newly appended object is a delete_range, then we must clear the cache and flush them.

    if (!is_delete //
        && (cache_rows + append_rows) < context.dm_context.delta_cache_limit_rows
        && (cache_bytes + block_bytes) < context.dm_context.delta_cache_limit_bytes)
    {
        // Simply put the newly appended block into cache.
        Chunk chunk = prepareChunkDataWrite(context.dm_context, context.gen_data_page_id, task->data_write_batch, append_block);

        task->append_cache      = true;
        task->remove_chunk_back = 0;

        serializeChunks(buf, chunks.begin(), chunks.end(), &chunk);
        task->append_chunks.emplace_back(std::move(chunk));
    }
    else
    {
        // Flush cache.

        task->append_cache      = false;
        task->remove_chunk_back = cache_chunks;

        if (!cache_chunks)
        {
            // There are no caches, simple write the newly block or delete_range is enough.
            Chunk chunk = is_delete
                ? delete_chunk
                : prepareChunkDataWrite(context.dm_context, context.gen_data_page_id, task->data_write_batch, append_block);

            serializeChunks(buf, chunks.begin(), chunks.end(), &chunk);
            task->append_chunks.emplace_back(std::move(chunk));
        }
        else
        {
            // Merge the former caches chunks and the newly block into one big chunk, and then flush it.
            // delete_range is stored by a separated chunk.

            size_t cache_start = chunks.size() - cache_chunks;
            for (size_t i = cache_start; i < chunks.size(); ++i)
            {
                auto & old_chunk = chunks[i];
                for (const auto & [col_id, col_meta] : old_chunk.getMetas())
                {
                    (void)col_id;
                    task->data_remove_write_batch.delPage(col_meta.page_id);
                }
            }

            Block  compacted_block;
            size_t compacted_rows = cache_rows + append_rows;
            if (cache.empty())
            {
                // Load fragment chunks' data from disk.
                compacted_block = read(context.dm_context.table_columns, //
                                       context.data_storage,
                                       in_storage_rows,
                                       cache_rows,
                                       compacted_rows);

                if (unlikely(compacted_block.rows() != cache_rows))
                    throw Exception("The fragment rows from storage mismatch");

                if (!is_delete)
                    concat(compacted_block, append_block);
            }
            else
            {
                // Use the cache.
                for (const auto & col_define : context.dm_context.table_columns)
                {
                    auto new_col = col_define.type->createColumn();
                    new_col->reserve(compacted_rows);
                    new_col->insertRangeFrom(*cache.at(col_define.id), 0, cache_rows);
                    if (!is_delete)
                        new_col->insertRangeFrom(*append_block.getByName(col_define.name).column, 0, append_rows);

                    ColumnWithTypeAndName col;
                    col.column    = std::move(new_col);
                    col.name      = col_define.name;
                    col.type      = col_define.type;
                    col.column_id = col_define.id;

                    compacted_block.insert(col);
                }
            }

            Chunk compacted_chunk
                = prepareChunkDataWrite(context.dm_context, context.gen_data_page_id, task->data_write_batch, compacted_block);

            if (is_delete)
                serializeChunks(buf, chunks.begin(), chunks.begin() + (chunks.size() - cache_chunks), &compacted_chunk, &delete_chunk);
            else
                serializeChunks(buf, chunks.begin(), chunks.begin() + (chunks.size() - cache_chunks), &compacted_chunk);

            task->append_chunks.emplace_back(std::move(compacted_chunk));
            if (is_delete)
                task->append_chunks.emplace_back(delete_chunk);
        }
    }

    auto data_size = buf.count(); // Must be read before tryGetReadBuffer().
    task->meta_write_batch.putPage(page_id, 0, buf.tryGetReadBuffer(), data_size);

    return task;
}

void DiskValueSpace::applyAppendTask(const OpContext & context, const AppendTaskPtr & task, const BlockOrDelete & block_or_delete)
{
    if (task->remove_chunk_back)
        chunks.resize(chunks.size() - task->remove_chunk_back);

    for (auto & chunk : task->append_chunks)
        chunks.emplace_back(std::move(chunk));

    if (task->append_cache)
    {
        auto block_rows = block_or_delete.block.rows();
        for (const auto & col_define : context.dm_context.table_columns)
        {
            const ColumnWithTypeAndName & col = block_or_delete.block.getByName(col_define.name);

            auto it = cache.find(col_define.id);
            if (it == cache.end())
                cache.emplace(col_define.id, col_define.type->createColumn());

            cache[col_define.id]->insertRangeFrom(*col.column, 0, block_rows);
        }
        ++cache_chunks;
    }
    else
    {
        cache.clear();
        cache_chunks = 0;
    }
}

Chunks DiskValueSpace::writeChunks(const OpContext & context, const BlockInputStreamPtr & input_stream)
{
    // TODO: investigate which way is better for scan: written by chunks vs written by columns.
    Chunks chunks;
    while (true)
    {
        Block block = input_stream->read();
        if (!block)
            break;
        if (!block.rows())
            continue;
        WriteBatch wb;
        Chunk      chunk = prepareChunkDataWrite(context.dm_context, context.gen_data_page_id, wb, block);
        context.data_storage.write(wb);
        chunks.push_back(std::move(chunk));
    }
    return chunks;
}

Chunk DiskValueSpace::writeDelete(const OpContext &, const HandleRange & delete_range)
{
    return Chunk(delete_range);
}

void DiskValueSpace::setChunks(Chunks && new_chunks, WriteBatch & meta_wb, WriteBatch & data_wb)
{
    MemoryWriteBuffer buf(0, CHUNK_SERIALIZE_BUFFER_SIZE);
    serializeChunks(buf, new_chunks.begin(), new_chunks.end(), {});
    auto data_size = buf.count();
    meta_wb.putPage(page_id, 0, buf.tryGetReadBuffer(), data_size);

    for (const auto & c : chunks)
        for (const auto & m : c.getMetas())
            data_wb.delPage(m.second.page_id);

    chunks.swap(new_chunks);

    cache.clear();
    cache_chunks = 0;
}

void DiskValueSpace::appendChunkWithCache(const OpContext & context, Chunk && chunk, const Block & block)
{
    // should only append to delta
    assert(should_cache);
    {
        MemoryWriteBuffer buf(0, CHUNK_SERIALIZE_BUFFER_SIZE);
        serializeChunks(buf, chunks.begin(), chunks.end(), &chunk);
        auto       data_size = buf.count();
        WriteBatch meta_wb;
        meta_wb.putPage(page_id, 0, buf.tryGetReadBuffer(), data_size);
        context.meta_storage.write(meta_wb);
    }

    chunks.push_back(std::move(chunk));

    auto write_rows  = block.rows();
    auto write_bytes = block.bytes();
    if (!write_rows)
    {
        // If it is an empty chunk, then nothing to append.
        ++cache_chunks;
        return;
    }

    // If former cache is empty, and this chunk is big enough, then no need to cache.
    if (cache_chunks == 0
        && (write_rows >= context.dm_context.delta_cache_limit_rows || write_bytes >= context.dm_context.delta_cache_limit_bytes))
        return;

    for (const auto & col_define : context.dm_context.table_columns)
    {
        const ColumnWithTypeAndName & col = block.getByName(col_define.name);

        auto it = cache.find(col_define.id);
        if (it == cache.end())
            cache.emplace(col_define.id, col_define.type->createColumn());

        cache[col_define.id]->insertRangeFrom(*col.column, 0, write_rows);
    }
    ++cache_chunks;
}

Block DiskValueSpace::read(const ColumnDefines & read_column_defines,
                           PageStorage &         data_storage,
                           size_t                rows_offset,
                           size_t                rows_limit,
                           std::optional<size_t> reserve_rows_) const
{
    if (read_column_defines.empty())
        return {};
    rows_limit = std::min(rows_limit, num_rows() - rows_offset);

    size_t         reserve_rows = reserve_rows_ ? *reserve_rows_ : rows_limit;
    MutableColumns columns;
    for (const auto & define : read_column_defines)
    {
        columns.emplace_back(define.type->createColumn());
        columns.back()->reserve(reserve_rows);
    }

    auto [start_chunk_index, rows_start_in_start_chunk] = findChunk(rows_offset);
    auto [end_chunk_index, rows_end_in_end_chunk]       = findChunk(rows_offset + rows_limit);

    size_t chunk_cache_start = chunks.size() - cache_chunks;
    size_t already_read_rows = 0;

    /// For the chunks in storage, we read chunk by chunk, instead of column by column.
    /// Because the column data of a same chunk are likely to be stored together, or at least closed.
    /// While for the chunks in cache, we read column by column.

    size_t chunk_index = start_chunk_index;
    for (; chunk_index < chunk_cache_start && chunk_index <= end_chunk_index; ++chunk_index)
    {
        const auto & cur_chunk = chunks[chunk_index];

        if (cur_chunk.getRows())
        {
            size_t rows_start_in_chunk = chunk_index == start_chunk_index ? rows_start_in_start_chunk : 0;
            size_t rows_end_in_chunk   = chunk_index == end_chunk_index ? rows_end_in_end_chunk : cur_chunk.getRows();

            if (rows_end_in_chunk > rows_start_in_chunk)
            {
                readChunkData(
                    columns, cur_chunk, read_column_defines, data_storage, rows_start_in_chunk, rows_end_in_chunk - rows_start_in_chunk);

                already_read_rows += rows_end_in_chunk - rows_start_in_chunk;
            }
        }
    }

    /// The reset of data is in cache, simply append them into result.

    if (already_read_rows < rows_limit)
    {
        // chunk_index could be larger than chunk_cache_start.
        size_t cache_rows_offset = 0;
        for (size_t i = chunk_cache_start; i < chunk_index; ++i)
            cache_rows_offset += chunks[i].getRows();

        for (size_t index = 0; index < read_column_defines.size(); ++index)
        {
            ColumnDefine define    = read_column_defines[index];
            auto &       cache_col = cache.at(define.id);

            size_t rows_offset_in_chunk = chunk_index == start_chunk_index ? rows_start_in_start_chunk : 0;

            columns[index]->insertRangeFrom(*cache_col, cache_rows_offset + rows_offset_in_chunk, rows_limit - already_read_rows);
        }
    }

    Block res;
    for (size_t index = 0; index < read_column_defines.size(); ++index)
    {
        const ColumnDefine &  define = read_column_defines[index];
        ColumnWithTypeAndName col;
        col.type      = define.type;
        col.name      = define.name;
        col.column_id = define.id;
        col.column    = std::move(columns[index]);

        res.insert(col);
    }
    return res;
}

Block DiskValueSpace::read(const ColumnDefines & read_column_defines, PageStorage & data_storage, size_t chunk_index) const
{
    if (read_column_defines.empty())
        return {};

    const auto & chunk = chunks.at(chunk_index);

    MutableColumns columns;
    for (const auto & define : read_column_defines)
    {
        columns.emplace_back(define.type->createColumn());
        columns.back()->reserve(chunk.getRows());
    }

    if (chunk.getRows())
    {
        size_t chunk_cache_start = chunks.size() - cache_chunks;
        if (chunk_index < chunk_cache_start)
        {
            // Read from storage
            readChunkData(columns, chunk, read_column_defines, data_storage, 0, chunk.getRows());
        }
        else
        {
            // Read from cache
            size_t cache_rows_offset = 0;
            for (size_t i = chunk_cache_start; i < chunk_index; ++i)
                cache_rows_offset += chunks[i].getRows();

            for (size_t index = 0; index < read_column_defines.size(); ++index)
            {
                const auto & define    = read_column_defines[index];
                auto &       cache_col = cache.at(define.id);
                columns[index]->insertRangeFrom(*cache_col, cache_rows_offset, chunk.getRows());
            }
        }
    }

    Block res;
    for (size_t index = 0; index < read_column_defines.size(); ++index)
    {
        const ColumnDefine &  define = read_column_defines[index];
        ColumnWithTypeAndName col;
        col.type      = define.type;
        col.name      = define.name;
        col.column_id = define.id;
        col.column    = std::move(columns[index]);

        res.insert(col);
    }
    return res;
}

BlockOrDeletes DiskValueSpace::getMergeBlocks(const ColumnDefine & handle,
                                              PageStorage &        data_storage,
                                              size_t               rows_begin,
                                              size_t               deletes_begin,
                                              size_t               rows_end,
                                              size_t               deletes_end) const
{
    BlockOrDeletes res;

    auto [start_chunk_index, rows_start_in_start_chunk] = findChunk(rows_begin, deletes_begin);
    auto [end_chunk_index, rows_end_in_end_chunk]       = findChunk(rows_end, deletes_end);

    size_t block_rows_start = rows_begin;
    size_t block_rows_end   = rows_begin;

    for (size_t chunk_index = start_chunk_index; chunk_index < chunks.size() && chunk_index <= end_chunk_index; ++chunk_index)
    {
        const Chunk & chunk = chunks[chunk_index];

        size_t rows_start_in_chunk = chunk_index == start_chunk_index ? rows_start_in_start_chunk : 0;
        size_t rows_end_in_chunk   = chunk_index == end_chunk_index ? rows_end_in_end_chunk : chunk.getRows();

        block_rows_end += rows_end_in_chunk - rows_start_in_chunk;

        if (chunk.isDeleteRange() || (chunk_index == chunks.size() - 1 || chunk_index == end_chunk_index))
        {
            if (chunk.isDeleteRange())
                res.emplace_back(chunk.getDeleteRange());
            if (block_rows_end != block_rows_start)
                res.emplace_back(read({handle, VERSION_COLUMN_DEFINE}, data_storage, block_rows_start, block_rows_end - block_rows_start));

            block_rows_start = block_rows_end;
        }
    }

    return res;
}

bool DiskValueSpace::tryFlushCache(const OpContext & context, bool force)
{
    if (!cache_chunks)
        return false;
    const size_t cache_rows = cacheRows();

    // A chunk can only contains one delete range.
    HandleRange delete_range = chunks.back().isDeleteRange() ? chunks.back().getDeleteRange() : HandleRange::newNone();
    if (!delete_range.none())
        force = true;
    if (!force && cache_rows < context.dm_context.delta_cache_limit_rows && cacheBytes() < context.dm_context.delta_cache_limit_bytes)
        return false;

    return doFlushCache(context);
}

bool DiskValueSpace::doFlushCache(const OpContext & context)
{
    const size_t cache_rows      = cacheRows();
    const size_t total_rows      = num_rows();
    const size_t in_storage_rows = total_rows - cache_rows;

    HandleRange delete_range = chunks.back().isDeleteRange() ? chunks.back().getDeleteRange() : HandleRange::newNone();

    if (cache_chunks == 1)
    {
        // One chunk no need to compact.
        cache.clear();
        cache_chunks = 0;
        return true;
    }

    EventRecorder recorder(ProfileEvents::DMFlushDeltaCache, ProfileEvents::DMFlushDeltaCacheNS);

    LOG_DEBUG(log, "Start flush cache");

    /// Flush cache to disk and replace the fragment chunks.

    WriteBatch data_wb_insert;
    WriteBatch data_wb_remove;
    WriteBatch meta_wb;

    size_t cache_start = chunks.size() - cache_chunks;
    for (size_t i = cache_start; i < chunks.size(); ++i)
    {
        auto & old_chunk = chunks[i];
        for (const auto & [col_id, col_meta] : old_chunk.getMetas())
        {
            (void)col_id;
            data_wb_remove.delPage(col_meta.page_id);
        }
    }

    Block compacted;
    if (cache.empty())
    {
        // Load fragment data from disk.
        compacted = read(context.dm_context.table_columns, context.data_storage, in_storage_rows, cache_rows);

        if (unlikely(compacted.rows() != cache_rows))
            throw Exception("The fragment rows from storage mismatch");
    }
    else
    {
        // Use the cache.
        for (const auto & col_define : context.dm_context.table_columns)
        {
            ColumnWithTypeAndName col;
            col.column    = cache.at(col_define.id)->cloneResized(cache_rows);
            col.name      = col_define.name;
            col.type      = col_define.type;
            col.column_id = col_define.id;
            compacted.insert(col);

            if (unlikely(col.column->size() != cache_rows))
                throw Exception("The cache rows mismatch");
        }
    }

    Chunk compacted_chunk = prepareChunkDataWrite(context.dm_context, context.gen_data_page_id, data_wb_insert, compacted);

    Chunks new_chunks(chunks.begin(), chunks.begin() + (chunks.size() - cache_chunks));
    new_chunks.push_back(std::move(compacted_chunk));
    if (!delete_range.none())
        new_chunks.emplace_back(delete_range);

    {
        MemoryWriteBuffer buf(0, CHUNK_SERIALIZE_BUFFER_SIZE);
        serializeChunks(buf, new_chunks.begin(), new_chunks.end(), {});
        auto data_size = buf.count();
        meta_wb.putPage(page_id, 0, buf.tryGetReadBuffer(), data_size);
    }

    /// Note that: We have the risk of dirty page here. Because writing between different storage is not atomic.
    /// But the correctness is still guaranteed.

    // The order here is critical.
    context.data_storage.write(data_wb_insert);
    context.meta_storage.write(meta_wb);

    // ============================================================
    // The following code are pure memory operations,
    // they are considered safe and won't fail.

    chunks.swap(new_chunks);
    cache.clear();
    cache_chunks = 0;

    // ============================================================

    context.data_storage.write(data_wb_remove);

    recorder.submit();

    LOG_DEBUG(log, "Done flush cache");

    return true;
}

ChunkBlockInputStreamPtr DiskValueSpace::getInputStream(const ColumnDefines & read_columns, PageStorage & data_storage) const
{
    return std::make_shared<ChunkBlockInputStream>(chunks, read_columns, data_storage);
}

size_t DiskValueSpace::num_rows() const
{
    size_t rows = 0;
    for (const auto & c : chunks)
        rows += c.getRows();
    return rows;
}

size_t DiskValueSpace::num_rows(size_t chunks_offset, size_t chunks_length) const
{
    size_t rows = 0;
    for (size_t i = chunks_offset; i < chunks_offset + chunks_length; ++i)
        rows += chunks.at(i).getRows();
    return rows;
}

size_t DiskValueSpace::num_deletes() const
{
    size_t deletes = 0;
    for (const auto & c : chunks)
        deletes += c.isDeleteRange();
    return deletes;
}

size_t DiskValueSpace::num_bytes() const
{
    size_t bytes = 0;
    for (const auto & c : chunks)
        bytes += c.getBytes();
    return bytes;
}

size_t DiskValueSpace::num_chunks() const
{
    return chunks.size();
}

size_t DiskValueSpace::rowsFromBack(size_t chunk_num_from_back) const
{
    size_t rows = 0;
    for (ssize_t i = chunks.size() - 1; i >= (ssize_t)(chunks.size() - chunk_num_from_back); --i)
        rows += chunks[i].getRows();
    return rows;
}

size_t DiskValueSpace::cacheRows() const
{
    return rowsFromBack(cache_chunks);
}

size_t DiskValueSpace::cacheBytes() const
{
    if (cacheRows() && cache.empty())
        throw Exception("cache empty");

    size_t cache_bytes = 0;
    for (const auto & [col_id, col] : cache)
    {
        (void)col_id;
        cache_bytes += col->byteSize();
    }
    return cache_bytes;
}

std::pair<size_t, size_t> DiskValueSpace::findChunk(size_t rows_offset) const
{
    size_t count       = 0;
    size_t chunk_index = 0;
    for (; chunk_index < chunks.size(); ++chunk_index)
    {
        if (count == rows_offset)
            return {chunk_index, 0};
        auto cur_chunk_rows = chunks[chunk_index].getRows();
        count += cur_chunk_rows;
        if (count > rows_offset)
            return {chunk_index, cur_chunk_rows - (count - rows_offset)};
    }
    if (count != rows_offset)
        throw Exception("rows_offset(" + DB::toString(rows_offset) + ") is out of total_rows(" + DB::toString(count) + ")");

    return {chunk_index, 0};
}

std::pair<size_t, size_t> DiskValueSpace::findChunk(size_t rows_offset, size_t deletes_offset) const
{
    size_t rows_count    = 0;
    size_t deletes_count = 0;
    size_t chunk_index   = 0;
    for (; chunk_index < chunks.size(); ++chunk_index)
    {
        if (rows_count == rows_offset && deletes_count == deletes_offset)
            return {chunk_index, 0};
        const auto & chunk = chunks[chunk_index];
        if (chunk.getRows())
        {
            rows_count += chunk.getRows();
            if (rows_count > rows_offset)
            {
                if (unlikely(deletes_count != deletes_offset))
                    throw Exception("deletes_offset and rows_offset are not matched");

                return {chunk_index, chunk.getRows() - (rows_count - rows_offset)};
            }
        }
        if (chunk.isDeleteRange())
        {
            if (deletes_count == deletes_offset)
            {
                if (unlikely(rows_count != rows_offset))
                    throw Exception("deletes_offset and rows_offset are not matched");
                return {chunk_index, 0};
            }
            ++deletes_count;
        }
    }
    if (rows_count != rows_offset || deletes_count != deletes_offset)
        throw Exception("illegal rows_offset(" + DB::toString(rows_offset) + "), deletes_count(" + DB::toString(deletes_count) + ")");

    return {chunk_index, 0};
}

} // namespace DM
} // namespace DB