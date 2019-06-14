#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DiskValueSpace.h>
#include <Storages/Page/PageStorage.h>

namespace DB
{


//==========================================================================================
// Helper functions.
//==========================================================================================

using BufferAndSize = std::pair<ReadBufferPtr, size_t>;
BufferAndSize serializeColumn(const IColumn & column, const DataTypePtr & type, size_t offset, size_t num, bool compress)
{
    MemoryWriteBuffer plain;
    CompressionMethod method = compress ? CompressionMethod::LZ4 : CompressionMethod::NONE;

    CompressedWriteBuffer compressed(plain, CompressionSettings(method));
    type->serializeBinaryBulkWithMultipleStreams(column, //
                                                 [&](const IDataType::SubstreamPath &) { return &compressed; },
                                                 offset,
                                                 num,
                                                 true,
                                                 {});
    compressed.next();

    auto data_size = plain.count();
    return {plain.tryGetReadBuffer(), data_size};
}


void deserializeColumn(IColumn & column, const ColumnMeta & meta, const Page & page, size_t rows_limit)
{
    ReadBufferFromMemory buf(page.data.begin(), page.data.size());
    CompressedReadBuffer compressed(buf);
    meta.type->deserializeBinaryBulkWithMultipleStreams(column, //
                                                        [&](const IDataType::SubstreamPath &) { return &compressed; },
                                                        rows_limit,
                                                        (double)(page.data.size()) / meta.rows,
                                                        true,
                                                        {});
}

void readChunkData(MutableColumns &      columns,
                   const Chunk &         chunk,
                   const ColumnDefines & column_defines,
                   PageStorage &         storage,
                   size_t                rows_offset,
                   size_t                rows_limit)
{

    PageIds page_ids;
    page_ids.reserve(column_defines.size());
    for (const auto & define : column_defines)
        page_ids.push_back(chunk.getColumn(define.id).page_id);

    auto pages = storage.read(page_ids);
    for (size_t index = 0; index < column_defines.size(); ++index)
    {
        ColumnDefine         define = column_defines[index];
        const Page &         page   = pages.at(page_ids[index]);
        ReadBufferFromMemory buf(page.data.begin(), page.data.size());
        const ColumnMeta &   meta = chunk.getColumn(define.id);
        IColumn &            col  = *columns[index];

        if (!rows_offset)
        {
            deserializeColumn(col, meta, page, rows_limit);
        }
        else
        {
            auto tmp_col = define.type->createColumn();
            deserializeColumn(*tmp_col, meta, page, rows_limit);
            col.insertRangeFrom(*tmp_col, rows_offset, rows_limit);
        }
    }
}

using GetColumn = std::function<const IColumn &(ColId)>;
Chunk prepareChunkDataWrite(const DMContext & dm_context, const GenPageId & gen_data_page_id, WriteBatch & wb, const Block & block)
{
    Chunk chunk;
    for (const auto & col_define : dm_context.table_columns)
    {
        auto            col_id = col_define.id;
        const IColumn & column = *(block.getByName(col_define.name).column);
        auto [buf, size]       = serializeColumn(column, col_define.type, 0, column.size(), !dm_context.not_compress.count(col_id));

        ColumnMeta d;
        d.col_id  = col_id;
        d.page_id = gen_data_page_id();
        d.rows    = column.size();
        d.bytes   = size;
        d.type    = col_define.type;

        wb.putPage(d.page_id, 0, buf, size);
        chunk.insert(d);
    }

    return chunk;
}

//==========================================================================================
// DiskValueSpace public methods.
//==========================================================================================

DiskValueSpace::DiskValueSpace(bool should_cache_, PageId page_id_) : should_cache(should_cache_), page_id(page_id_) {}

DiskValueSpace::DiskValueSpace(bool should_cache_, PageId page_id_, const Chunks & chunks_)
    : should_cache(should_cache_), page_id(page_id_), chunks(chunks_)
{
}

void DiskValueSpace::swap(DiskValueSpace & other)
{
    std::swap(page_id, other.page_id);
    chunks.swap(other.chunks);

    pk_columns.swap(other.pk_columns);

    cache.swap(other.cache);
    std::swap(cache_chunks, other.cache_chunks);
}

void DiskValueSpace::restore(const OpContext & context)
{
    Page                 page = context.meta_storage.read(page_id);
    ReadBufferFromMemory buf(page.data.begin(), page.data.size());
    chunks = deserializeChunks(buf);

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

Chunks DiskValueSpace::writeChunks(const OpContext & context, const BlockInputStreamPtr & input_stream)
{
    // TODO: investigate which way is better for scan: written by chunks vs written by columns.
    Chunks chunks;
    while (true)
    {
        Block block = input_stream->read();
        if (!block)
            break;
        WriteBatch wb;
        Chunk      chunk = prepareChunkDataWrite(context.dm_context, context.gen_data_page_id, wb, block);
        context.data_storage.write(wb);
        chunks.push_back(std::move(chunk));
    }
    return chunks;
}

Chunk DiskValueSpace::writeDelete(const OpContext &, const HandleRange & delete_range)
{
    return Chunk::newChunk(delete_range);
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
    pk_columns = {};

    cache.clear();
    cache_chunks = 0;
}

void DiskValueSpace::appendChunkWithCache(const OpContext & context, Chunk && chunk, const Block & block)
{
    {
        MemoryWriteBuffer buf(0, CHUNK_SERIALIZE_BUFFER_SIZE);
        serializeChunks(buf, chunks.begin(), chunks.end(), chunk);
        auto       data_size = buf.count();
        WriteBatch meta_wb;
        meta_wb.putPage(page_id, 0, buf.tryGetReadBuffer(), data_size);
        context.meta_storage.write(meta_wb);
    }

    const auto & handle = context.dm_context.table_handle_define;
    ensurePKColumns(handle, context.data_storage);

    chunks.push_back(std::move(chunk));

    auto write_rows  = block.rows();
    auto write_bytes = block.bytes();
    if (!write_rows)
    {
        // If it is an empty chunk, then nothing to append.
        ++cache_chunks;
        return;
    }

    pk_columns.append(block, handle);

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

Block DiskValueSpace::read(const ColumnDefines & read_column_defines, PageStorage & data_storage, size_t rows_offset, size_t rows_limit)
{
    if (read_column_defines.empty())
        return {};
    rows_limit = std::min(rows_limit, num_rows() - rows_offset);

    MutableColumns columns;
    for (const auto & define : read_column_defines)
    {
        columns.emplace_back(define.type->createColumn());
        columns.back()->reserve(rows_limit);
    }

    auto [start_chunk_index, rows_offset_in_start_chunk] = findChunk(rows_offset);
    auto [end_chunk_index, rows_limit_in_end_chunk]      = findChunk(rows_offset + rows_limit);

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
            size_t rows_offset_in_chunk = chunk_index == start_chunk_index ? rows_offset_in_start_chunk : 0;
            size_t rows_limit_in_chunk  = chunk_index == end_chunk_index ? rows_limit_in_end_chunk : cur_chunk.getRows();

            readChunkData(columns, cur_chunk, read_column_defines, data_storage, rows_offset_in_chunk, rows_limit_in_chunk);

            already_read_rows += rows_limit_in_chunk - rows_offset_in_chunk;
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

            size_t rows_offset_in_chunk = chunk_index == start_chunk_index ? rows_offset_in_start_chunk : 0;

            columns[index]->insertRangeFrom(*cache_col, cache_rows_offset + rows_offset_in_chunk, rows_limit - already_read_rows);
        }
    }


    Block res;
    for (size_t index = 0; index < read_column_defines.size(); ++index)
    {
        ColumnDefine          define = read_column_defines[index];
        ColumnWithTypeAndName col;
        col.type      = define.type;
        col.name      = define.name;
        col.column_id = define.id;
        col.column    = std::move(columns[index]);

        res.insert(col);
    }
    return res;
}

Block DiskValueSpace::read(const ColumnDefines & read_column_defines, PageStorage & data_storage, size_t chunk_index)
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
                ColumnDefine define    = read_column_defines[index];
                auto &       cache_col = cache.at(define.id);
                columns[index]->insertRangeFrom(*cache_col, cache_rows_offset, chunk.getRows());
            }
        }
    }

    Block res;
    for (size_t index = 0; index < read_column_defines.size(); ++index)
    {
        ColumnDefine          define = read_column_defines[index];
        ColumnWithTypeAndName col;
        col.type      = define.type;
        col.name      = define.name;
        col.column_id = define.id;
        col.column    = std::move(columns[index]);

        res.insert(col);
    }
    return res;
}

BlockOrRanges
DiskValueSpace::getMergeBlocks(const ColumnDefine & handle, PageStorage & data_storage, size_t rows_offset, size_t deletes_offset)
{
    BlockOrRanges res;

    auto [start_chunk_index, rows_offset_in_start_chunk] = findChunk(rows_offset, deletes_offset);

    ensurePKColumns(handle, data_storage);

    size_t block_rows_start = rows_offset;
    size_t block_rows_end   = rows_offset;
    for (size_t chunk_index = start_chunk_index; chunk_index < chunks.size(); ++chunk_index)
    {
        const Chunk & chunk = chunks[chunk_index];
        block_rows_end += chunk_index == start_chunk_index ? chunk.getRows() - rows_offset_in_start_chunk : chunk.getRows();

        if (chunk.isDeleteRange() || chunk_index == chunks.size() - 1)
        {
            BlockOrRange block_or_range;
            if (chunk.isDeleteRange())
                block_or_range.delete_range = chunk.getDeleteRange();
            else if (block_rows_start != block_rows_end)
                block_or_range.block = PKColumns(pk_columns, block_rows_start, block_rows_end - block_rows_start).toBlock(handle);

            res.emplace_back(block_or_range);
            block_rows_start = block_rows_end;
        }
    }

    return res;
}

bool DiskValueSpace::tryFlushCache(const OpContext & context, bool force)
{
    if (!cache_chunks)
        return false;
    const size_t cache_rows      = cacheRows();
    const size_t total_rows      = num_rows();
    const size_t in_storage_rows = total_rows - cache_rows;

    // A chunk can only contains one delete range.
    HandleRange delete_range = chunks.back().isDeleteRange() ? chunks.back().getDeleteRange() : HandleRange::newNone();
    if (!delete_range.none())
        force = true;
    if (!force && cache_rows < context.dm_context.delta_cache_limit_rows && cacheBytes() < context.dm_context.delta_cache_limit_bytes)
        return false;

    auto & handle = context.dm_context.table_handle_define;
    ensurePKColumns(handle, context.data_storage);

    if (cache_chunks == 1)
    {
        // One chunk no need to compact.
        cache.clear();
        cache_chunks = 0;
        return true;
    }

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
    {
        // Remove the last cache_rows of pk_columns and append the compacted data
        PKColumns new_pk_columns(pk_columns.handle_column->cloneResized(in_storage_rows),
                                 pk_columns.version_column->cloneResized(in_storage_rows));
        new_pk_columns.append(compacted, handle);

        pk_columns = new_pk_columns;
    }

    // ============================================================

    context.data_storage.write(data_wb_remove);
    return true;
}

class DiskValueSpace::DVSBlockInputStream final : public IProfilingBlockInputStream
{
public:
    DVSBlockInputStream(DiskValueSpace & dvs_, const ColumnDefines & read_columns_, PageStorage & data_storage_)
        : dvs(dvs_), read_columns(read_columns_), data_storage(data_storage_)
    {
    }

    String getName() const override { return "DiskValueSpace"; }
    Block  getHeader() const override
    {
        Block res;
        for (const auto & c : read_columns)
        {
            ColumnWithTypeAndName col;
            col.column    = c.type->createColumn();
            col.type      = c.type;
            col.name      = c.name;
            col.column_id = c.id;
            res.insert(col);
        }
        return res;
    }

protected:
    Block readImpl() override
    {
        if (chunk_index >= dvs.num_chunks())
            return {};
        return dvs.read(read_columns, data_storage, chunk_index++);
    }

private:
    DiskValueSpace & dvs;
    ColumnDefines    read_columns;
    PageStorage &    data_storage;
    size_t           chunk_index = 0;
};

BlockInputStreamPtr DiskValueSpace::getInputStream(const ColumnDefines & read_columns, PageStorage & data_storage)
{
    return std::make_shared<DVSBlockInputStream>(*this, read_columns, data_storage);
}

const PKColumns & DiskValueSpace::getPKColumns(const ColumnDefine & handle, PageStorage & data_storage)
{
    if (!should_cache)
        throw Exception("You should not call this method if should_cache is false");
    ensurePKColumns(handle, data_storage);
    return pk_columns;
}

size_t DiskValueSpace::num_rows()
{
    size_t rows = 0;
    for (const auto & c : chunks)
        rows += c.getRows();
    return rows;
}

size_t DiskValueSpace::num_rows(size_t chunks_offset, size_t chunks_length)
{
    size_t rows = 0;
    for (size_t i = chunks_offset; i < chunks_offset + chunks_length; ++i)
        rows += chunks.at(i).getRows();
    return rows;
}

size_t DiskValueSpace::num_deletes()
{
    size_t deletes = 0;
    for (const auto & c : chunks)
        deletes += c.isDeleteRange();
    return deletes;
}

size_t DiskValueSpace::num_bytes()
{
    size_t bytes = 0;
    for (const auto & c : chunks)
        bytes += c.getBytes();
    return bytes;
}

size_t DiskValueSpace::num_chunks()
{
    return chunks.size();
}

void DiskValueSpace::ensurePKColumns(const ColumnDefine & handle, PageStorage & data_storage)
{
    if (!pk_columns && num_rows())
    {
        Block block = read({handle, VERSION_COLUMN_DEFINE}, data_storage, 0, num_rows());
        pk_columns  = {std::move(block), handle};
    }
}

size_t DiskValueSpace::rowsFromBack(size_t chunk_num_from_back)
{
    size_t rows = 0;
    for (ssize_t i = chunks.size() - 1; i >= (ssize_t)(chunks.size() - chunk_num_from_back); --i)
        rows += chunks[i].getRows();
    return rows;
}

size_t DiskValueSpace::cacheRows()
{
    return rowsFromBack(cache_chunks);
}

size_t DiskValueSpace::cacheBytes()
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

std::pair<size_t, size_t> DiskValueSpace::findChunk(size_t rows_offset)
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

std::pair<size_t, size_t> DiskValueSpace::findChunk(size_t rows_offset, size_t deletes_offset)
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

} // namespace DB