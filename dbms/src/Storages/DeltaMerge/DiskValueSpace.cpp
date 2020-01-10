#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>

#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DiskValueSpace.h>
#include <Storages/DeltaMerge/ReorganizeBlockInputStream.h>
#include <Storages/Page/PageStorage.h>

#include <IO/WriteHelpers.h>
#include <ext/scope_guard.h>

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

DiskValueSpace::DiskValueSpace(bool is_delta_vs_, PageId page_id_, Chunks && chunks_, MutableColumnMap && cache_, size_t cache_chunks_)
    : is_delta_vs(is_delta_vs_),
      page_id(page_id_),
      chunks(std::move(chunks_)),
      cache(std::move(cache_)),
      cache_chunks(cache_chunks_),
      log(&Logger::get("DiskValueSpace"))
{
}

DiskValueSpace::DiskValueSpace(const DiskValueSpace & other)
    : is_delta_vs(other.is_delta_vs), //
      page_id(other.page_id),
      chunks(other.chunks),
      cache_chunks(other.cache_chunks),
      log(other.log)
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
    if (is_delta_vs && !chunks.empty())
    {
        size_t chunks_to_cache = 0;
        for (ssize_t i = chunks.size() - 1; i >= 0; --i)
        {
            const auto & chunk = chunks[i];
            // We can not cache a chunk which is a delete range and not the latest one.
            if (chunk.isDeleteRange() && i != (ssize_t)chunks.size() - 1)
                break;
            if (chunk.getRows() >= context.dm_context.delta_cache_limit_rows)
                break;
            ++chunks_to_cache;
        }

        // Load cache into memory.
        size_t     total_rows = num_rows();
        size_t     cache_rows = rowsFromBack(chunks_to_cache);
        PageReader page_reader(context.data_storage);
        Block      cache_data = read(context.dm_context.store_columns, page_reader, total_rows - cache_rows, cache_rows);
        if (unlikely(cache_data.rows() != cache_rows))
            throw Exception("The fragment rows from storage mismatch");

        for (const auto & col_define : context.dm_context.store_columns)
        {
            ColumnWithTypeAndName & col = cache_data.getByName(col_define.name);
            cache[col_define.id]        = (*std::move(col.column)).mutate();
        }
        // Note: cache_chunks must be set after #read() .
        cache_chunks = chunks_to_cache;
    }

    // Try flush now, in case of last flush failure recover.
    WriteBatch remove_data_wb;
    auto       new_instance = tryFlushCache(context, remove_data_wb);
    if (new_instance)
    {
        chunks.swap(new_instance->chunks);
        cache.swap(new_instance->cache);
        cache_chunks = new_instance->cache_chunks;
    }
    context.data_storage.write(remove_data_wb);
}


AppendTaskPtr DiskValueSpace::createAppendTask(const OpContext & context, WriteBatches & wbs, const BlockOrDelete & update) const
{
    assert(is_delta_vs == true);

    auto task = std::make_unique<AppendTask>();

    auto & append_block = update.block;
    auto & delete_range = update.delete_range;

    const bool   is_delete       = !append_block;
    const size_t append_rows     = is_delete ? 0 : append_block.rows();
    const size_t cache_rows      = cacheRows();
    const size_t total_rows      = num_rows();
    const size_t in_storage_rows = total_rows - cache_rows;

    Chunk delete_chunk = is_delete ? Chunk(delete_range) : Chunk();

    // If the newly appended object is a delete_range, then we must clear the cache and flush them.

    if (is_delta_vs && !is_delete //
        && (cache_rows + append_rows) < context.dm_context.delta_cache_limit_rows)
    {
        // Simply put the newly appended block into cache.
        Chunk chunk = prepareChunkDataWrite(context.dm_context, context.gen_data_page_id, wbs.log, append_block);

        task->append_cache       = true;
        task->remove_chunks_back = 0;

        task->append_chunks.emplace_back(std::move(chunk));
    }
    else
    {
        // Flush cache.

        task->append_cache       = false;
        task->remove_chunks_back = cache_chunks;

        if (!cache_chunks)
        {
            // There are no caches, simple write the newly block or delete_range is enough.
            Chunk chunk
                = is_delete ? delete_chunk : prepareChunkDataWrite(context.dm_context, context.gen_data_page_id, wbs.log, append_block);

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
                    wbs.removed_log.delPage(col_meta.page_id);
                }
            }

            Block  compacted_block;
            size_t compacted_rows = cache_rows + append_rows;
            if (cache.empty())
            {
                // TODO: Currently in write thread, we do not use snapshot read. This may need to refactor later.

                PageReader page_reader(context.data_storage);
                // Load fragment chunks' data from disk.
                compacted_block = read(context.dm_context.store_columns, //
                                       page_reader,
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
                for (const auto & col_define : context.dm_context.store_columns)
                {
                    auto new_col = col_define.type->createColumn();
                    new_col->reserve(compacted_rows);
                    new_col->insertRangeFrom(*cache.at(col_define.id), 0, cache_rows);
                    if (!is_delete)
                        new_col->insertRangeFrom(*append_block.getByName(col_define.name).column, 0, append_rows);

                    ColumnWithTypeAndName col(std::move(new_col), col_define.type, col_define.name, col_define.id);
                    compacted_block.insert(std::move(col));
                }
            }

            Chunk compacted_chunk = prepareChunkDataWrite(context.dm_context, context.gen_data_page_id, wbs.log, compacted_block);

            task->append_chunks.emplace_back(std::move(compacted_chunk));
            if (is_delete)
                task->append_chunks.emplace_back(delete_chunk);
        }
    }

    return task;
}

void DiskValueSpace::applyAppendToWriteBatches(const AppendTaskPtr & task, WriteBatches & wbs)
{
    assert(is_delta_vs == true);
    // Serialize meta updates and return by wbs.meta, later we will commit them in DeltaMergeStore level.
    MemoryWriteBuffer buf(0, CHUNK_SERIALIZE_BUFFER_SIZE);
    serializeChunks(buf, chunks.begin(), chunks.begin() + (chunks.size() - task->remove_chunks_back), task->append_chunks);
    const auto data_size = buf.count();
    wbs.meta.putPage(page_id, 0, buf.tryGetReadBuffer(), data_size);
}

DiskValueSpacePtr DiskValueSpace::applyAppendTask(const OpContext & context, const AppendTaskPtr & task, const BlockOrDelete & update)
{
    assert(is_delta_vs == true);

    // Apply in memory.
    DiskValueSpace * instance = this;
    if (task->remove_chunks_back > 0)
    {
        // If any chunks got removed, then we create a new instance.
        // Make sure a reference to an instance is read constantly by rows.
        Chunks new_chunks(chunks.begin(), chunks.end());
        instance = new DiskValueSpace(is_delta_vs, page_id, std::move(new_chunks));
        instance->chunks.resize(chunks.size() - task->remove_chunks_back);
    }

    for (auto & chunk : task->append_chunks)
        instance->chunks.emplace_back(std::move(chunk));

    if (task->append_cache)
    {
        if (unlikely(instance != this))
            throw Exception("cache should only be applied to this");

        auto block_rows = update.block.rows();
        for (const auto & col_define : context.dm_context.store_columns)
        {
            const ColumnWithTypeAndName & col = update.block.getByName(col_define.name);

            auto it = instance->cache.find(col_define.id);
            if (it == instance->cache.end())
                instance->cache.emplace(col_define.id, col_define.type->createColumn());

            instance->cache[col_define.id]->insertRangeFrom(*col.column, 0, block_rows);
        }
        ++instance->cache_chunks;
    }
    else
    {
        instance->cache.clear();
        instance->cache_chunks = 0;
    }

    if (instance != this)
        return DiskValueSpacePtr(instance);
    else
        return {};
}

Chunks DiskValueSpace::writeChunks(const OpContext & context, const BlockInputStreamPtr & sorted_input_stream, WriteBatch & wb)
{
    // Cleanning up the boundary between blocks.
    const String &             pk_column_name = context.dm_context.handle_column.name;
    ReorganizeBlockInputStream stream(sorted_input_stream, pk_column_name);

    Chunks chunks;
    while (true)
    {
        Block block = stream.read();
        if (!block)
            break;
        Chunk chunk = prepareChunkDataWrite(context.dm_context, context.gen_data_page_id, wb, block);
        chunks.push_back(std::move(chunk));
    }

    if constexpr (DM_RUN_CHECK)
    {
        //  Sanity check the boundary between different chunks is not overlap
        if (chunks.size() > 1)
        {
            for (size_t i = 1; i < chunks.size(); ++i)
            {
                const Chunk & prev = chunks[i - 1];
                const Chunk & curr = chunks[i];
                if (prev.isDeleteRange() || curr.isDeleteRange())
                {
                    throw Exception("Unexpected DeleteRange in stable inputstream. prev:" + prev.info() + " curr: " + curr.info(),
                                    ErrorCodes::LOGICAL_ERROR);
                }

                const HandlePair prev_handle = prev.getHandleFirstLast();
                const HandlePair curr_handle = curr.getHandleFirstLast();
                // pk should be increasing and no overlap between chunks
                if (prev_handle.second >= curr_handle.first)
                {
                    throw Exception("Overlap chunks between " + prev.info() + " and " + curr.info(), ErrorCodes::LOGICAL_ERROR);
                }
            }
        }
    }
    return chunks;
}

Chunk DiskValueSpace::writeDelete(const OpContext &, const HandleRange & delete_range)
{
    return Chunk(delete_range);
}

void DiskValueSpace::clearChunks(WriteBatch & removed_wb)
{
    for (const auto & c : chunks)
    {
        for (const auto & m : c.getMetas())
            removed_wb.delPage(m.second.page_id);
    }

    Chunks tmp;
    chunks.swap(tmp);

    cache.clear();
    cache_chunks = 0;
}

void DiskValueSpace::replaceChunks(
    WriteBatch & meta_wb, WriteBatch & removed_wb, Chunks && new_chunks, MutableColumnMap && cache_, size_t cache_chunks_)
{
    MemoryWriteBuffer buf(0, CHUNK_SERIALIZE_BUFFER_SIZE);
    serializeChunks(buf, new_chunks.begin(), new_chunks.end(), {});
    auto data_size = buf.count();
    meta_wb.putPage(page_id, 0, buf.tryGetReadBuffer(), data_size);

    for (const auto & c : chunks)
    {
        for (const auto & m : c.getMetas())
            removed_wb.delPage(m.second.page_id);
    }

    chunks.swap(new_chunks);

    cache        = std::move(cache_);
    cache_chunks = cache_chunks_;
}

void DiskValueSpace::replaceChunks(WriteBatch & meta_wb, WriteBatch & removed_wb, Chunks && new_chunks)
{
    replaceChunks(meta_wb, removed_wb, std::move(new_chunks), {}, 0);
}

void DiskValueSpace::setChunks(WriteBatch & meta_wb, Chunks && new_chunks)
{
    setChunksAndCache(meta_wb, std::move(new_chunks), {}, 0);
}

void DiskValueSpace::setChunksAndCache(WriteBatch & meta_wb, Chunks && new_chunks, MutableColumnMap && cache_, size_t cache_chunks_)
{
    MemoryWriteBuffer buf(0, CHUNK_SERIALIZE_BUFFER_SIZE);
    serializeChunks(buf, new_chunks.begin(), new_chunks.end(), {});
    auto data_size = buf.count();
    meta_wb.putPage(page_id, 0, buf.tryGetReadBuffer(), data_size);

    cache        = std::move(cache_);
    cache_chunks = cache_chunks_;
}

void DiskValueSpace::appendChunkWithCache(const OpContext & context, Chunk && chunk, const Block & block)
{
    // should only append to delta
    assert(is_delta_vs);
    {
        MemoryWriteBuffer buf(0, CHUNK_SERIALIZE_BUFFER_SIZE);
        serializeChunks(buf, chunks.begin(), chunks.end(), &chunk);
        auto       data_size = buf.count();
        WriteBatch meta_wb;
        meta_wb.putPage(page_id, 0, buf.tryGetReadBuffer(), data_size);
        context.meta_storage.write(meta_wb);
    }

    chunks.push_back(std::move(chunk));

    auto write_rows = block.rows();
    if (!write_rows)
    {
        // If it is an empty chunk, then nothing to append.
        ++cache_chunks;
        return;
    }

    // If former cache is empty, and this chunk is big enough, then no need to cache.
    if (cache_chunks == 0 && write_rows >= context.dm_context.delta_cache_limit_rows)
        return;

    for (const auto & col_define : context.dm_context.store_columns)
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
                           const PageReader &    page_reader,
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
                    columns, read_column_defines, cur_chunk, page_reader, rows_start_in_chunk, rows_end_in_chunk - rows_start_in_chunk);

                already_read_rows += rows_end_in_chunk - rows_start_in_chunk;
            }
        }
    }

    /// The reset of data is in cache, simply append them into result.

    if (already_read_rows < rows_limit)
    {
        // TODO We do flush each time in `StorageDeltaMerge::alterImpl`, so that there is only the data with newest schema in cache. We ignore either new inserted col nor col type changed in cache for now.

        // chunk_index could be larger than chunk_cache_start.
        size_t cache_rows_offset = 0;
        for (size_t i = chunk_cache_start; i < chunk_index; ++i)
            cache_rows_offset += chunks[i].getRows();

        for (size_t index = 0; index < read_column_defines.size(); ++index)
        {
            const ColumnDefine & define    = read_column_defines[index];
            auto &               cache_col = cache.at(define.id); // TODO new inserted col'id don't exist in cache.

            size_t rows_offset_in_chunk = chunk_index == start_chunk_index ? rows_start_in_start_chunk : 0;

            // TODO columns[index].type maybe not consisted with cache_col after ddl.
            columns[index]->insertRangeFrom(*cache_col, cache_rows_offset + rows_offset_in_chunk, rows_limit - already_read_rows);
        }
    }

    Block res;
    for (size_t index = 0; index < read_column_defines.size(); ++index)
    {
        const ColumnDefine &  define = read_column_defines[index];
        ColumnWithTypeAndName col(std::move(columns[index]), define.type, define.name, define.id);
        res.insert(std::move(col));
    }
    return res;
}

Block DiskValueSpace::read(const ColumnDefines & read_column_defines, const PageReader & page_reader, size_t chunk_index) const
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
            readChunkData(columns, read_column_defines, chunk, page_reader, 0, chunk.getRows());
        }
        else
        {
            // Read from cache

            // TODO We do flush each time in `StorageDeltaMerge::alterImpl`, so that there is only the data with newest schema in cache. We ignore either new inserted col nor col type changed in cache for now.
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
        ColumnWithTypeAndName col(std::move(columns[index]), define.type, define.name, define.id);
        res.insert(std::move(col));
    }
    return res;
}

BlockOrDeletes DiskValueSpace::getMergeBlocks(const ColumnDefine & handle,
                                              const PageReader &   page_reader,
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
            if (block_rows_end != block_rows_start)
                res.emplace_back(
                    read({handle, getVersionColumnDefine()}, page_reader, block_rows_start, block_rows_end - block_rows_start));
            if (chunk.isDeleteRange())
                res.emplace_back(chunk.getDeleteRange());

            block_rows_start = block_rows_end;
        }
    }

    return res;
}

DiskValueSpacePtr DiskValueSpace::tryFlushCache(const OpContext & context, WriteBatch & remove_data_wb, bool force)
{
    if (cache_chunks == 0)
        return {};

    // If last chunk is a delete range, we should flush cache.
    HandleRange delete_range = chunks.back().isDeleteRange() ? chunks.back().getDeleteRange() : HandleRange::newNone();
    if (!delete_range.none())
        force = true;

    const size_t cache_rows = cacheRows();
    if (!force && cache_rows < context.dm_context.delta_cache_limit_rows)
        return {};

    return doFlushCache(context, remove_data_wb);
}

DiskValueSpacePtr DiskValueSpace::doFlushCache(const OpContext & context, WriteBatch & remove_data_wb)
{
    const size_t cache_rows      = cacheRows();
    const size_t total_rows      = num_rows();
    const size_t in_storage_rows = total_rows - cache_rows;

    HandleRange delete_range = chunks.back().isDeleteRange() ? chunks.back().getDeleteRange() : HandleRange::newNone();

    if (cache_chunks <= 1)
    {
        // One chunk no need to compact.
        cache.clear();
        cache_chunks = 0;
        return {};
    }

    // Create an new instance.
    Chunks tmp_chunks(chunks.begin(), chunks.end());
    auto   new_instance = std::make_shared<DiskValueSpace>(is_delta_vs, page_id, std::move(tmp_chunks));

    EventRecorder recorder(ProfileEvents::DMFlushDeltaCache, ProfileEvents::DMFlushDeltaCacheNS);

    LOG_DEBUG(log, "Start flush cache");

    /// Flush cache to disk and replace the fragment chunks.

    WriteBatch data_wb_insert;
    WriteBatch meta_wb;

    size_t cache_start = chunks.size() - cache_chunks;
    for (size_t i = cache_start; i < chunks.size(); ++i)
    {
        auto & old_chunk = chunks[i];
        for (const auto & [col_id, col_meta] : old_chunk.getMetas())
        {
            (void)col_id;
            remove_data_wb.delPage(col_meta.page_id);
        }
    }

    Block compacted;
    if (cache.empty())
    {
        // Load fragment data from disk.
        PageReader page_reader(context.data_storage);
        compacted = read(context.dm_context.store_columns, page_reader, in_storage_rows, cache_rows);

        if (unlikely(compacted.rows() != cache_rows))
            throw Exception("The fragment rows from storage mismatch");
    }
    else
    {
        // Use the cache.
        for (const auto & col_define : context.dm_context.store_columns)
        {
            ColumnWithTypeAndName col(cache.at(col_define.id)->cloneResized(cache_rows), col_define.type, col_define.name, col_define.id);
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

    new_instance->chunks.swap(new_chunks);
    new_instance->cache.clear();
    new_instance->cache_chunks = 0;

    // ============================================================

    recorder.submit();

    LOG_DEBUG(log, "Done flush cache");

    return new_instance;
}

ChunkBlockInputStreamPtr DiskValueSpace::getInputStream(const ColumnDefines & read_columns, const PageReader & page_reader) const
{
    return std::make_shared<ChunkBlockInputStream>(chunks, read_columns, page_reader, RSOperatorPtr());
}

DeltaValueSpacePtr DiskValueSpace::getValueSpace(const PageReader &    page_reader,
                                                 const ColumnDefines & read_columns,
                                                 const HandleRange &   range,
                                                 size_t                rows_limit) const
{
    auto mvs = std::make_shared<DeltaValueSpace>();

    size_t chunk_cache_start = chunks.size() - cache_chunks;
    size_t already_read_rows = 0;
    size_t chunk_index       = 0;
    for (; chunk_index < chunk_cache_start && chunk_index < chunks.size(); ++chunk_index)
    {
        auto & chunk = chunks[chunk_index];
        if (chunk.isDeleteRange() || !chunk.getRows())
            continue;
#if 0
        // FIXME: Disable filter since we need to use all values to build DeltaTree.
        auto & handle_meta            = chunk.getColumn(EXTRA_HANDLE_COLUMN_ID);
        auto [min_handle, max_handle] = handle_meta.minmax->getIntMinMax(0);
        if (range.intersect(min_handle, max_handle))
            mvs->addBlock(read(read_columns, page_reader, chunk_index), chunk.getRows());
        else
            mvs->addBlock({}, chunk.getRows());
#else
        (void)range;
        mvs->addBlock(read(read_columns, page_reader, chunk_index), chunk.getRows());
#endif

        already_read_rows += chunk.getRows();
        if (already_read_rows >= rows_limit)
            break;
    }

    /// The reset of data is in cache, simply append them into result.

    if (already_read_rows < rows_limit)
    {
        Block block;
        auto  cache_limit = rows_limit - already_read_rows;
        for (size_t index = 0; index < read_columns.size(); ++index)
        {
            const ColumnDefine & define    = read_columns[index];
            auto &               cache_col = cache.at(define.id); // TODO new inserted col'id don't exist in cache.
            auto                 clone     = cache_col->cloneResized(cache_limit);
            block.insert(ColumnWithTypeAndName(std::move(clone), define.type, define.name, define.id));
        }
        mvs->addBlock(block, cache_limit);
    }

    return mvs;
}

Chunks DiskValueSpace::getChunksBefore(size_t rows, size_t deletes) const
{
    auto [chunk_index, offset_in_chunk] = findChunk(rows, deletes);
    if (offset_in_chunk != 0)
        throw Exception("offset_in_chunk is expected to be zero");
    return Chunks(chunks.begin(), chunks.begin() + chunk_index);
}

Chunks DiskValueSpace::getChunksAfter(size_t rows, size_t deletes) const
{
    auto [chunk_index, offset_in_chunk] = findChunk(rows, deletes);
    if (offset_in_chunk != 0)
        throw Exception("offset_in_chunk is expected to be zero");
    return Chunks(chunks.begin() + chunk_index, chunks.end());
}

void DiskValueSpace::check(const PageReader & meta_page_reader, const String & when)
{
    MemoryWriteBuffer buf(0, CHUNK_SERIALIZE_BUFFER_SIZE);
    serializeChunks(buf, chunks.begin(), chunks.end());
    auto   size        = buf.count();
    char * data_buffer = (char *)::malloc(size);
    SCOPE_EXIT({ ::free(data_buffer); });
    auto read_buf = buf.tryGetReadBuffer();
    read_buf->readStrict(data_buffer, size);
    auto page_checksum = CityHash_v1_0_2::CityHash64(data_buffer, size);
    if (meta_page_reader.getPageChecksum(page_id) != page_checksum)
    {
        auto                 page = meta_page_reader.read(page_id);
        ReadBufferFromMemory rb(page.data.begin(), page.data.size());
        auto                 disk_chunks = deserializeChunks(rb);
        throw Exception(when + ", DiskValueSpace [" + DB::toString(page_id) + "] memory and disk content not match, memory: "
                        + DB::toString(chunks.size()) + ", disk: " + DB::toString(disk_chunks.size()));
    }
}

MutableColumnMap DiskValueSpace::cloneCache()
{
    MutableColumnMap clone_cache;
    for (auto & [col_id, col_cache] : cache)
    {
        clone_cache.emplace(col_id, col_cache->cloneResized(col_cache->size()));
    }
    return clone_cache;
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
