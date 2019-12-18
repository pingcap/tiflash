#pragma once

#include <common/logger_useful.h>

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Core/SortDescription.h>

#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/Chunk.h>
#include <Storages/DeltaMerge/ChunkBlockInputStream.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaValueSpace.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/Page/WriteBatch.h>

namespace DB
{
namespace DM
{


struct AppendTask
{
    bool   append_cache; // If not append cache, then clear cache.
    size_t remove_chunks_back;
    Chunks append_chunks;
};
using AppendTaskPtr = std::shared_ptr<AppendTask>;

class DeltaValueSpace
{
public:
    DeltaValueSpace() {}

    void addBlock(const Block & block, size_t rows)
    {
        blocks.push_back(block);
        sizes.push_back(rows);
    }

    size_t write(MutableColumns & output_columns, size_t offset, size_t limit)
    {
        auto [start_chunk_index, rows_start_in_start_chunk] = findChunk(offset);
        auto [end_chunk_index, rows_end_in_end_chunk]       = findChunk(offset + limit);

        size_t actually_read = 0;
        size_t chunk_index   = start_chunk_index;
        for (; chunk_index <= end_chunk_index && chunk_index < sizes.size(); ++chunk_index)
        {
            size_t rows_start_in_chunk = chunk_index == start_chunk_index ? rows_start_in_start_chunk : 0;
            size_t rows_end_in_chunk   = chunk_index == end_chunk_index ? rows_end_in_end_chunk : sizes[chunk_index];
            size_t rows_in_chunk_limit = rows_end_in_chunk - rows_start_in_chunk;

            auto & block = blocks[chunk_index];
            // Empty block means we don't need to read.
            if (rows_end_in_chunk > rows_start_in_chunk && block)
            {
                for (size_t col_index = 0; col_index < output_columns.size(); ++col_index)
                {
                    if (rows_in_chunk_limit == 1)
                        output_columns[col_index]->insertFrom(*(block.getByPosition(col_index).column), rows_start_in_chunk);
                    else
                        output_columns[col_index]->insertRangeFrom(*(block.getByPosition(col_index).column), //
                                                                   rows_start_in_chunk,
                                                                   rows_in_chunk_limit);
                }

                actually_read += rows_in_chunk_limit;
            }
        }
        return actually_read;
    }

private:
    inline std::pair<size_t, size_t> findChunk(size_t offset)
    {
        size_t rows = 0;
        for (size_t block_id = 0; block_id < sizes.size(); ++block_id)
        {
            rows += sizes[block_id];
            if (rows > offset)
                return {block_id, sizes[block_id] - (rows - offset)};
        }
        return {sizes.size(), 0};
    }

private:
    Blocks              blocks;
    std::vector<size_t> sizes;
};
using DeltaValueSpacePtr = std::shared_ptr<DeltaValueSpace>;

class DiskValueSpace;
using DiskValueSpacePtr = std::shared_ptr<DiskValueSpace>;

class DiskValueSpace
{
public:
    struct OpContext
    {
        static OpContext createForLogStorage(DMContext & context)
        {
            return OpContext{
                .dm_context       = context,
                .data_storage     = context.storage_pool.log(),
                .meta_storage     = context.storage_pool.meta(),
                .gen_data_page_id = std::bind(&StoragePool::newLogPageId, &context.storage_pool),
            };
        }

        static OpContext createForDataStorage(DMContext & context)
        {
            return OpContext{
                .dm_context       = context,
                .data_storage     = context.storage_pool.data(),
                .meta_storage     = context.storage_pool.meta(),
                .gen_data_page_id = std::bind(&StoragePool::newDataPageId, &context.storage_pool),
            };
        }

        const DMContext & dm_context;

        PageStorage & data_storage;
        PageStorage & meta_storage;
        GenPageId     gen_data_page_id;
    };

    DiskValueSpace(bool should_cache_, PageId page_id_, Chunks && chunks_ = {}, MutableColumnMap && cache_ = {}, size_t cache_chunks_ = 0);
    DiskValueSpace(const DiskValueSpace & other);

    /// Called after the instance is created from existing metadata.
    void restore(const OpContext & context);

    AppendTaskPtr     createAppendTask(const OpContext & context, WriteBatches & wbs, const BlockOrDelete & update) const;
    DiskValueSpacePtr applyAppendTask(const OpContext & context, const AppendTaskPtr & task, const BlockOrDelete & update);

    /// Write the blocks from sorted_input_stream into underlying storage, the returned chunks can be added to
    /// specified value space instance by #setChunks or #appendChunkWithCache later.
    static Chunks writeChunks(const OpContext & context, const BlockInputStreamPtr & sorted_input_stream, WriteBatch & wb);

    static Chunk writeDelete(const OpContext & context, const HandleRange & delete_range);

    /// Remove all chunks and clear cache.
    void replaceChunks(WriteBatch &        meta_wb, //
                       WriteBatch &        removed_wb,
                       Chunks &&           new_chunks,
                       MutableColumnMap && cache_,
                       size_t              cache_chunks_);
    void replaceChunks(WriteBatch & meta_wb, WriteBatch & removed_wb, Chunks && new_chunks);
    void clearChunks(WriteBatch & removed_wb);
    void setChunks(WriteBatch & meta_wb, Chunks && new_chunks);
    void setChunksAndCache(WriteBatch & meta_wb, Chunks && new_chunks, MutableColumnMap && cache_, size_t cache_chunks_);

    /// Append the chunk to this value space, and could cache the block in memory if it is too fragment.
    void appendChunkWithCache(const OpContext & context, Chunk && chunk, const Block & block);

    /// Read the requested chunks' data and compact into a block.
    /// The columns of the returned block are guaranteed to be in order of read_columns.
    Block read(const ColumnDefines & read_columns,
               const PageReader &    page_reader,
               size_t                rows_offset,
               size_t                rows_limit,
               std::optional<size_t> reserve_rows = {}) const;

    /// Read the chunk data.
    /// The columns of the returned block are guaranteed to be in order of read_columns.
    Block read(const ColumnDefines & read_columns, const PageReader & page_reader, size_t chunk_index) const;

    /// The data of returned block is in insert order.
    BlockOrDeletes getMergeBlocks(const ColumnDefine & handle,
                                  const PageReader &   page_reader,
                                  size_t               rows_begin,
                                  size_t               deletes_begin,
                                  size_t               rows_end,
                                  size_t               deletes_end) const;

    DiskValueSpacePtr tryFlushCache(const OpContext & context, WriteBatch & remove_data_wb, bool force = false);

    // TODO: getInputStream can be removed
    ChunkBlockInputStreamPtr getInputStream(const ColumnDefines & read_columns, const PageReader & page_reader) const;

    DeltaValueSpacePtr getValueSpace(const PageReader &    page_reader, //
                                     const ColumnDefines & read_columns,
                                     const HandleRange &   range,
                                     size_t                rows_limit) const;

    MutableColumnMap cloneCache();
    size_t           cacheChunks() { return cache_chunks; }

    size_t num_rows() const;
    size_t num_rows(size_t chunks_offset, size_t chunk_length) const;
    size_t num_deletes() const;
    size_t num_bytes() const;
    size_t num_chunks() const;

    size_t cacheRows() const;
    size_t cacheBytes() const;

    PageId         pageId() const { return page_id; }
    const Chunks & getChunks() const { return chunks; }
    Chunks         getChunksBefore(size_t rows, size_t deletes) const;
    Chunks         getChunksAfter(size_t rows, size_t deletes) const;

    void check(const PageReader & meta_page_reader, const String & when);

private:
    DiskValueSpacePtr doFlushCache(const OpContext & context, WriteBatch & remove_data_wb);

    size_t rowsFromBack(size_t chunks) const;

    /// Return (chunk_index, offset_in_chunk)
    std::pair<size_t, size_t> findChunk(size_t rows) const;
    std::pair<size_t, size_t> findChunk(size_t rows, size_t deletes) const;

private:
    bool is_delta_vs;

    // page_id and chunks are the only vars needed to persist.
    PageId page_id;
    Chunks chunks;

    // The cache is mainly used to merge fragment chunks.
    MutableColumnMap cache;
    size_t           cache_chunks = 0;

    Logger * log;
};

} // namespace DM
} // namespace DB
