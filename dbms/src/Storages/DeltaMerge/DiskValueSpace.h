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
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/Page/WriteBatch.h>

namespace DB
{
namespace DM
{

struct BlockOrRange
{
    Block       block;
    HandleRange delete_range;
};
using BlockOrRanges = std::vector<BlockOrRange>;

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

    DiskValueSpace(bool should_cache_, PageId page_id_);
    DiskValueSpace(bool should_cache_, PageId page_id_, const Chunks & chunks_);

    void swap(DiskValueSpace & other);

    /// Called after
    void restore(const OpContext & context);

    /// Write the blocks from input_stream into underlying storage, the returned chunks can be added to
    /// specified value space instance by #prepareSetChunk + #commitSetChunks or #appendChunkWithCache later.
    static Chunks writeChunks(const OpContext & context, const BlockInputStreamPtr & input_stream);

    static Chunk writeDelete(const OpContext & context, const HandleRange & delete_range);

    /// Replace current chunks.
    void setChunks(Chunks && new_chunks, WriteBatch & meta_wb, WriteBatch & data_wb);

    /// Append the chunk to this value space, and could cache the block in memory if it is too fragment.
    void appendChunkWithCache(const OpContext & context, Chunk && chunk, const Block & block);

    /// Read the requested chunks' data and compact into a block.
    /// For convenient, the columns of the block are guaranteed to be in order of read_columns.
    Block read(const ColumnDefines & read_columns, PageStorage & data_storage, size_t rows_offset, size_t rows_limit);

    /// Read the chunk data
    /// For convenient, the columns of the block are guaranteed to be in order of read_columns.
    Block read(const ColumnDefines & read_columns, PageStorage & data_storage, size_t chunk_index);

    /// The data of returned block is in insert order.
    BlockOrRanges getMergeBlocks(const ColumnDefine & handle, PageStorage & data_storage, size_t rows_offset, size_t deletes_offset);


    ChunkBlockInputStreamPtr getInputStream(const ColumnDefines & read_columns, PageStorage & data_storage);

    bool tryFlushCache(const OpContext & context, bool force = false);

    size_t num_rows();
    size_t num_rows(size_t chunks_offset, size_t chunk_length);
    size_t num_deletes();
    size_t num_bytes();
    size_t num_chunks();

    PageId         pageId() const { return page_id; }
    const Chunk &  getChunk(size_t index) const { return chunks.at(index); }
    const Chunks & getChunks() { return chunks; }

private:
    bool doFlushCache(const OpContext & context);

    size_t rowsFromBack(size_t chunks);
    size_t cacheRows();
    size_t cacheBytes();
    /// Return (chunk_index, offset_in_chunk)
    std::pair<size_t, size_t> findChunk(size_t rows);
    std::pair<size_t, size_t> findChunk(size_t rows, size_t deletes);

private:
    // page_id and chunks are the only vars needed to persisted.
    bool   should_cache;
    PageId page_id;
    Chunks chunks;

    // The cache is mainly used to merge fragment chunks.
    MutableColumnMap cache;
    size_t           cache_chunks = 0;

    Logger * log;
};

} // namespace DM
} // namespace DB