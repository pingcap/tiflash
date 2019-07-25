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

struct BlockOrDelete
{
    BlockOrDelete(Block && block_) : block(block_) {}
    BlockOrDelete(const HandleRange & delete_range_) : delete_range(delete_range_) {}

    Block       block;
    HandleRange delete_range;
};
using BlockOrDeletes = std::vector<BlockOrDelete>;

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

    struct AppendTask
    {
        /// The write order of the following wirte batch is critical!

        WriteBatch data_write_batch;
        WriteBatch meta_write_batch;

        WriteBatch data_remove_write_batch;

        bool   append_cache; // If not append cache, then clear cache.
        size_t remove_chunk_back;
        Chunks append_chunks;
    };
    using AppendTaskPtr = std::unique_ptr<AppendTask>;

    DiskValueSpace(bool should_cache_, PageId page_id_);
    DiskValueSpace(bool should_cache_, PageId page_id_, const Chunks & chunks_);
    DiskValueSpace(const DiskValueSpace & other);

    void swap(DiskValueSpace & other);

    /// Called after the instance is created from existing metadata.
    void restore(const OpContext & context);

    AppendTaskPtr createAppendTask(const OpContext & context, const BlockOrDelete & block_or_delete) const;

    void applyAppendTask(const OpContext & context, const AppendTaskPtr & task, const BlockOrDelete & block_or_delete);

    /// Write the blocks from input_stream into underlying storage, the returned chunks can be added to
    /// specified value space instance by #setChunks or #appendChunkWithCache later.
    static Chunks writeChunks(const OpContext & context, const BlockInputStreamPtr & input_stream);

    static Chunk writeDelete(const OpContext & context, const HandleRange & delete_range);

    /// Replace current chunks.
    void setChunks(Chunks && new_chunks, WriteBatch & meta_wb, WriteBatch & data_wb);

    /// Append the chunk to this value space, and could cache the block in memory if it is too fragment.
    void appendChunkWithCache(const OpContext & context, Chunk && chunk, const Block & block);

    /// Read the requested chunks' data and compact into a block.
    /// The columns of the returned block are guaranteed to be in order of read_columns.
    Block read(const ColumnDefines & read_columns,
               PageStorage &         data_storage,
               size_t                rows_offset,
               size_t                rows_limit,
               std::optional<size_t> reserve_rows = {}) const;

    /// Read the chunk data.
    /// The columns of the returned block are guaranteed to be in order of read_columns.
    Block read(const ColumnDefines & read_columns, PageStorage & data_storage, size_t chunk_index) const;

    /// The data of returned block is in insert order.
    BlockOrDeletes getMergeBlocks(const ColumnDefine & handle,
                                  PageStorage &        data_storage,
                                  size_t               rows_begin,
                                  size_t               deletes_begin,
                                  size_t               rows_end,
                                  size_t               deletes_end) const;

    ChunkBlockInputStreamPtr getInputStream(const ColumnDefines & read_columns, PageStorage & data_storage) const;

    bool tryFlushCache(const OpContext & context, bool force = false);

    size_t num_rows() const;
    size_t num_rows(size_t chunks_offset, size_t chunk_length) const;
    size_t num_deletes() const;
    size_t num_bytes() const;
    size_t num_chunks() const;

    PageId         pageId() const { return page_id; }
    const Chunks & getChunks() const { return chunks; }

private:
    bool doFlushCache(const OpContext & context);

    size_t rowsFromBack(size_t chunks) const;
    size_t cacheRows() const;
    size_t cacheBytes() const;

    /// Return (chunk_index, offset_in_chunk)
    std::pair<size_t, size_t> findChunk(size_t rows) const;
    std::pair<size_t, size_t> findChunk(size_t rows, size_t deletes) const;

private:
    bool should_cache;

    // page_id and chunks are the only vars needed to persist.
    PageId page_id;
    Chunks chunks;

    // The cache is mainly used to merge fragment chunks.
    MutableColumnMap cache;
    size_t           cache_chunks = 0;

    Logger * log;
};

using DiskValueSpacePtr = std::shared_ptr<DiskValueSpace>;

} // namespace DM
} // namespace DB