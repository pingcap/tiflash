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
    BlockOrDelete() = default;
    BlockOrDelete(Block && block_) : block(block_) {}
    BlockOrDelete(const HandleRange & delete_range_) : delete_range(delete_range_) {}

    Block       block;
    HandleRange delete_range;
};
using BlockOrDeletes = std::vector<BlockOrDelete>;

struct WriteBatches
{
    WriteBatch log;
    WriteBatch data;
    WriteBatch meta;

    Ids writtenLog;
    Ids writtenData;

    WriteBatch removed_log;
    WriteBatch removed_data;
    WriteBatch removed_meta;

    void writeLogAndData(StoragePool & storage_pool)
    {
        storage_pool.log().write(log);
        storage_pool.data().write(data);

        for (auto & w : log.getWrites())
            writtenLog.push_back(w.page_id);
        for (auto & w : data.getWrites())
            writtenData.push_back(w.page_id);

        log.clear();
        data.clear();
    }

    void rollbackWrittenLogAndData(StoragePool & storage_pool)
    {
        WriteBatch log_wb;
        for (auto p : writtenLog)
            log_wb.delPage(p);
        WriteBatch data_wb;
        for (auto p : writtenData)
            data_wb.delPage(p);

        storage_pool.log().write(log_wb);
        storage_pool.data().write(data_wb);
    }

    void writeMeta(StoragePool & storage_pool)
    {
        storage_pool.meta().write(meta);
        meta.clear();
    }

    void writeRemoves(StoragePool & storage_pool)
    {
        storage_pool.log().write(removed_log);
        storage_pool.data().write(removed_data);
        storage_pool.meta().write(removed_meta);

        removed_log.clear();
        removed_data.clear();
        removed_meta.clear();
    }

    void writeAll(StoragePool & storage_pool)
    {
        writeLogAndData(storage_pool);
        writeMeta(storage_pool);
        writeRemoves(storage_pool);
    }
};

struct AppendTask
{
    bool   append_cache; // If not append cache, then clear cache.
    size_t remove_chunks_back;
    Chunks append_chunks;
};
using AppendTaskPtr = std::shared_ptr<AppendTask>;

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

    DiskValueSpace(bool should_cache_, PageId page_id_);
    DiskValueSpace(bool should_cache_, PageId page_id_, const Chunks & chunks_);
    DiskValueSpace(const DiskValueSpace & other);

    /// Called after the instance is created from existing metadata.
    void restore(const OpContext & context);

    AppendTaskPtr     createAppendTask(const OpContext & context, WriteBatches & wbs, const BlockOrDelete & update) const;
    DiskValueSpacePtr applyAppendTask(const OpContext & context, const AppendTaskPtr & task, const BlockOrDelete & update);

    /// Write the blocks from input_stream into underlying storage, the returned chunks can be added to
    /// specified value space instance by #setChunks or #appendChunkWithCache later.
    static Chunks writeChunks(const OpContext & context, const BlockInputStreamPtr & input_stream, WriteBatch & wb);

    static Chunk writeDelete(const OpContext & context, const HandleRange & delete_range);

    /// Replace current chunks.
    void setChunks(Chunks && new_chunks, WriteBatch & meta_wb, WriteBatch & data_wb_remove);

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

    ChunkBlockInputStreamPtr getInputStream(const ColumnDefines & read_columns, const PageReader & page_reader) const;

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