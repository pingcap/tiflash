#pragma once

#include <common/logger_useful.h>

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Core/SortDescription.h>

#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/Chunk.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/Page/WriteBatch.h>

namespace DB
{

using GenPageId = std::function<PageId()>;

struct BlockOrRange
{
    Block       block;
    HandleRange delete_range;
};
using BlockOrRanges = std::vector<BlockOrRange>;

struct PKColumns
{
    ColumnPtr handle_column;
    ColumnPtr version_column;

    // We don't like to do copy each time update those columns, so let's do a little hack here.
    IColumn * handle_column_raw  = nullptr;
    IColumn * version_column_raw = nullptr;

    PKColumns() {}

    PKColumns(const ColumnPtr & handle_column, const ColumnPtr & version_column)
        : handle_column(handle_column),
          version_column(version_column),
          handle_column_raw(const_cast<IColumn *>(handle_column.get())),
          version_column_raw(const_cast<IColumn *>(version_column.get()))
    {
    }

    PKColumns(const PKColumns & from, size_t offset, size_t limit)
        : handle_column(from.handle_column->cloneEmpty()),
          version_column(from.version_column->cloneEmpty()),
          handle_column_raw(const_cast<IColumn *>(handle_column.get())),
          version_column_raw(const_cast<IColumn *>(version_column.get()))
    {
        handle_column_raw->insertRangeFrom(*from.handle_column, offset, limit);
        version_column_raw->insertRangeFrom(*from.version_column, offset, limit);
    }

    PKColumns(Block && block, const ColumnDefine & handle)
    {
        if (block)
        {
            handle_column  = block.getByName(handle.name).column;
            version_column = block.getByName(VERSION_COLUMN_NAME).column;
        }
        else
        {
            handle_column  = handle.type->createColumn();
            version_column = VERSION_COLUMN_TYPE->createColumn();
        }

        handle_column_raw  = const_cast<IColumn *>(handle_column.get());
        version_column_raw = const_cast<IColumn *>(version_column.get());
    }

    void swap(PKColumns & other)
    {
        handle_column.swap(other.handle_column);
        version_column.swap(other.version_column);

        std::swap(handle_column_raw, other.handle_column_raw);
        std::swap(version_column_raw, other.version_column_raw);
    }

    void append(const Block & block, const ColumnDefine & handle)
    {
        if (!handle_column)
        {
            handle_column  = handle.type->createColumn();
            version_column = VERSION_COLUMN_TYPE->createColumn();

            handle_column_raw  = const_cast<IColumn *>(handle_column.get());
            version_column_raw = const_cast<IColumn *>(version_column.get());
        }
        handle_column_raw->insertRangeFrom(*block.getByName(handle.name).column, 0, block.rows());
        version_column_raw->insertRangeFrom(*block.getByName(VERSION_COLUMN_NAME).column, 0, block.rows());
    }

    Block toBlock(const ColumnDefine & handle) const
    {
        return {createColumnWithTypeAndName(handle_column, handle.type, handle.name, handle.id),
                createColumnWithTypeAndName(version_column, VERSION_COLUMN_TYPE, VERSION_COLUMN_NAME, VERSION_COLUMN_ID)};
    }

    size_t rows() const { return handle_column->size(); }

    explicit operator bool() { return bool(handle_column); }
    bool     operator!() { return !bool(handle_column); }
};

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

    class DVSBlockInputStream;
    BlockInputStreamPtr getInputStream(const ColumnDefines & read_columns, PageStorage & data_storage);

    const PKColumns & getPKColumns(const ColumnDefine & handle, PageStorage & data_storage);

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
    void ensurePKColumns(const ColumnDefine & handle, PageStorage & data_storage);

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

    PKColumns pk_columns;

    // The cache is mainly used to merge fragment chunks.
    MutableColumnMap cache;
    size_t           cache_chunks = 0;
};

} // namespace DB