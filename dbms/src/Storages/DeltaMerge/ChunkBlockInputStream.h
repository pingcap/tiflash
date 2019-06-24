#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

#include <Storages/DeltaMerge/Chunk.h>

namespace DB
{
namespace DM
{
class ChunkBlockInputStream final : public IBlockInputStream
{
public:
    ChunkBlockInputStream(const Chunks & chunks_, const ColumnDefines & read_columns_, PageStorage & data_storage_)
        : chunks(chunks_), read_columns(read_columns_), data_storage(data_storage_)
    {
    }

    String getName() const override { return "Chunk"; }
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

    Block read() override
    {
        if (chunk_index >= chunks.size())
            return {};
        return readChunk(chunks[chunk_index++], read_columns, data_storage);
    }

    bool       hasNextBlock() { return chunk_index < chunks.size(); }
    HandlePair nextBlockHandle() { return chunks[chunk_index].getHandleFirstLast(); }
    size_t     nextBlockRows() { return chunks[chunk_index].getRows(); }
    void       skipNextBlock() { ++chunk_index; }

private:
    const Chunks & chunks;
    size_t         chunk_index = 0;
    ColumnDefines  read_columns;
    PageStorage &  data_storage;
    Block          header;
};

using ChunkBlockInputStreamPtr = std::shared_ptr<ChunkBlockInputStream>;

} // namespace DM
} // namespace DB