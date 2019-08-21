#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

#include <Storages/DeltaMerge/Chunk.h>

namespace DB
{
namespace DM
{
/// Read `chunks` as blocks according to `read_columns`
class ChunkBlockInputStream final : public IBlockInputStream
{
public:
    ChunkBlockInputStream(const Chunks & chunks_, const ColumnDefines & read_columns_, PageStorage & data_storage_)
        : chunks(chunks_), read_columns(read_columns_), data_storage(data_storage_)
    {
    }

    String getName() const override { return "Chunk"; }
    Block  getHeader() const override { return toEmptyBlock(read_columns); }

    Block read() override
    {
        if (!hasNextBlock())
            return {};
        return readChunk(chunks[chunk_index++], read_columns, data_storage);
    }

    bool       hasNextBlock() { return chunk_index < chunks.size(); }
    HandlePair nextBlockHandle() { return chunks[chunk_index].getHandleFirstLast(); }
    size_t     nextBlockRows() { return chunks[chunk_index].getRows(); }
    void       skipNextBlock() { ++chunk_index; }

private:
    Chunks        chunks;
    size_t        chunk_index = 0;
    ColumnDefines read_columns;
    PageStorage & data_storage;
    Block         header;
};

using ChunkBlockInputStreamPtr = std::shared_ptr<ChunkBlockInputStream>;

} // namespace DM
} // namespace DB