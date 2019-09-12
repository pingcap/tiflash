#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

#include <Storages/DeltaMerge/Chunk.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>

namespace DB
{
namespace DM
{
/// Read `chunks` as blocks according to `read_columns`
class ChunkBlockInputStream final : public IBlockInputStream
{
public:
    ChunkBlockInputStream(const Chunks &        chunks_,
                          const RSOperatorPtr & filter,
                          const ColumnDefines & read_columns_,
                          const PageReader &    page_reader_)
        : chunks(chunks_), skip_chunks(chunks.size()), read_columns(read_columns_), page_reader(page_reader_)
    {
        for (size_t i = 0; i < chunks.size(); ++i)
        {
            if (!filter)
            {
                skip_chunks[i] = 0;
                continue;
            }
            auto &       chunk = chunks[i];
            RSCheckParam param;
            for (auto & [col_id, meta] : chunk.getMetas())
                param.indexes.emplace(col_id, RSIndex(meta.type, meta.minmax));

            skip_chunks[i] = filter->roughCheck(param) == None;
        }
    }

    String getName() const override { return "Chunk"; }
    Block  getHeader() const override { return toEmptyBlock(read_columns); }

    Block read() override
    {
        if (!hasNext())
            return {};
        return readChunk(chunks[chunk_index++], read_columns, page_reader);
    }

    bool   hasNext() { return chunk_index < chunks.size(); }
    size_t nextRows() { return chunks[chunk_index].getRows(); }
    bool   shouldSkipNext() { return skip_chunks[chunk_index]; }
    void   skipNext() { ++chunk_index; }

private:
    Chunks             chunks;
    std::vector<UInt8> skip_chunks;

    size_t        chunk_index = 0;
    ColumnDefines read_columns;
    PageReader    page_reader;
    Block         header;
};

using ChunkBlockInputStreamPtr = std::shared_ptr<ChunkBlockInputStream>;

} // namespace DM
} // namespace DB