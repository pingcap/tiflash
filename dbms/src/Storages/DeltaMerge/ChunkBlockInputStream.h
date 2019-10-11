#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

#include <Storages/DeltaMerge/Chunk.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/HandleFilter.h>

namespace DB
{
namespace DM
{
class ChunkBlockInputStream final : public IBlockInputStream
{
public:
    ChunkBlockInputStream(const Chunks &        chunks_,
                          const ColumnDefines & read_columns_,
                          const PageReader &    page_reader_,
                          const RSOperatorPtr & filter)
        : chunks(chunks_), skip_chunks(chunks.size(), 0), read_columns(read_columns_), page_reader(page_reader_)
    {
        if (filter)
        {
            for (size_t i = 0; i < chunks.size(); ++i)
            {
                auto &       chunk = chunks[i];
                RSCheckParam param;
                for (auto & [col_id, meta] : chunk.getMetas())
                    param.indexes.emplace(col_id, RSIndex(meta.type, meta.minmax));
                skip_chunks[i] = filter->roughCheck(param) == None;
            }
        }
    }

    String getName() const override { return "Chunk"; }
    Block  getHeader() const override { return toEmptyBlock(read_columns); }

    Block read() override
    {
        if (!hasNext())
            return {};
        return readChunk(chunks[cur_chunk_index++], read_columns, page_reader);
    }

    bool   hasNext() { return cur_chunk_index < chunks.size(); }
    size_t nextRows() { return chunks[cur_chunk_index].getRows(); }

    bool shouldSkipNext() { return skip_chunks[cur_chunk_index]; }
    void skipNext() { ++cur_chunk_index; }

private:
    Chunks             chunks;
    std::vector<UInt8> skip_chunks;

    ColumnDefines read_columns;
    PageReader    page_reader;

    size_t cur_chunk_index = 0;
};

using ChunkBlockInputStreamPtr = std::shared_ptr<ChunkBlockInputStream>;

} // namespace DM
} // namespace DB