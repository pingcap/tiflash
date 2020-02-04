#pragma once

#include <Columns/IColumn.h>
#include <Core/Block.h>

namespace DB
{
namespace DM
{

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
                    auto & col = *(block.getByPosition(col_index).column);
                    if (rows_in_chunk_limit == 1)
                        output_columns[col_index]->insertFrom(col, rows_start_in_chunk);
                    else
                        output_columns[col_index]->insertRangeFrom(col, rows_start_in_chunk, rows_in_chunk_limit);
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

} // namespace DM
} // namespace DB