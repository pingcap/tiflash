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
    DeltaValueSpace(size_t handle_col_pos_) : handle_col_pos(handle_col_pos_) {}

    void addBlock(const Block & block, size_t rows)
    {
        blocks.push_back(block);
        sizes.push_back(rows);
    }

    size_t write(const HandleRange & range, MutableColumns & output_columns, size_t offset, size_t limit)
    {
        auto [start_chunk_index, rows_start_in_start_chunk] = findChunk(offset);
        auto [end_chunk_index, rows_end_in_end_chunk]       = findChunk(offset + limit);

        size_t actual_write = 0;
        size_t chunk_index  = start_chunk_index;
        for (; chunk_index <= end_chunk_index && chunk_index < sizes.size(); ++chunk_index)
        {
            size_t rows_start_in_chunk = chunk_index == start_chunk_index ? rows_start_in_start_chunk : 0;
            size_t rows_end_in_chunk   = chunk_index == end_chunk_index ? rows_end_in_end_chunk : sizes[chunk_index];
            size_t rows_in_chunk_limit = rows_end_in_chunk - rows_start_in_chunk;

            // TODO: block should be loaded by demand.
            auto & block           = blocks[chunk_index];
            auto & handle_col_data = toColumnVectorData<Handle>(block.getByPosition(handle_col_pos).column);
            if (rows_end_in_chunk > rows_start_in_chunk)
            {
                if (rows_in_chunk_limit == 1)
                {
                    if (range.check(handle_col_data[rows_start_in_chunk]))
                    {
                        ++actual_write;
                        for (size_t col_index = 0; col_index < output_columns.size(); ++col_index)
                            output_columns[col_index]->insertFrom(*(block.getByPosition(col_index).column), rows_start_in_chunk);
                    }
                }
                else
                {
                    auto [actual_offset, actual_limit]
                        = HandleFilter::getPosRangeOfSorted(range, handle_col_data, rows_start_in_chunk, rows_in_chunk_limit);
                    actual_write += actual_limit;
                    for (size_t col_index = 0; col_index < output_columns.size(); ++col_index)
                        output_columns[col_index]->insertRangeFrom(*(block.getByPosition(col_index).column), actual_offset, actual_limit);
                }
            }
        }
        return actual_write;
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
    size_t              handle_col_pos;
    Blocks              blocks;
    std::vector<size_t> sizes;
};

using DeltaValueSpacePtr = std::shared_ptr<DeltaValueSpace>;

} // namespace DM
} // namespace DB