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
        auto [start_pack_index, rows_start_in_start_pack] = findPack(offset);
        auto [end_pack_index, rows_end_in_end_pack]       = findPack(offset + limit);

        size_t actually_read = 0;
        size_t pack_index   = start_pack_index;
        for (; pack_index <= end_pack_index && pack_index < sizes.size(); ++pack_index)
        {
            size_t rows_start_in_pack = pack_index == start_pack_index ? rows_start_in_start_pack : 0;
            size_t rows_end_in_pack   = pack_index == end_pack_index ? rows_end_in_end_pack : sizes[pack_index];
            size_t rows_in_pack_limit = rows_end_in_pack - rows_start_in_pack;

            auto & block = blocks[pack_index];
            // Empty block means we don't need to read.
            if (rows_end_in_pack > rows_start_in_pack && block)
            {
                for (size_t col_index = 0; col_index < output_columns.size(); ++col_index)
                {
                    auto & col = *(block.getByPosition(col_index).column);
                    if (rows_in_pack_limit == 1)
                        output_columns[col_index]->insertFrom(col, rows_start_in_pack);
                    else
                        output_columns[col_index]->insertRangeFrom(col, rows_start_in_pack, rows_in_pack_limit);
                }

                actually_read += rows_in_pack_limit;
            }
        }
        return actually_read;
    }

private:
    inline std::pair<size_t, size_t> findPack(size_t offset)
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