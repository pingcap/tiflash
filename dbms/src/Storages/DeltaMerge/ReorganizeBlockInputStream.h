#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>

namespace DB
{
namespace DM
{

/// Reorganize the boundary of blocks.
/// Note that child must be a sorted input stream with increasing pk column.
class ReorganizeBlockInputStream final : public IBlockInputStream
{
public:
    ReorganizeBlockInputStream(BlockInputStreamPtr child, String pk_column_name_)
        : sorted_input_stream(std::move(child)), pk_column_name(std::move(pk_column_name_))
    {
        assert(sorted_input_stream != nullptr);
        cur_block = {};
        if constexpr (DM_RUN_CHECK)
        {
            // Sanity check for existence of pk column
            Block header = sorted_input_stream->getHeader();
            if (!header.has(pk_column_name))
            {
                throw Exception("Try to write block to Pack without pk column", ErrorCodes::LOGICAL_ERROR);
            }
            is_common_handle = !header.getByName(pk_column_name).type->isInteger();
        }
    }

    String getName() const override { return "ReorganizeBlockBoundary"; }
    Block  getHeader() const override { return sorted_input_stream->getHeader(); }

    Block read() override
    {
        if (first_read)
        {
            next_block = DB::DM::readNextBlock(sorted_input_stream);
            first_read = false;
        }

        cur_block = next_block;
        if (!cur_block)
            return cur_block;

        while (true)
        {
            next_block = DB::DM::readNextBlock(sorted_input_stream);

            const size_t cut_offset = findCutOffsetInNextBlock(cur_block, next_block, pk_column_name, is_common_handle);
            if (unlikely(cut_offset == 0))
                // There is no pk overlap between `cur_block` and `next_block`, or `next_block` is empty, just return `cur_block`.
                return cur_block;
            else
            {
                const size_t next_block_nrows = next_block.rows();
                for (size_t col_idx = 0; col_idx != cur_block.columns(); ++col_idx)
                {
                    auto & cur_col_with_name  = cur_block.getByPosition(col_idx);
                    auto & next_col_with_name = next_block.getByPosition(col_idx);
                    auto * cur_col_raw        = const_cast<IColumn *>(cur_col_with_name.column.get());
                    cur_col_raw->insertRangeFrom(*next_col_with_name.column, 0, cut_offset);

                    if (cut_offset != next_block_nrows)
                    {
                        // TODO: we can track the valid range instead of copying data.
                        size_t nrows_to_copy = next_block_nrows - cut_offset;
                        // Pop front `cut_offset` elems from `next_col_with_name`
                        assert(next_block_nrows == next_col_with_name.column->size());
                        MutableColumnPtr cutted_next_column = next_col_with_name.column->cloneEmpty();
                        cutted_next_column->insertRangeFrom(*next_col_with_name.column, cut_offset, nrows_to_copy);
                        next_col_with_name.column = cutted_next_column->getPtr();
                    }
                }
                if (cut_offset != next_block_nrows)
                {
                    // We merge some rows to `cur_block`, return it.
                    return cur_block;
                }
                // else we merge all rows from `next_block` to `cur_block`, continue to check if we should merge more blocks.
            }
        }
    }

private:
    static size_t
    findCutOffsetInNextBlock(const Block & cur_block, const Block & next_block, const String & pk_column_name, bool is_common_handle)
    {
        assert(cur_block);
        if (!next_block)
            return 0;

        auto                  cur_col = cur_block.getByName(pk_column_name).column;
        RowKeyColumnContainer cur_rowkey_column(cur_col, is_common_handle);
        const auto            last_curr_pk = cur_rowkey_column.getRowKeyValue(cur_col->size() - 1);
        auto                  next_col     = next_block.getByName(pk_column_name).column;
        RowKeyColumnContainer next_rowkey_column(next_col, is_common_handle);
        size_t                cut_offset = 0;
        for (/* */; cut_offset < next_col->size(); ++cut_offset)
        {
            const auto next_pk = next_rowkey_column.getRowKeyValue(cut_offset);
            if (compare(next_pk, last_curr_pk) != 0)
            {
                if constexpr (DM_RUN_CHECK)
                {
                    if (unlikely(next_pk < last_curr_pk))
                        throw Exception("InputStream is not sorted, pk in next block is smaller than current block: "
                                            + next_pk.toDebugString() + " < " + last_curr_pk.toDebugString(),
                                        ErrorCodes::LOGICAL_ERROR);
                }
                break;
            }
        }
        return cut_offset;
    }

private:
    BlockInputStreamPtr sorted_input_stream;
    const String        pk_column_name;
    bool                is_common_handle;

    Block cur_block;
    Block next_block;

    bool first_read = true;
};

} // namespace DM
} // namespace DB
