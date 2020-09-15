#pragma once

#include <Columns/ColumnConst.h>
#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/RowKeyRange.h>

namespace DB
{
namespace DM
{

namespace RowKeyFilter
{

/// return <offset, limit>
inline std::pair<size_t, size_t>
getPosRangeOfSorted(const RowKeyRange & rowkey_range, const ColumnPtr & rowkey_column, const size_t offset, const size_t limit)
{
    return rowkey_range.getPosRange(rowkey_column, offset, limit);
}

inline Block cutBlock(Block && block, size_t offset, size_t limit)
{
    size_t rows = block.rows();
    if (!limit)
        return {};
    if (offset == 0 && limit == rows)
        return std::move(block);

    if (offset == 0)
    {
        size_t pop_size = rows - limit;
        for (size_t i = 0; i < block.columns(); i++)
        {
            auto & column     = block.getByPosition(i);
            auto   mutate_col = (*std::move(column.column)).mutate();
            mutate_col->popBack(pop_size);
            column.column = std::move(mutate_col);
        }
    }
    else
    {
        for (size_t i = 0; i < block.columns(); i++)
        {
            auto & column     = block.getByPosition(i);
            auto   new_column = column.column->cloneEmpty();
            new_column->insertRangeFrom(*column.column, offset, limit);
            column.column = std::move(new_column);
        }
    }
    return std::move(block);
}

inline Block filterSorted(const RowKeyRange & rowkey_range, Block && block, size_t handle_pos)
{
    auto [offset, limit] = getPosRangeOfSorted(rowkey_range, block.getByPosition(handle_pos).column, 0, block.rows());
    return cutBlock(std::move(block), offset, limit);
}

inline Block filterUnsorted(const RowKeyRange & rowkey_range, Block && block, size_t handle_pos)
{
    size_t rows          = block.rows();
    auto   rowkey_column = RowKeyColumnContainer(block.getByPosition(handle_pos).column, rowkey_range.is_common_handle);

    IColumn::Filter filter(rows);
    size_t          passed_count = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        bool ok   = rowkey_range.check(rowkey_column.getRowKeyValue(i));
        filter[i] = ok;
        passed_count += ok;
    }

    if (!passed_count)
        return {};
    if (passed_count == rows)
        return std::move(block);

    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto & column = block.getByPosition(i);
        column.column = column.column->filter(filter, passed_count);
    }
    return std::move(block);
}
} // namespace RowKeyFilter

template <bool is_block_sorted>
class DMRowKeyFilterBlockInputStream : public IBlockInputStream
{
public:
    DMRowKeyFilterBlockInputStream(const BlockInputStreamPtr & input, const RowKeyRange & rowkey_range_, size_t handle_col_pos_)
        : rowkey_range(rowkey_range_), handle_col_pos(handle_col_pos_)
    {
        children.push_back(input);
    }

    String getName() const override { return "DeltaMergeHandleFilter"; }
    Block  getHeader() const override { return children.back()->getHeader(); }

    Block read() override
    {
        while (true)
        {
            Block block = children.back()->read();
            if (!block)
                return {};
            if (!block.rows())
                continue;

            auto rowkey_column = RowKeyColumnContainer(block.getByPosition(handle_col_pos).column, rowkey_range.is_common_handle);
            /// If clean read optimized, only first row's (the smallest) handle is returned as a ColumnConst.
            if (rowkey_column.column->isColumnConst())
            {
                if (rowkey_range.check(rowkey_column.getRowKeyValue(0)))
                    return block;
                else
                    return {};
            }

            Block res = is_block_sorted ? RowKeyFilter::filterSorted(rowkey_range, std::move(block), handle_col_pos)
                                        : RowKeyFilter::filterUnsorted(rowkey_range, std::move(block), handle_col_pos);
            if (!res || !res.rows())
                continue;
            else
                return res;
        }
    }

private:
    RowKeyRange rowkey_range;
    size_t      handle_col_pos;
};

} // namespace DM
} // namespace DB
