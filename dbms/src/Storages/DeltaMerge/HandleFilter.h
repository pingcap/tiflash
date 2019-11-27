#pragma once

#include <Columns/ColumnConst.h>
#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/Range.h>

namespace DB
{
namespace DM
{

namespace HandleFilter
{

/// return <offset, limit>
inline std::pair<size_t, size_t> getPosRangeOfSorted(const HandleRange &            handle_range,
                                                     const PaddedPODArray<Handle> & handle_col_data,
                                                     const size_t                   offset,
                                                     const size_t                   limit)
{
    const auto begin_it    = handle_col_data.cbegin() + offset;
    const auto end_it      = begin_it + limit;
    const auto first_value = handle_col_data[offset];
    const auto last_value  = handle_col_data[offset + limit - 1];

    const auto low_it  = handle_range.check(first_value) ? begin_it : std::lower_bound(begin_it, end_it, handle_range.start);
    const auto high_it = handle_range.check(last_value) ? end_it : std::lower_bound(low_it, end_it, handle_range.end);

    size_t  low_pos   = low_it - handle_col_data.cbegin();
    ssize_t res_limit = high_it - low_it;

    return {low_pos, res_limit};
}

/// return <offset, limit>
inline std::pair<size_t, size_t>
getPosRangeOfSorted(const HandleRange & handle_range, const ColumnPtr & handle_column, const size_t offset, const size_t limit)
{
    return getPosRangeOfSorted(handle_range, toColumnVectorData<Handle>(handle_column), offset, limit);
}

inline Block filterSorted(const HandleRange & handle_range, Block && block, size_t handle_pos)
{
    size_t rows          = block.rows();
    auto [offset, limit] = getPosRangeOfSorted(handle_range, block.getByPosition(handle_pos).column, 0, rows);
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

inline Block filterUnsorted(const HandleRange & handle_range, Block && block, size_t handle_pos)
{
    size_t rows            = block.rows();
    auto & handle_col_data = getColumnVectorData<Handle>(block, handle_pos);

    IColumn::Filter filter(rows);
    size_t          passed_count = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        bool ok   = handle_range.check(handle_col_data[i]);
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
} // namespace HandleFilter

template <bool is_block_sorted>
class DMHandleFilterBlockInputStream : public IBlockInputStream
{
public:
    DMHandleFilterBlockInputStream(const BlockInputStreamPtr & input, HandleRange handle_range_, size_t handle_col_pos_)
        : handle_range(handle_range_), handle_col_pos(handle_col_pos_)
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

            auto handle_column = block.getByPosition(handle_col_pos).column;
            if (handle_column->isColumnConst())
            {
                if (handle_range.check(handle_column->getInt(0)))
                    return block;
                else
                    return {};
            }

            Block res = is_block_sorted ? HandleFilter::filterSorted(handle_range, std::move(block), handle_col_pos)
                                        : HandleFilter::filterUnsorted(handle_range, std::move(block), handle_col_pos);
            if (!res || !res.rows())
                continue;
            else
                return res;
        }
    }

private:
    HandleRange handle_range;
    size_t      handle_col_pos;
};

} // namespace DM
} // namespace DB