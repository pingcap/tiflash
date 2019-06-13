#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/Range.h>

namespace DB
{
namespace DM
{

inline Block filterByHandleSorted(const HandleRange & handle_range, Block && block, size_t handle_pos)
{
    size_t rows            = block.rows();
    auto & handle_col_data = getColumnVectorData<Handle>(block, handle_pos);
    auto   first           = handle_col_data[0];
    auto   last            = handle_col_data[rows - 1];

    if (handle_range.include(first, last))
        return std::move(block);
    if (!handle_range.intersect(first, last))
        return {};

    auto   low_it  = std::lower_bound(handle_col_data.cbegin(), handle_col_data.cend(), handle_range.start);
    size_t low_pos = low_it - handle_col_data.cbegin();
    size_t high_pos;
    if (handle_range.check(last))
    {
        high_pos = rows;
    }
    else
    {
        // It mean end of range is not MAX.
        auto high_it = std::lower_bound(low_it, handle_col_data.cend(), handle_range.end);
        high_pos     = high_it - handle_col_data.cbegin();
    }
    if (low_pos >= high_pos)
        return {};
    if (low_pos == 0 && high_pos == rows)
        return std::move(block);

    for (size_t i = 0; i < block.columns(); i++)
    {
        auto & column     = block.getByPosition(i);
        auto   new_column = column.column->cloneEmpty();
        new_column->insertRangeFrom(*column.column, low_pos, high_pos - low_pos);
        column.column = std::move(new_column);
    }
    return std::move(block);
}

inline Block filterByHandleUnsorted(const HandleRange & handle_range, Block && block, size_t handle_pos)
{
    size_t rows            = block.rows();
    auto & handle_col_data = getColumnVectorData<Handle>(block, handle_pos);

    IColumn::Filter filter(rows);
    size_t          passed_count = 0;
    for (size_t i = 0; i < rows - 1; ++i)
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

class DMHandleFilterBlockInputStream : public IProfilingBlockInputStream
{
public:
    DMHandleFilterBlockInputStream(const BlockInputStreamPtr & input,
                                   HandleRange                 handle_range_,
                                   size_t                      handle_col_pos_,
                                   bool                        is_block_sorted_)
        : handle_range(handle_range_), handle_col_pos(handle_col_pos_), is_block_sorted(is_block_sorted_)
    {
        children.push_back(input);
    }

    String getName() const override { return "DeltaMergeHandleFilter"; }
    Block  getHeader() const override { return children.back()->getHeader(); }

protected:
    Block readImpl() override
    {
        while (true)
        {
            Block block = children.back()->read();
            if (!block)
                return {};
            if (!block.rows())
                continue;
            Block res = is_block_sorted ? filterByHandleSorted(handle_range, std::move(block), handle_col_pos)
                                        : filterByHandleUnsorted(handle_range, std::move(block), handle_col_pos);
            if (!res || !res.rows())
                continue;
            else
                return res;
        }
    }

private:
    HandleRange handle_range;
    size_t      handle_col_pos;
    bool        is_block_sorted;
};

} // namespace DM
} // namespace DB