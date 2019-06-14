#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/Range.h>

namespace DB
{
// TODO: This class should be replaced by better one which use index to skip irrelevant blocks.
class DMHandleFilterBlockInputStream : public IProfilingBlockInputStream
{
public:
    DMHandleFilterBlockInputStream(const BlockInputStreamPtr & input, const ColumnDefine & handle_define_, HandleRange handle_range_)
        : handle_range(handle_range_), handle_define(handle_define_)
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
            Block res = filter(std::move(block));
            if (!res || !res.rows())
                continue;
            else
                return res;
        }
    }

    Block filter(Block && block)
    {
        size_t rows            = block.rows();
        auto & handle_col_data = getColumnVectorData<Handle>(block, block.getPositionByName(handle_define.name));
        bool   handle_range_include_all
            = handle_range.all() || (handle_range.check(handle_col_data[0]) && handle_range.check(handle_col_data[rows - 1]));
        if (handle_range_include_all)
            return std::move(block);
        bool handle_range_exclude_all
            = handle_range.none() || (handle_col_data[0] >= handle_range.end || handle_col_data[rows - 1] < handle_range.start);
        if (handle_range_exclude_all)
            return {};

        auto   low_it  = std::lower_bound(handle_col_data.cbegin(), handle_col_data.cend(), handle_range.start);
        size_t low_pos = low_it - handle_col_data.cbegin();
        size_t high_pos;
        if (handle_range.check(handle_col_data[rows - 1]))
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

        for (size_t i = 0; i < block.columns(); i++)
        {
            auto & column     = block.getByPosition(i);
            auto   new_column = column.column->cloneEmpty();
            new_column->insertRangeFrom(*column.column, low_pos, high_pos - low_pos);
            column.column = std::move(new_column);
        }
        return std::move(block);
    }

private:
    HandleRange  handle_range;
    ColumnDefine handle_define;
};
} // namespace DB