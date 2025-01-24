// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/countBytesInFilter.h>
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
inline std::pair<size_t, size_t> getPosRangeOfSorted(
    const RowKeyRange & rowkey_range,
    const ColumnPtr & rowkey_column,
    const size_t offset,
    const size_t limit)
{
    return rowkey_range.getPosRange(rowkey_column, offset, limit);
}

inline Block cutBlock(Block && block, const std::vector<std::pair<size_t, size_t>> & offset_and_limits)
{
    size_t rows = block.rows();
    if (offset_and_limits.size() == 1)
    {
        auto [offset, limit] = offset_and_limits[0];
        if (!limit)
            return {};
        if (offset == 0 && limit == rows)
            return std::move(block);

        if (offset == 0)
        {
            size_t pop_size = rows - limit;
            for (auto & col : block)
            {
                auto mutate_col = (*std::move(col.column)).mutate();
                mutate_col->popBack(pop_size);
                col.column = std::move(mutate_col);
            }
            if (block.segmentRowIdCol() != nullptr)
            {
                auto mut_col = (*std::move(block.segmentRowIdCol())).mutate();
                mut_col->popBack(pop_size);
                block.setSegmentRowIdCol(std::move(mut_col));
            }
        }
        else
        {
            for (auto & col : block)
            {
                col.column = col.column->cut(offset, limit);
            }
            if (block.segmentRowIdCol() != nullptr)
            {
                block.setSegmentRowIdCol(block.segmentRowIdCol()->cut(offset, limit));
            }
        }
        return std::move(block);
    }
    else
    {
        auto new_columns = block.cloneEmptyColumns();
        MutableColumnPtr new_seg_row_id_col;
        if (block.segmentRowIdCol() != nullptr)
        {
            new_seg_row_id_col = block.segmentRowIdCol()->cloneEmpty();
        }
        for (const auto & [offset, limit] : offset_and_limits)
        {
            if (!limit)
                continue;

            for (size_t i = 0; i < block.columns(); ++i)
            {
                new_columns[i]->insertRangeFrom(*block.getByPosition(i).column, offset, limit);
            }
            if (block.segmentRowIdCol() != nullptr)
            {
                new_seg_row_id_col->insertRangeFrom(*block.segmentRowIdCol(), offset, limit);
            }
        }
        auto new_block = block.cloneWithColumns(std::move(new_columns));
        new_block.setSegmentRowIdCol(std::move(new_seg_row_id_col));
        return new_block;
    }
}

inline Block filterSorted(const RowKeyRanges & rowkey_ranges, Block && block, size_t handle_pos)
{
    if (rowkey_ranges.empty())
        return {};

    RUNTIME_CHECK(handle_pos < block.columns(), handle_pos, block.columns());

    std::vector<std::pair<size_t, size_t>> offset_and_limits;
    offset_and_limits.reserve(rowkey_ranges.size());
    for (const auto & rowkey_range : rowkey_ranges)
    {
        offset_and_limits.emplace_back(
            getPosRangeOfSorted(rowkey_range, block.getByPosition(handle_pos).column, 0, block.rows()));
    }
    if (offset_and_limits.empty())
        return {};

    // try combine adjacent range before cut blocks
    std::sort(
        offset_and_limits.begin(),
        offset_and_limits.end(),
        [](const std::pair<size_t, size_t> & a, const std::pair<size_t, size_t> & b) { return a.first < b.first; });
    size_t current_offset = offset_and_limits[0].first;
    size_t current_limit = offset_and_limits[0].second;
    std::vector<std::pair<size_t, size_t>> combined_offset_and_limits;
    combined_offset_and_limits.reserve(offset_and_limits.size());
    for (size_t i = 1; i < offset_and_limits.size(); i++)
    {
        auto [offset, limit] = offset_and_limits[i];
        if (offset <= current_offset + current_limit)
        {
            current_limit = std::max(current_limit, (offset - current_offset + limit));
        }
        else
        {
            combined_offset_and_limits.emplace_back(current_offset, current_limit);
            current_offset = offset;
            current_limit = limit;
        }
    }
    combined_offset_and_limits.emplace_back(current_offset, current_limit);

    if (combined_offset_and_limits.empty())
        return {};

    return cutBlock(std::move(block), combined_offset_and_limits);
}

inline Block filterUnsorted(const RowKeyRanges & rowkey_ranges, Block && block, size_t handle_pos)
{
    size_t rows = block.rows();
    auto rowkey_column
        = RowKeyColumnContainer(block.getByPosition(handle_pos).column, rowkey_ranges[0].is_common_handle);

    IColumn::Filter filter(rows, 0);
    for (size_t i = 0; i < rows; ++i)
    {
        for (const auto & rowkey_range : rowkey_ranges)
        {
            if (rowkey_range.check(rowkey_column.getRowKeyValue(i)))
            {
                filter[i] = 1;
                break;
            }
        }
    }
    size_t passed_count = countBytesInFilter(filter);

    if (!passed_count)
        return {};
    if (passed_count == rows)
        return std::move(block);

    for (auto & col : block)
    {
        col.column = col.column->filter(filter, passed_count);
    }
    if (block.segmentRowIdCol() != nullptr)
    {
        block.setSegmentRowIdCol(block.segmentRowIdCol()->filter(filter, passed_count));
    }
    return std::move(block);
}
} // namespace RowKeyFilter

/**
  * DMRowKeyFilterBlockInputStream is used to filter block by rowkey ranges.
  * Rows whose rowkey is not in the rowkey ranges will be filtered.
  * Basically, only the rows in first and the last block of the child stream will be filtered.
  */
template <bool is_block_sorted>
class DMRowKeyFilterBlockInputStream : public IBlockInputStream
{
public:
    DMRowKeyFilterBlockInputStream(
        const BlockInputStreamPtr & input,
        const RowKeyRanges & rowkey_ranges_,
        size_t handle_col_pos_)
        : rowkey_ranges(rowkey_ranges_)
        , handle_col_pos(handle_col_pos_)
    {
        children.push_back(input);
    }

    String getName() const override { return "DeltaMergeHandleFilter"; }
    Block getHeader() const override { return children.back()->getHeader(); }

    Block read() override
    {
        while (true)
        {
            Block block = children.back()->read();
            if (!block)
                return {};
            if (!block.rows())
                continue;

            auto rowkey_column
                = RowKeyColumnContainer(block.getByPosition(handle_col_pos).column, rowkey_ranges[0].is_common_handle);
            /// If clean read optimized, only first row's (the smallest) handle is returned as a ColumnConst.
            if (rowkey_column.column->isColumnConst())
            {
                for (const auto & rowkey_range : rowkey_ranges)
                {
                    if (rowkey_range.check(rowkey_column.getRowKeyValue(0)))
                        return block;
                }
                return {};
            }

            Block res;

            if constexpr (is_block_sorted)
                res = RowKeyFilter::filterSorted(rowkey_ranges, std::move(block), handle_col_pos);
            else
                res = RowKeyFilter::filterUnsorted(rowkey_ranges, std::move(block), handle_col_pos);

            if (!res || !res.rows())
                continue;
            else
                return res;
        }
    }

private:
    RowKeyRanges rowkey_ranges;
    size_t handle_col_pos;
};

} // namespace DM
} // namespace DB
