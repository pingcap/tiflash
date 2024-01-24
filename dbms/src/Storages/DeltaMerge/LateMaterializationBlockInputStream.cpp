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

#include <Columns/ColumnsCommon.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/LateMaterializationBlockInputStream.h>


namespace DB::DM
{

LateMaterializationBlockInputStream::LateMaterializationBlockInputStream(
    const ColumnDefines & columns_to_read,
    BlockInputStreamPtr filter_column_stream_,
    std::shared_ptr<ConcatSkippableBlockInputStream<false>> rest_column_stream_,
    const BitmapFilterPtr & bitmap_filter_,
    UInt64 filter_column_max_block_rows_)
    : header(toEmptyBlock(columns_to_read))
    , filter_column_stream(std::move(filter_column_stream_))
    , rest_column_stream(std::move(rest_column_stream_))
    , bitmap_filter(bitmap_filter_)
    , filter_column_max_block_rows(filter_column_max_block_rows_)
{
    start_offset_each_stream.resize(rest_column_stream->getRows().size() + 1);
    std::partial_sum(
        rest_column_stream->getRows().begin(),
        rest_column_stream->getRows().end(),
        start_offset_each_stream.begin() + 1);
}

Block LateMaterializationBlockInputStream::readImpl()
{
    size_t passed_count = countBytesInFilter(filter);
    size_t total_read_rows = filter.size();

    while (true)
    {
        FilterPtr block_filter = nullptr;
        auto filter_column_block = filter_column_stream->read(block_filter, true);
        // If filter_column_block is empty, it means that the stream has ended.
        if (!filter_column_block)
            break;

        if (filter_column_block.startOffset() >= start_offset_each_stream[current_stream_index + 1])
        {
            // When filter_column_block.startOffset() > start_offset_each_stream[current_stream_index + 1],
            // it means that the filter_column_stream has been switched to the next stream.
            // If there is no rows passed in the previous stream, clear the blocks and filter, and continue to read the next stream.
            if (passed_count == 0)
            {
                blocks.clear();
                filter.clear();
                total_read_rows = 0;
                rest_column_stream->switchToNextStream();
            }
        }

        filter.resize(total_read_rows + filter_column_block.rows(), 1);
        if (block_filter)
        {
            std::copy(block_filter->begin(), block_filter->end(), filter.begin() + total_read_rows);
            passed_count += countBytesInFilter(*block_filter);
        }
        else
        {
            // If the filter is empty, it means that all rows in the block are passed.
            passed_count += filter_column_block.rows();
        }
        total_read_rows += filter_column_block.rows();
        blocks.emplace_back(std::move(filter_column_block));
        if (passed_count >= filter_column_max_block_rows)
            break;
        if (current_stream_index < start_offset_each_stream.size() - 1
            && blocks.back().startOffset() >= start_offset_each_stream[current_stream_index + 1])
            break;
    }

    if (blocks.empty())
        return {};

    size_t offset = blocks.front().startOffset();
    Block filter_column_block;
    IColumn::Filter block_filter;
    if (blocks.size() > 1
        // one block can be larger than filter_column_max_block_rows, in this case, do not need to back up.
        && ((current_stream_index < start_offset_each_stream.size() - 1
             && blocks.back().startOffset() >= start_offset_each_stream[current_stream_index + 1])
            || passed_count >= filter_column_max_block_rows))
    {
        // need to back up the last block
        auto next_read_block = std::move(blocks.back());
        blocks.pop_back();
        filter_column_block = vstackBlocks<Blocks::const_iterator, false>(blocks.cbegin(), blocks.cend());
        blocks.clear();
        blocks.emplace_back(std::move(next_read_block));

        total_read_rows = filter.size() - blocks.back().rows();
        block_filter.reserve(blocks.back().rows());
        std::move(filter.begin() + total_read_rows, filter.end(), std::back_inserter(block_filter));
        filter.resize(total_read_rows);

        if (current_stream_index < start_offset_each_stream.size() - 1
            && blocks.back().startOffset() >= start_offset_each_stream[current_stream_index + 1])
            ++current_stream_index;
    }
    else
    {
        if (current_stream_index < start_offset_each_stream.size() - 1
            && blocks.back().startOffset() >= start_offset_each_stream[current_stream_index + 1])
            ++current_stream_index;

        filter_column_block = vstackBlocks<Blocks::const_iterator, false>(blocks.cbegin(), blocks.cend());
        blocks.clear();
    }
    std::swap(filter, block_filter);
    // bitmap_filter[start_offset, start_offset + rows] & filter -> filter
    bitmap_filter->rangeAnd(block_filter, offset, total_read_rows);
    passed_count = countBytesInFilter(block_filter);
    for (auto & col : filter_column_block)
        col.column = col.column->filter(block_filter, passed_count);

    auto rest_column_block = rest_column_stream->readWithFilter(block_filter);
    RUNTIME_CHECK_MSG(
        rest_column_block.startOffset() == offset && rest_column_block.rows() == passed_count,
        "Late materialization meets unexpected block unmatched, filter_column_block: [start_offset={}, "
        "rows={}], rest_column_block: [start_offset={}, rows={}], total_read_rows={}",
        offset,
        total_read_rows,
        rest_column_block.startOffset(),
        rest_column_block.rows(),
        total_read_rows);
    return hstackBlocks({std::move(filter_column_block), std::move(rest_column_block)}, header);
}

} // namespace DB::DM
