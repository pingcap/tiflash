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
#include <Storages/DeltaMerge/ReadUtil.h>


namespace DB::DM
{

namespace
{

void filterFilterColumnBlock(
    const Block & header,
    Block & block,
    const FilterPtr & filter,
    size_t passed_count,
    const String & filter_column_name)
{
    ColumnPtr filter_column;
    for (auto & col : block)
    {
        if (col.name == filter_column_name)
        {
            filter_column = col.column;
            continue;
        }
        col.column = col.column->filter(*filter, passed_count);
    }
    if (header.has(filter_column_name))
        filter_column = filter_column->filter(*filter, passed_count);
}

} // namespace

LateMaterializationBlockInputStream::LateMaterializationBlockInputStream(
    const ColumnDefines & columns_to_read,
    const String & filter_column_name_,
    BlockInputStreamPtr filter_column_stream_,
    SkippableBlockInputStreamPtr rest_column_stream_,
    const BitmapFilterPtr & bitmap_filter_,
    const String & req_id_)
    : header(toEmptyBlock(columns_to_read))
    , filter_column_name(filter_column_name_)
    , filter_column_stream(std::move(filter_column_stream_))
    , rest_column_stream(std::move(rest_column_stream_))
    , bitmap_filter(bitmap_filter_)
    , log(Logger::get(NAME, req_id_))
{}

Block LateMaterializationBlockInputStream::read()
{
    Block filter_column_block;
    FilterPtr filter = nullptr;

    // Until non-empty block after filtering or end of stream.
    while (true)
    {
        filter_column_block = filter_column_stream->read(filter, true);

        // If filter_column_block is empty, it means that the stream has ended.
        // No need to read the rest_column_stream, just return an empty block.
        if (!filter_column_block)
            return filter_column_block;

        // If filter is nullptr, it means that these push down filters are always true.
        if (!filter)
        {
            IColumn::Filter col_filter;
            col_filter.resize(filter_column_block.rows());
            Block rest_column_block;
            if (bitmap_filter->get(col_filter, filter_column_block.startOffset(), filter_column_block.rows()))
            {
                rest_column_block = rest_column_stream->read();
            }
            else
            {
                rest_column_block = rest_column_stream->read();
                size_t passed_count = countBytesInFilter(col_filter);
                for (auto & col : rest_column_block)
                {
                    col.column = col.column->filter(col_filter, passed_count);
                }
                for (auto & col : filter_column_block)
                {
                    col.column = col.column->filter(col_filter, passed_count);
                }
            }
            return hstackBlocks({std::move(filter_column_block), std::move(rest_column_block)}, header);
        }

        size_t rows = filter_column_block.rows();
        // bitmap_filter[start_offset, start_offset + rows] & filter -> filter
        bitmap_filter->rangeAnd(*filter, filter_column_block.startOffset(), rows);

        if (size_t passed_count = countBytesInFilter(*filter); passed_count == 0)
        {
            // if all rows are filtered, skip the next block of rest_column_stream
            if (size_t skipped_rows = rest_column_stream->skipNextBlock(); skipped_rows == 0)
            {
                // if we fail to skip, we need to call read() of rest_column_stream, but ignore the result
                // NOTE: skipNextBlock() return 0 only if failed to skip or meets the end of stream,
                //       but the filter_column_stream doesn't meet the end of stream
                //       so it is an unexpected behavior.
                rest_column_stream->read();
                LOG_ERROR(
                    log,
                    "Late materialization skip block failed, at start_offset: {}, rows: {}",
                    filter_column_block.startOffset(),
                    filter_column_block.rows());
            }
        }
        else
        {
            Block rest_column_block;
            auto filter_out_count = rows - passed_count;
            if (filter_out_count >= DEFAULT_MERGE_BLOCK_SIZE * 2)
            {
                // When DEFAULT_MERGE_BLOCK_SIZE < row_left < DEFAULT_MERGE_BLOCK_SIZE * 2,
                // the possibility of skipping a pack in the next block is quite small, less than 1%.
                // And the performance read and then filter is is better than readWithFilter,
                // so only if the number of rows left after filtering out is large enough,
                // we can skip some packs of the next block, call readWithFilter to get the next block.
                rest_column_block = rest_column_stream->readWithFilter(*filter);
                ColumnPtr filter_column;
                filterFilterColumnBlock(header, filter_column_block, filter, passed_count, filter_column_name);
            }
            else if (filter_out_count > 0)
            {
                // if the number of rows left after filtering out is small, we can't skip any packs of the next block
                // so we call read() to get the next block, and then filter it.
                rest_column_block = rest_column_stream->read();
                for (auto & col : rest_column_block)
                {
                    col.column = col.column->filter(*filter, passed_count);
                }
                filterFilterColumnBlock(header, filter_column_block, filter, passed_count, filter_column_name);
            }
            else
            {
                // if all rows are passed, just read the next block of rest_column_stream
                rest_column_block = rest_column_stream->read();
            }

            // make sure the position and size of filter_column_block and rest_column_block are the same
            RUNTIME_CHECK_MSG(
                rest_column_block.startOffset() == filter_column_block.startOffset(),
                "Late materialization meets unexpected block unmatched, filter_column_block: [start_offset={}, "
                "rows={}], rest_column_block: [start_offset={}, rows={}], pass_count={}",
                filter_column_block.startOffset(),
                filter_column_block.rows(),
                rest_column_block.startOffset(),
                rest_column_block.rows(),
                passed_count);
            // join filter_column_block and rest_column_block by columns,
            // the tmp column added by FilterBlockInputStream will be removed.
            return hstackBlocks({std::move(filter_column_block), std::move(rest_column_block)}, header);
        }
    }

    return filter_column_block;
}

} // namespace DB::DM
