// Copyright 2022 PingCAP, Ltd.
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

#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/LateMaterializationBlockInputStream.h>
#include <Storages/DeltaMerge/ReadUtil.h>

#include <algorithm>

namespace DB::DM
{

LateMaterializationBlockInputStream::LateMaterializationBlockInputStream(
    const ColumnDefines & columns_to_read,
    BlockInputStreamPtr filter_column_stream_,
    SkippableBlockInputStreamPtr rest_column_stream_,
    const BitmapFilterPtr & bitmap_filter_,
    const String & req_id_)
    : header(toEmptyBlock(columns_to_read))
    , filter_column_stream(filter_column_stream_)
    , rest_column_stream(rest_column_stream_)
    , bitmap_filter(bitmap_filter_)
    , log(Logger::get(NAME, req_id_))
{}

Block LateMaterializationBlockInputStream::readImpl()
{
    Block filter_column_block;
    FilterPtr filter = nullptr;

    // Until non-empty block after filtering or end of stream.
    while (true)
    {
        filter_column_block = filter_column_stream->read(filter, true);

        if (!filter_column_block)
            return filter_column_block;

        RUNTIME_CHECK_MSG(filter, "Late materialization meets unexpected null filter");

        // Get mvcc-filter
        size_t rows = filter_column_block.rows();
        mvcc_filter.resize(rows);
        bool all_match = bitmap_filter->get(mvcc_filter, filter_column_block.startOffset(), rows);
        if (!all_match)
        {
            // if mvcc-filter is all match, use filter directly
            // else use `mvcc-filter & filter` to get the final filter
            std::transform(mvcc_filter.cbegin(), mvcc_filter.cend(), filter->cbegin(), filter->begin(), [](const UInt8 a, const UInt8 b) { return a != 0 && b != 0; });
        }

        if (size_t passed_count = std::count(filter->cbegin(), filter->cend(), 1); passed_count == 0)
        {
            if (!rest_column_stream->skipNextBlock())
            {
                // if we fail to skip, we need to read the rest of the block and ignore it
                rest_column_stream->read();
            }
            else
            {
                LOG_DEBUG(log, "Late materialization skip read block at start_offset: {}, rows: {}", filter_column_block.startOffset(), filter_column_block.rows());
            }
        }
        else
        {
            Block rest_column_block;
            if (passed_count != rows)
            {
                for (auto & col : filter_column_block)
                {
                    col.column = col.column->filter(*filter, passed_count);
                }
                rest_column_block = rest_column_stream->readWithFilter(*filter);
            }
            else
            {
                rest_column_block = rest_column_stream->read();
            }

            RUNTIME_CHECK_MSG(rest_column_block.rows() == filter_column_block.rows() && rest_column_block.startOffset() == filter_column_block.startOffset(),
                              "Late materialization meets unexpected size of block unmatched, filter_column_block: [start_offset={}, rows={}], rest_column_block: [start_offset={}, rows={}], pass_count: {}",
                              filter_column_block.startOffset(),
                              filter_column_block.rows(),
                              rest_column_block.startOffset(),
                              rest_column_block.rows(),
                              passed_count);

            return hstackBlocks({std::move(filter_column_block), std::move(rest_column_block)}, header);
        }
    }

    return filter_column_block;
}

} // namespace DB::DM
