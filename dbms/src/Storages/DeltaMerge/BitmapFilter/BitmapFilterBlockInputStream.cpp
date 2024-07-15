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
#include <Storages/DeltaMerge/BitmapFilter/BitmapFilter.h>
#include <Storages/DeltaMerge/BitmapFilter/BitmapFilterBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/ReadUtil.h>

namespace DB::DM
{
BitmapFilterBlockInputStream::BitmapFilterBlockInputStream(
    const ColumnDefines & columns_to_read,
    SkippableBlockInputStreamPtr stable_,
    SkippableBlockInputStreamPtr delta_,
    size_t stable_rows_,
    const BitmapFilterPtr & bitmap_filter_,
    const String & req_id_)
    : header(toEmptyBlock(columns_to_read))
    , stable(stable_)
    , delta(delta_)
    , stable_rows(stable_rows_)
    , bitmap_filter(bitmap_filter_)
    , log(Logger::get(NAME, req_id_))
{}

Block BitmapFilterBlockInputStream::read(FilterPtr & res_filter, bool return_filter)
{
    FilterPtr block_filter = nullptr;
    auto [block, from_delta] = readBlock(stable, delta, block_filter, /*return_filter*/ true);
    if (block)
    {
        if (from_delta)
        {
            block.setStartOffset(block.startOffset() + stable_rows);
        }

        bool all_match = false;
        if (block_filter)
        {
            bitmap_filter->rangeAnd(*block_filter, block.startOffset(), block.rows());
        }
        else
        {
            filter.resize(block.rows());
            block_filter = &filter;
            all_match = bitmap_filter->get(*block_filter, block.startOffset(), block.rows());
        }
        if (!all_match)
        {
            if (return_filter)
            {
                res_filter = block_filter;
            }
            else
            {
                size_t passed_count = countBytesInFilter(*block_filter);
                for (auto & col : block)
                {
                    col.column = col.column->filter(*block_filter, passed_count);
                }
            }
        }
        else
        {
            res_filter = nullptr;
        }
    }
    return block;
}

} // namespace DB::DM
