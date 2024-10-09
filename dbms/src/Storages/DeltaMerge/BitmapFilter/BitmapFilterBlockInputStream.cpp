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
    if (return_filter)
        return readImpl(res_filter);

    // The caller want a filtered resut, so let's filter by ourselves.

    FilterPtr block_filter;
    auto block = readImpl(block_filter);
    if (!block)
        return {};

    // all rows in block are not filtered out, simply do nothing.
    if (!block_filter) // NOLINT
        return block;

    // some rows should be filtered according to `block_filter`:
    size_t passed_count = countBytesInFilter(*block_filter);
    for (auto & col : block)
    {
        col.column = col.column->filter(*block_filter, passed_count);
    }
    return block;
}

Block BitmapFilterBlockInputStream::readImpl(FilterPtr & res_filter)
{
    FilterPtr block_filter = nullptr;
    auto [block, from_delta] = readBlockWithReturnFilter(stable, delta, block_filter);

    if (block)
    {
        if (from_delta)
        {
            block.setStartOffset(block.startOffset() + stable_rows);
        }

        filter.resize(block.rows());
        bool all_match = bitmap_filter->get(filter, block.startOffset(), block.rows());

        if (!block_filter)
        {
            if (all_match)
                res_filter = nullptr;
            else
                res_filter = &filter;
        }
        else
        {
            RUNTIME_CHECK(filter.size() == block_filter->size(), filter.size(), block_filter->size());
            if (!all_match)
            {
                // We have a `block_filter`, and have a bitmap filter in `filter`.
                // filter ← filter & block_filter.
                std::transform( //
                    filter.begin(),
                    filter.end(),
                    block_filter->begin(),
                    filter.begin(),
                    [](UInt8 a, UInt8 b) { return a && b; });
                res_filter = &filter;
            }
            else
            {
                // We only have a `block_filter`.
                // res_filter ← block_filter.
                res_filter = block_filter;
            }
        }
    }
    return block;
}

} // namespace DB::DM
