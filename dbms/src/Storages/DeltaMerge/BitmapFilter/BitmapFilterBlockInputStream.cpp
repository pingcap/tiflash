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

#include <Storages/DeltaMerge/BitmapFilter/BitmapFilter.h>
#include <Storages/DeltaMerge/BitmapFilter/BitmapFilterBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>

namespace DB::DM
{
BitmapFilterBlockInputStream::BitmapFilterBlockInputStream(
    const ColumnDefines & columns_to_read,
    BlockInputStreamPtr stable_,
    BlockInputStreamPtr delta_,
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

Block BitmapFilterBlockInputStream::readImpl(FilterPtr & res_filter, bool return_filter)
{
    auto [block, from_delta] = readBlock();
    if (block)
    {
        if (from_delta)
        {
            block.setStartOffset(block.startOffset() + stable_rows);
        }

        filter.resize(block.rows());
        bool all_match = bitmap_filter->get(filter, block.startOffset(), block.rows());
        if (!all_match)
        {
            if (return_filter)
            {
                res_filter = &filter;
            }
            else
            {
                for (auto & col : block)
                {
                    col.column = col.column->filter(filter, block.rows());
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

// <Block, from_delta>
std::pair<Block, bool> BitmapFilterBlockInputStream::readBlock()
{
    if (stable == nullptr && delta == nullptr)
    {
        return {{}, false};
    }

    if (stable == nullptr)
    {
        return {delta->read(), true};
    }

    auto block = stable->read();
    if (block)
    {
        return {block, false};
    }
    else
    {
        stable = nullptr;
        if (delta != nullptr)
        {
            block = delta->read();
        }
        return {block, true};
    }
}

} // namespace DB::DM
