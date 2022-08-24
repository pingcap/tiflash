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

namespace DB::DM
{
BitmapFilterBlockInputStream::BitmapFilterBlockInputStream(BlockInputStreamPtr stable_, BlockInputStreamPtr delta_, size_t stable_rows_, const ArrayBitmapFilterPtr & bitmap_filter_, const String & req_id_)
    : stable(stable_)
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

        static const UInt8 zero = 0;
        filter.assign(block.rows(), zero);
        bitmap_filter->get(filter, block.startOffset(), block.rows());

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
