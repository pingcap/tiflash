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
#include <Storages/DeltaMerge/BitmapFilter/BitmapFilterBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>


namespace DB::DM
{

BitmapFilterBlockInputStream::BitmapFilterBlockInputStream(
    const ColumnDefines & columns_to_read,
    BlockInputStreamPtr stream_,
    const BitmapFilterPtr & bitmap_filter_,
    const String & req_id_)
    : header(toEmptyBlock(columns_to_read))
    , stream(stream_)
    , bitmap_filter(bitmap_filter_)
    , log(Logger::get(NAME, req_id_))
{}

Block BitmapFilterBlockInputStream::read()
{
    FilterPtr block_filter = nullptr;
    auto block = stream->read(block_filter, true);
    if (!block)
        return block;

    filter.resize(block.rows());
    bool all_match = bitmap_filter->get(filter, block.startOffset(), block.rows());
    if (!block_filter)
    {
        if (all_match)
            return block;
        size_t passed_count = countBytesInFilter(filter);
        for (auto & col : block)
        {
            col.column = col.column->filter(filter, passed_count);
        }
    }
    else
    {
        RUNTIME_CHECK(filter.size() == block_filter->size(), filter.size(), block_filter->size());
        if (!all_match)
        {
            std::transform(
                filter.begin(),
                filter.end(),
                block_filter->begin(),
                block_filter->begin(),
                [](UInt8 a, UInt8 b) { return a && b; });
        }
        size_t passed_count = countBytesInFilter(*block_filter);
        for (auto & col : block)
        {
            col.column = col.column->filter(*block_filter, passed_count);
        }
    }
    return block;
}

} // namespace DB::DM
