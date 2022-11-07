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

#include <DataStreams/LimitTransformAction.h>
#include <common/likely.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

LimitTransformAction::LimitTransformAction(
    const Block & header_,
    size_t limit_,
    size_t offset_)
    : header(header_)
    , limit(limit_)
    , offset(offset_)
{
}

Block LimitTransformAction::getHeader() const
{
    return header;
}

size_t LimitTransformAction::getLimit() const
{
    return limit;
}

bool LimitTransformAction::transform(Block & block)
{
    if (unlikely(!block))
        return true;

    /// pos - how many lines were read, including the last read block
    if (pos >= offset + limit)
    {
        return false;
    }

    auto rows = block.rows();
    pos += rows;
    if (pos >= offset + rows && pos <= offset + limit)
    {
        // give away the whole block
        return true;
    }
    else
    {
        // give away a piece of the block
        size_t start = std::max(
            static_cast<Int64>(0),
            static_cast<Int64>(offset) - static_cast<Int64>(pos) + static_cast<Int64>(rows));

        size_t length = std::min(
            static_cast<Int64>(limit),
            std::min(
                static_cast<Int64>(pos) - static_cast<Int64>(offset),
                static_cast<Int64>(limit) + static_cast<Int64>(offset) - static_cast<Int64>(pos) + static_cast<Int64>(rows)));

        for (size_t i = 0; i < block.columns(); ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->cut(start, length);
        return true;
    }
}

} // namespace DB