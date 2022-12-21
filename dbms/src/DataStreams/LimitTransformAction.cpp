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
namespace
{
void cut(Block & block, size_t rows, size_t limit, size_t pos)
{
    // give away a piece of the block
    assert(rows + limit > pos);
    size_t length = rows + limit - pos;
    for (size_t i = 0; i < block.columns(); ++i)
        block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->cut(0, length);
}
} // namespace

bool LocalLimitTransformAction::transform(Block & block)
{
    if (unlikely(!block))
        return true;

    /// pos - how many lines were read, including the last read block
    if (pos >= limit)
        return false;

    auto rows = block.rows();
    pos += rows;
    if (pos > limit)
        cut(block, rows, limit, pos);
    // for pos <= limit, give away the whole block
    return true;
}

bool GlobalLimitTransformAction::transform(Block & block)
{
    if (unlikely(!block))
        return true;

    /// pos - how many lines were read, including the last read block
    if (pos >= limit)
        return false;

    auto rows = block.rows();
    size_t prev_pos = pos.fetch_add(rows);
    if (prev_pos >= limit)
        return false;

    size_t cur_pos = prev_pos + rows;
    if (cur_pos > limit)
        cut(block, rows, limit, cur_pos);
    // for pos <= limit, give away the whole block
    return true;
}
} // namespace DB
