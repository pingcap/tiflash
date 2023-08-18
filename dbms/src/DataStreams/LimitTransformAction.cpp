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

#include <DataStreams/LimitTransformAction.h>
#include <common/likely.h>

namespace DB
{
namespace
{
// Removes all rows outside specified range of Block.
void cut(Block & block, size_t rows [[maybe_unused]], size_t limit, size_t pos)
{
    assert(rows + limit > pos);
    size_t pop_back_cnt = pos - limit;
    for (auto & col : block)
    {
        auto mutate_col = (*std::move(col.column)).mutate();
        mutate_col->popBack(pop_back_cnt);
        col.column = std::move(mutate_col);
    }
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
