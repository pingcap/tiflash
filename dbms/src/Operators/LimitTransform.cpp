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

#include <Operators/LimitTransform.h>

namespace DB
{
void GlobalLimitTransformAction::transform(Block & block)
{
    if (unlikely(!block))
        return;

    /// pos - how many lines were read, including the last read block
    if (pos >= limit)
    {
        block = {};
    }

    auto rows = block.rows();
    size_t pre_pos = pos.fetch_add(rows);
    // The limit has been reached.
    if (pre_pos >= limit)
    {
        block = {};
        return;
    }
    size_t cur_pos = pre_pos + rows;
    if (cur_pos <= limit)
    {
        // give away the whole block
        return;
    }
    else
    {
        // cur_pos > limit
        // give away a piece of the block
        assert(rows + limit > cur_pos);
        size_t length = rows + limit - cur_pos;
        for (size_t i = 0; i < block.columns(); ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->cut(0, length);
        return;
    }
}

OperatorStatus LimitTransform::transform(Block & block)
{
    if (likely(block))
        action->transform(block);
    return OperatorStatus::PASS;
}

void LimitTransform::transformHeader(Block & header)
{
    header = action->getHeader();
}
} // namespace DB
