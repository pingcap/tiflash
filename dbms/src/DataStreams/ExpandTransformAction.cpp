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
#include <DataStreams/ExpandTransformAction.h>

#include <algorithm>

namespace DB
{
ExpandTransformAction::ExpandTransformAction(const Block & header_, const Expand2Ptr & expand_)
    : header(header_)
    , expand(expand_)
    , i_th_project(0)
{}

Block ExpandTransformAction::getHeader() const
{
    return header;
}

bool ExpandTransformAction::tryOutput(Block & block)
{
    if (!block_cache || i_th_project >= expand->getLevelProjectionNum())
        return false;
    auto res_block = expand->next(block_cache, i_th_project++);
    block.swap(res_block);
    return true;
}

void ExpandTransformAction::transform(Block & block)
{
    // empty block should be output too.
    if (unlikely(!block))
        return;

    // when calling this function, that means we read a new block from source OP, and pull it up through lower to upper OP incrementally.
    // so just caching this new block here and handle expansion, notice: Expand never accumulate blocks.
    expand->getBeforeExpandActions()->execute(block);
    block_cache = block;
    i_th_project = 0;
    auto res_block = expand->next(block_cache, i_th_project++);
    block.swap(res_block);
}
} // namespace DB
