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

#include <DataStreams/SquashingHashJoinBlockTransform.h>

namespace DB
{

SquashingHashJoinBlockTransform::SquashingHashJoinBlockTransform(UInt64 max_block_size_)
    : output_rows(0)
    , max_block_size(max_block_size_)
    , join_finished(false)
{}

void SquashingHashJoinBlockTransform::handleOverLimitBlock()
{
    // if over_limit_block is not null, we need to push it into blocks.
    if (over_limit_block)
    {
        assert(!(output_rows && blocks.empty()));
        output_rows += over_limit_block->rows();
        blocks.push_back(std::move(over_limit_block.value()));
        over_limit_block.reset();
    }
}

void SquashingHashJoinBlockTransform::appendBlock(Block & block)
{
    if (!block)
    {
        // if append block is {}, mark join finished.
        join_finished = true;
        return;
    }
    size_t current_rows = block.rows();

    if (!output_rows || output_rows + current_rows <= max_block_size)
    {
        blocks.push_back(std::move(block));
        output_rows += current_rows;
    }
    else
    {
        // if output_rows + current_rows > max block size, put the current result block into over_limit_block and handle it in next read.
        assert(!over_limit_block);
        over_limit_block.emplace(std::move(block));
    }
}

Block SquashingHashJoinBlockTransform::getFinalOutputBlock()
{
    Block final_block = mergeBlocks(std::move(blocks));
    reset();
    handleOverLimitBlock();
    return final_block;
}

void SquashingHashJoinBlockTransform::reset()
{
    blocks.clear();
    output_rows = 0;
}

bool SquashingHashJoinBlockTransform::isJoinFinished() const
{
    return join_finished;
}

bool SquashingHashJoinBlockTransform::needAppendBlock() const
{
    return !over_limit_block && !join_finished;
}

} // namespace DB
