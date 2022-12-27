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

#pragma once

#include <Core/Block.h>

namespace DB
{

class SquashingHashJoinBlockTransform
{
public:
    SquashingHashJoinBlockTransform(UInt64 max_block_size_);

    void appendBlock(Block & block);
    Block getFinalOutputBlock();
    bool isJoinFinished() const;
    bool needAppendBlock() const;


private:
    void handleOverLimitBlock();
    void reset();

    Blocks blocks;
    std::optional<Block> over_limit_block;
    size_t output_rows;
    UInt64 max_block_size;
    bool join_finished;
    bool over_limit;
};

} // namespace DB