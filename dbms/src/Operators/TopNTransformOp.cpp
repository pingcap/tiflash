// Copyright 2023 PingCAP, Ltd.
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

#include <DataStreams/MergeSortingBlocksBlockInputStream.h>
#include <Interpreters/sortBlock.h>
#include <Operators/TopNTransformOp.h>

#include <memory>

namespace DB
{
OperatorStatus TopNTransformOp::transformImpl(Block & block)
{
    if (unlikely(!block))
    {
        if (!blocks.empty())
        {
            impl = std::make_unique<MergeSortingBlocksBlockInputStream>(
                blocks,
                order_desc,
                req_id,
                max_block_size,
                limit);
            block = impl->read();
        }
        return OperatorStatus::HAS_OUTPUT;
    }
    if (impl)
        throw Exception("Can not reach here.");

    sortBlock(block, order_desc, limit);
    blocks.emplace_back(std::move(block));
    return OperatorStatus::NEED_INPUT;
}

OperatorStatus TopNTransformOp::tryOutputImpl(Block & block)
{
    if (impl)
    {
        block = impl->read();
    }
    return OperatorStatus::HAS_OUTPUT;
}

void TopNTransformOp::transformHeaderImpl(Block &)
{
}

} // namespace DB
