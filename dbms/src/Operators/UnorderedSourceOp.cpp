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

#include <Operators/UnorderedSourceOp.h>

namespace DB
{
OperatorStatus UnorderedSourceOp::readImpl(Block & block)
{
    auto await_status = awaitImpl();
    if (await_status == OperatorStatus::HAS_OUTPUT)
    {
        if (t_block.has_value())
        {
            std::swap(block, t_block.value());
            t_block.reset();
            action.transform(block);
        }
    }
    return await_status;
}

OperatorStatus UnorderedSourceOp::awaitImpl()
{
    if (t_block.has_value())
        return OperatorStatus::HAS_OUTPUT;
    while (true)
    {
        Block res;
        if (!task_pool->tryPopBlock(res))
            return OperatorStatus::WAITING;
        if (res)
        {
            if (unlikely(res.rows() == 0))
                continue;
            t_block.emplace(std::move(res));
            return OperatorStatus::HAS_OUTPUT;
        }
        else
            return OperatorStatus::HAS_OUTPUT;
    }
}
} // namespace DB
