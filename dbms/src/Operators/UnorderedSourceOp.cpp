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
    if (has_temp_block)
    {
        std::swap(block, t_block);
        has_temp_block = false;
        if (action.transform(block))
        {
            return OperatorStatus::HAS_OUTPUT;
        }
        else
            return OperatorStatus::FINISHED;
    }
    else
    {
        while (true)
        {
            Block res;
            if (!task_pool->tryPopBlock(res))
            {
                return OperatorStatus::WAITING;
            }

            if (res)
            {
                if (action.transform(res))
                {
                    std::swap(block, res);
                    return OperatorStatus::HAS_OUTPUT;
                }
                else
                {
                    continue;
                }
            }
            else
            {
                return OperatorStatus::FINISHED;
            }
        }
    }
}

OperatorStatus UnorderedSourceOp::awaitImpl()
{
    Block res;
    if (!task_pool->tryPopBlock(res))
        return OperatorStatus::WAITING;
    if (res)
    {
        if (res.rows() == 0)
            return OperatorStatus::WAITING;
        t_block = std::move(res);
        has_temp_block = true;
        return OperatorStatus::HAS_OUTPUT;
    }
    else
        return OperatorStatus::FINISHED;
}
} // namespace DB
