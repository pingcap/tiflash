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

#include <Operators/GetResultSinkOp.h>

namespace DB
{
OperatorStatus GetResultSinkOp::writeImpl(Block && block)
{
    if (!block)
        return OperatorStatus::FINISHED;

    assert(!t_block);
    auto ret = result_queue->tryPush(std::move(block));
    switch (ret)
    {
    case MPMCQueueResult::OK:
        return OperatorStatus::NEED_INPUT;
    case MPMCQueueResult::FULL:
        // If returning Full, the block was not actually moved.
        assert(block); // NOLINT(bugprone-use-after-move)
        t_block.emplace(std::move(block)); // NOLINT(bugprone-use-after-move)
        return OperatorStatus::WAITING;
    default:
        return OperatorStatus::FINISHED;
    }
}

OperatorStatus GetResultSinkOp::prepareImpl()
{
    return awaitImpl();
}

OperatorStatus GetResultSinkOp::awaitImpl()
{
    if (!t_block)
        return OperatorStatus::NEED_INPUT;

    auto ret = result_queue->tryPush(std::move(*t_block));
    switch (ret)
    {
    case MPMCQueueResult::OK:
        t_block.reset();
        return OperatorStatus::NEED_INPUT;
    case MPMCQueueResult::FULL:
        return OperatorStatus::WAITING;
    default:
        return OperatorStatus::FINISHED;
    }
}
} // namespace DB
