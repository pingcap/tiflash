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

#include <Flash/Executor/ResultQueue.h>
#include <Operators/GetResultSinkOp.h>

namespace DB
{
OperatorStatus GetResultSinkOp::writeImpl(Block && block)
{
    if unlikely (!block)
        return OperatorStatus::FINISHED;

    assert(!t_block);
    t_block.emplace(std::move(block));
    return tryFlush();
}

OperatorStatus GetResultSinkOp::prepareImpl()
{
    return t_block ? tryFlush() : OperatorStatus::NEED_INPUT;
}

OperatorStatus GetResultSinkOp::tryFlush()
{
    auto queue_result = result_queue->tryPush(std::move(*t_block));
    switch (queue_result)
    {
    case MPMCQueueResult::FULL:
        setNotifyFuture(result_queue.get());
        return OperatorStatus::WAIT_FOR_NOTIFY;
    case MPMCQueueResult::OK:
        t_block.reset();
        return OperatorStatus::NEED_INPUT;
    case MPMCQueueResult::CANCELLED:
        return OperatorStatus::CANCELLED;
    default:
        // queue result can not be finished/empty here.
        RUNTIME_CHECK_MSG(
            false,
            "Unexpected queue result for GetResultSinkOp: {}",
            magic_enum::enum_name(queue_result));
    }
}
} // namespace DB
