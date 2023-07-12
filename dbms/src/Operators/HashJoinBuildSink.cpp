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

#include <Interpreters/Join.h>
#include <Operators/HashJoinBuildSink.h>

namespace DB
{
HashJoinBuildSink::HashJoinBuildSink(
    PipelineExecutorContext & exec_context_,
    const String & req_id,
    const JoinPtr & join_ptr_,
    size_t concurrency_build_index_)
    : SinkOp(exec_context_, req_id)
    , join_ptr(join_ptr_)
    , concurrency_build_index(concurrency_build_index_)
    , spill_context(*join_ptr_->spill_context)
{
}

OperatorStatus HashJoinBuildSink::writeImpl(Block && block)
{
    if unlikely (!block)
    {
        join_ptr->finishOneBuild(concurrency_build_index);
        is_finish_status = true;
        return spill_context.isBuildSideSpilling(concurrency_build_index)
            ? OperatorStatus::WAITING
            : OperatorStatus::FINISHED;
    }
    join_ptr->insertFromBlock(block, concurrency_build_index);
    block.clear();
    return spill_context.isBuildSideSpilling(concurrency_build_index)
        ? OperatorStatus::WAITING
        : OperatorStatus::NEED_INPUT;
}

OperatorStatus HashJoinBuildSink::awaitImpl()
{
    if (is_finish_status)
    {
        return spill_context.isBuildSideSpilling(concurrency_build_index)
            ? OperatorStatus::WAITING
            : OperatorStatus::FINISHED;
    }
    else
    {
        return spill_context.isBuildSideSpilling(concurrency_build_index)
            ? OperatorStatus::WAITING
            : OperatorStatus::NEED_INPUT;
    }
}
} // namespace DB
