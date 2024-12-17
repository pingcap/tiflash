// Copyright 2024 PingCAP, Inc.
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

#include <Flash/Executor/PipelineExecutorContext.h>
#include <Operators/HashJoinV2ProbeTransformOp.h>
#include <Operators/Operator.h>

namespace DB
{

HashJoinV2ProbeTransformOp::HashJoinV2ProbeTransformOp(
    PipelineExecutorContext & exec_context_,
    const String & req_id,
    const HashJoinPtr & join_,
    size_t op_index_)
    : TransformOp(exec_context_, req_id)
    , join_ptr(join_)
    , op_index(op_index_)
{
    RUNTIME_CHECK_MSG(join_ptr != nullptr, "join ptr should not be null.");
    RUNTIME_CHECK_MSG(join_ptr->getProbeConcurrency() > 0, "Join probe concurrency must be greater than 0");
    RUNTIME_CHECK_MSG(join_ptr->isFinalize(), "join should be finalized first.");
}

void HashJoinV2ProbeTransformOp::transformHeaderImpl(Block & header_)
{
    header_ = join_ptr->getOutputBlock();
}

void HashJoinV2ProbeTransformOp::operateSuffixImpl()
{
    LOG_DEBUG(
        log,
        "Finish joinV2 probe, total output rows {}, joined rows {}, scan hash map rows {}",
        joined_rows + scan_hash_map_rows,
        joined_rows,
        scan_hash_map_rows);
}

OperatorStatus HashJoinV2ProbeTransformOp::onOutput(Block & block)
{
    assert(!probe_context.isCurrentProbeFinished());
    block = join_ptr->joinBlock(probe_context, op_index);
    size_t rows = block.rows();
    joined_rows += rows;
    return OperatorStatus::HAS_OUTPUT;
}

OperatorStatus HashJoinV2ProbeTransformOp::transformImpl(Block & block)
{
    assert(probe_context.isCurrentProbeFinished());
    if unlikely (!block)
    {
        join_ptr->finishOneProbe(op_index);
        probe_context.input_is_finished = true;
        block = join_ptr->getProbeBufferedResultBlock(op_index);
        return OperatorStatus::HAS_OUTPUT;
    }
    if (block.rows() == 0)
        return OperatorStatus::NEED_INPUT;
    probe_context.resetBlock(block);
    return onOutput(block);
}

OperatorStatus HashJoinV2ProbeTransformOp::tryOutputImpl(Block & block)
{
    if unlikely (probe_context.input_is_finished)
    {
        block = {};
        return OperatorStatus::HAS_OUTPUT;
    }
    if (probe_context.isCurrentProbeFinished())
        return OperatorStatus::NEED_INPUT;
    return onOutput(block);
}


} // namespace DB
