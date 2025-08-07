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
    while (true)
    {
        switch (status)
        {
        case ProbeStatus::PROBE:
            if unlikely (probe_context.isAllFinished())
            {
                join_ptr->finishOneProbe(op_index);
                status = join_ptr->needProbeScanBuildSide() ? ProbeStatus::WAIT_PROBE_FINISH : ProbeStatus::FINISHED;
                block = join_ptr->probeLastResultBlock(op_index);
                if (block)
                    return OperatorStatus::HAS_OUTPUT;
                break;
            }
            block = join_ptr->probeBlock(probe_context, op_index);
            joined_rows += block.rows();
            return OperatorStatus::HAS_OUTPUT;
        case ProbeStatus::WAIT_PROBE_FINISH:
            if (join_ptr->isAllProbeFinished())
            {
                status = ProbeStatus::SCAN_BUILD_SIDE;
                break;
            }
            return OperatorStatus::WAIT_FOR_NOTIFY;
        case ProbeStatus::SCAN_BUILD_SIDE:
            block = join_ptr->scanBuildSideAfterProbe(op_index);
            scan_hash_map_rows += block.rows();
            if unlikely (!block)
                status = ProbeStatus::FINISHED;
            return OperatorStatus::HAS_OUTPUT;
        case ProbeStatus::FINISHED:
            block = {};
            return OperatorStatus::HAS_OUTPUT;
        }
    }
}

OperatorStatus HashJoinV2ProbeTransformOp::transformImpl(Block & block)
{
    assert(status == ProbeStatus::PROBE);
    assert(probe_context.isAllFinished());
    if likely (block)
    {
        if (block.rows() == 0)
            return OperatorStatus::NEED_INPUT;
        probe_context.resetBlock(block);
    }
    return onOutput(block);
}

OperatorStatus HashJoinV2ProbeTransformOp::tryOutputImpl(Block & block)
{
    if (status == ProbeStatus::PROBE && probe_context.isAllFinished())
        return OperatorStatus::NEED_INPUT;
    return onOutput(block);
}


} // namespace DB
