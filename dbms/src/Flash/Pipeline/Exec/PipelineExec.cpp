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

#include <Flash/Executor/PipelineExecutorStatus.h>
#include <Flash/Pipeline/Exec/PipelineExec.h>

namespace DB
{
#define CHECK_IS_CANCELLED(exec_status)        \
    if (unlikely((exec_status).isCancelled())) \
        return OperatorStatus::CANCELLED;

/**
 *  sink_op   transform_op    ...   transform_op   source_op
 *
 *  prepare────►tryOutput───► ... ───►tryOutput────►read────┐
 *                                                          │ block
 *    write◄────transform◄─── ... ◄───transform◄────────────┘
 */
OperatorStatus PipelineExec::execute(PipelineExecutorStatus & exec_status)
{
    Block block;
    size_t start_transform_op_index = 0;
    auto op_status = fetchBlock(block, start_transform_op_index, exec_status);
    // If the state `fetchBlock` returns isn't `HAS_OUTPUT`, it means that `fetchBlock` did not return a block and is an exception state.
    if (op_status != OperatorStatus::HAS_OUTPUT)
        return op_status;

    // start from the next transform after fetch block transform.
    for (size_t transform_op_index = start_transform_op_index; transform_op_index < transform_ops.size(); ++transform_op_index)
    {
        CHECK_IS_CANCELLED(exec_status);
        auto op_status = transform_ops[transform_op_index]->transform(block);
        if (op_status != OperatorStatus::PASS_THROUGH)
            return prepareSpillOpIfNeed(op_status, transform_ops[transform_op_index]);
    }
    CHECK_IS_CANCELLED(exec_status);
    return prepareSpillOpIfNeed(sink_op->write(std::move(block)), sink_op);
}

// try fetch block from transform_ops and source_op.
OperatorStatus PipelineExec::fetchBlock(
    Block & block,
    size_t & start_transform_op_index,
    PipelineExecutorStatus & exec_status)
{
    CHECK_IS_CANCELLED(exec_status);
    auto op_status = sink_op->prepare();
    if (op_status != OperatorStatus::NEED_INPUT)
        return prepareSpillOpIfNeed(op_status, sink_op);
    for (int64_t index = transform_ops.size() - 1; index >= 0; --index)
    {
        CHECK_IS_CANCELLED(exec_status);
        auto op_status = transform_ops[index]->tryOutput(block);
        if (op_status != OperatorStatus::NEED_INPUT)
        {
            // Once the transform op tryOutput has succeeded, execution will begin with the next transform op.
            start_transform_op_index = index + 1;
            return prepareSpillOpIfNeed(op_status, transform_ops[index]);
        }
    }
    CHECK_IS_CANCELLED(exec_status);
    start_transform_op_index = 0;
    return prepareSpillOpIfNeed(source_op->read(block), source_op);
}

OperatorStatus PipelineExec::await(PipelineExecutorStatus & exec_status)
{
    CHECK_IS_CANCELLED(exec_status);

    auto op_status = sink_op->await();
    if (op_status != OperatorStatus::NEED_INPUT)
        return op_status;
    for (auto it = transform_ops.rbegin(); it != transform_ops.rend(); ++it)
    {
        // If the transform_op returns `NEED_INPUT`,
        // we need to call the upstream transform_op until a transform_op returns something other than `NEED_INPUT`.
        auto op_status = (*it)->await();
        if (op_status != OperatorStatus::NEED_INPUT)
            return op_status;
    }
    return source_op->await();
}

OperatorStatus PipelineExec::spill(PipelineExecutorStatus & exec_status)
{
    CHECK_IS_CANCELLED(exec_status);

    assert(ready_spill_op);
    assert(*ready_spill_op);
    auto op_status = (*ready_spill_op)->spill();
    if (op_status != OperatorStatus::SPILLING)
        ready_spill_op.reset();
    return op_status;
}

#undef CHECK_IS_CANCELLED

} // namespace DB
