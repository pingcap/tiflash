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
 *     sink     transform     ...     transform    source
 *
 *  prepare───►fetchBlock───► ... ───►fetchBlock───►read────┐
 *                                                          │ block
 *    write◄────transform◄─── ... ◄───transform◄────────────┘
 */
OperatorStatus PipelineExec::execute(PipelineExecutorStatus & exec_status)
{
    Block block;
    size_t transform_index = 0;
    auto op_status = fetchBlock(block, transform_index, exec_status);
    if (op_status != OperatorStatus::PASS)
        return op_status;

    // start from the next transform after fetch block transform.
    for (; transform_index < transforms.size(); ++transform_index)
    {
        CHECK_IS_CANCELLED(exec_status);
        auto op_status = transforms[transform_index]->transform(block);
        if (op_status != OperatorStatus::PASS)
            return prepareSpillOp(op_status, transforms[transform_index]);
    }
    CHECK_IS_CANCELLED(exec_status);
    return prepareSpillOp(sink->write(std::move(block)), sink);
}

// try fetch block from transforms and source.
OperatorStatus PipelineExec::fetchBlock(Block & block, size_t & transform_index, PipelineExecutorStatus & exec_status)
{
    CHECK_IS_CANCELLED(exec_status);
    auto op_status = sink->prepare();
    if (op_status != OperatorStatus::PASS)
        return prepareSpillOp(op_status, sink);
    for (int64_t index = transforms.size() - 1; index >= 0; --index)
    {
        CHECK_IS_CANCELLED(exec_status);
        auto op_status = transforms[index]->fetchBlock(block);
        if (op_status != OperatorStatus::NO_OUTPUT)
        {
            // Once the transform fetch block has succeeded, execution will begin with the next transform.
            transform_index = index + 1;
            return prepareSpillOp(op_status, transforms[index]);
        }
    }
    CHECK_IS_CANCELLED(exec_status);
    transform_index = 0;
    return prepareSpillOp(source->read(block), source);
}

OperatorStatus PipelineExec::await(PipelineExecutorStatus & exec_status)
{
    CHECK_IS_CANCELLED(exec_status);

    auto op_status = sink->await();
    if (op_status != OperatorStatus::PASS)
        return op_status;
    for (auto it = transforms.rbegin(); it != transforms.rend(); ++it)
    {
        // if one of the transform return ready status, we don't need to check the upstream operator.
        auto op_status = (*it)->await();
        if (op_status != OperatorStatus::SKIP)
            return op_status;
    }
    return source->await();
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
