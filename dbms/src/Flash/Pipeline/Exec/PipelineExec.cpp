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

#include <Common/FailPoint.h>
#include <Flash/Pipeline/Exec/PipelineExec.h>
#include <Operators/OperatorHelper.h>

namespace DB
{
namespace FailPoints
{
extern const char random_pipeline_model_execute_prefix_failpoint[];
extern const char random_pipeline_model_execute_suffix_failpoint[];
} // namespace FailPoints

#define HANDLE_OP_STATUS(op, op_status, expect_status)                                                 \
    switch (op_status)                                                                                 \
    {                                                                                                  \
    /* For the expected status, it will not return here, */                                            \
    /* but instead return control to the macro caller, */                                              \
    /* who will continue to call the next operator. */                                                 \
    case (expect_status):                                                                              \
        break;                                                                                         \
    /* For the io status, the operator needs to be filled in io_op for later use in executeIO. */      \
    case OperatorStatus::IO_IN:                                                                        \
    case OperatorStatus::IO_OUT:                                                                       \
        fillIOOp((op).get());                                                                          \
        return (op_status);                                                                            \
    /* For the waiting status, the operator needs to be filled in awaitable for later use in await. */ \
    case OperatorStatus::WAITING:                                                                      \
        fillAwaitable((op).get());                                                                     \
        return (op_status);                                                                            \
    /* For unexpected status, an immediate return is required. */                                      \
    default:                                                                                           \
        return (op_status);                                                                            \
    }

#define HANDLE_LAST_OP_STATUS(op, op_status)                                                           \
    assert(op);                                                                                        \
    switch (op_status)                                                                                 \
    {                                                                                                  \
    /* For the io status, the operator needs to be filled in io_op for later use in executeIO. */      \
    case OperatorStatus::IO_IN:                                                                        \
    case OperatorStatus::IO_OUT:                                                                       \
        fillIOOp((op).get());                                                                          \
        return (op_status);                                                                            \
    /* For the waiting status, the operator needs to be filled in awaitable for later use in await. */ \
    case OperatorStatus::WAITING:                                                                      \
        fillAwaitable((op).get());                                                                     \
        return (op_status);                                                                            \
    /* For the last operator, the status will always be returned. */                                   \
    default:                                                                                           \
        return (op_status);                                                                            \
    }

PipelineExec::PipelineExec(
    SourceOpPtr && source_op_,
    TransformOps && transform_ops_,
    SinkOpPtr && sink_op_)
    : source_op(std::move(source_op_))
    , transform_ops(std::move(transform_ops_))
    , sink_op(std::move(sink_op_))
{
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_execute_prefix_failpoint);
}

void PipelineExec::executePrefix()
{
    sink_op->operatePrefix();
    for (auto it = transform_ops.rbegin(); it != transform_ops.rend(); ++it) // NOLINT(modernize-loop-convert)
        (*it)->operatePrefix();
    source_op->operatePrefix();
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_execute_suffix_failpoint);
}

void PipelineExec::executeSuffix()
{
    sink_op->operateSuffix();
    for (auto it = transform_ops.rbegin(); it != transform_ops.rend(); ++it) // NOLINT(modernize-loop-convert)
        (*it)->operateSuffix();
    source_op->operateSuffix();
}

OperatorStatus PipelineExec::execute()
{
    auto op_status = executeImpl();
#ifndef NDEBUG
    // `NEED_INPUT` means that pipeline_exec need data to do the calculations and expect the next call to `execute`.
    assertOperatorStatus(op_status, {OperatorStatus::FINISHED, OperatorStatus::NEED_INPUT});
#endif
    return op_status;
}
/**
 *  sink_op   transform_op    ...   transform_op   source_op
 *
 *  prepare────►tryOutput───► ... ───►tryOutput────►read────┐
 *                                                          │ block
 *    write◄────transform◄─── ... ◄───transform◄────────────┘
 */
OperatorStatus PipelineExec::executeImpl()
{
    Block block;
    size_t start_transform_op_index = 0;
    auto op_status = fetchBlock(block, start_transform_op_index);
    // If the status `fetchBlock` returns isn't `HAS_OUTPUT`, it means that `fetchBlock` did not return a block.
    if (op_status != OperatorStatus::HAS_OUTPUT)
        return op_status;

    // start from the next transform op after fetched block transform op.
    for (size_t transform_op_index = start_transform_op_index; transform_op_index < transform_ops.size(); ++transform_op_index)
    {
        const auto & transform_op = transform_ops[transform_op_index];
        op_status = transform_op->transform(block);
        HANDLE_OP_STATUS(transform_op, op_status, OperatorStatus::HAS_OUTPUT);
    }
    op_status = sink_op->write(std::move(block));
    HANDLE_LAST_OP_STATUS(sink_op, op_status);
}

// try fetch block from transform_ops and source_op.
OperatorStatus PipelineExec::fetchBlock(Block & block, size_t & start_transform_op_index)
{
    auto op_status = sink_op->prepare();
    HANDLE_OP_STATUS(sink_op, op_status, OperatorStatus::NEED_INPUT);
    for (int64_t index = transform_ops.size() - 1; index >= 0; --index)
    {
        const auto & transform_op = transform_ops[index];
        op_status = transform_op->tryOutput(block);
        // Once the transform op tryOutput has succeeded, execution will begin with the next transform op.
        start_transform_op_index = index + 1;
        HANDLE_OP_STATUS(transform_op, op_status, OperatorStatus::NEED_INPUT);
    }
    start_transform_op_index = 0;
    op_status = source_op->read(block);
    HANDLE_LAST_OP_STATUS(source_op, op_status);
}

OperatorStatus PipelineExec::executeIO()
{
    auto op_status = executeIOImpl();
#ifndef NDEBUG
    // `NEED_INPUT` means that pipeline_exec need data to do the calculations and expect the next call to `execute`.
    // `HAS_OUTPUT` means that pipeline_exec has data to do the calculations and expect the next call to `execute`.
    assertOperatorStatus(op_status, {OperatorStatus::FINISHED, OperatorStatus::HAS_OUTPUT, OperatorStatus::NEED_INPUT});
#endif
    return op_status;
}
OperatorStatus PipelineExec::executeIOImpl()
{
    assert(io_op);
    auto op_status = io_op->executeIO();
    if (op_status == OperatorStatus::WAITING)
        fillAwaitable(io_op);
    if (op_status != OperatorStatus::IO_IN && op_status != OperatorStatus::IO_OUT)
        io_op = nullptr;
    return op_status;
}

OperatorStatus PipelineExec::await()
{
    auto op_status = awaitImpl();
#ifndef NDEBUG
    // `HAS_OUTPUT` means that pipeline_exec has data to do the calculations and expect the next call to `execute`.
    // `NEED_INPUT` means that pipeline_exec need data to do the calculations and expect the next call to `execute`.
    assertOperatorStatus(op_status, {OperatorStatus::FINISHED, OperatorStatus::HAS_OUTPUT, OperatorStatus::NEED_INPUT});
#endif
    return op_status;
}
OperatorStatus PipelineExec::awaitImpl()
{
    assert(awaitable);
    auto op_status = awaitable->await();
    if (op_status == OperatorStatus::IO_IN || op_status == OperatorStatus::IO_OUT)
        fillIOOp(awaitable);
    if (op_status != OperatorStatus::WAITING)
        awaitable = nullptr;
    return op_status;
}

#undef HANDLE_OP_STATUS
#undef HANDLE_LAST_OP_STATUS

void PipelineExec::finalizeProfileInfo(UInt64 extra_time)
{
    // `extra_time` usually includes pipeline schedule duration and task queuing time.
    //
    // The pipeline schedule duration should be added to the pipeline breaker operator(AggConvergent and JoinProbe),
    // However, if there are multiple pipeline breaker operators within a single pipeline, it can become very complex.
    // Therefore, to simplify matters, we will include the pipeline schedule duration in the execution time of the source operator.
    //
    // ditto for task queuing time.
    //
    // TODO Refining execution summary, excluding extra time from execution time.
    // For example: [total_time:6s, execution_time:1s, pending_time:2s, pipeline_waiting_time:3s]

    // The execution time of operator[i] = self_time_from_profile_info + sum(self_time_from_profile_info[i-1, .., 0]) + extra_time.
    source_op->getProfileInfo()->execution_time += extra_time;
    extra_time = source_op->getProfileInfo()->execution_time;
    for (const auto & transform_op : transform_ops)
    {
        transform_op->getProfileInfo()->execution_time += extra_time;
        extra_time = transform_op->getProfileInfo()->execution_time;
    }
    sink_op->getProfileInfo()->execution_time += extra_time;
}

} // namespace DB
