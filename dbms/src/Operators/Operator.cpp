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

#include <Common/FailPoint.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Pipeline/Exec/PipelineExec.h>
#include <Operators/Operator.h>
#include <Operators/OperatorHelper.h>

namespace DB
{
namespace FailPoints
{
extern const char random_pipeline_model_operator_run_failpoint[];
extern const char random_pipeline_model_cancel_failpoint[];
} // namespace FailPoints

void Operator::operatePrefix()
{
    profile_info.anchor();
    operatePrefixImpl();
    profile_info.update();
}

void Operator::operateSuffix()
{
    profile_info.anchor();
    operateSuffixImpl();
    profile_info.update();
}

#define CHECK_IS_CANCELLED                                                                \
    fiu_do_on(FailPoints::random_pipeline_model_cancel_failpoint, exec_context.cancel()); \
    if (unlikely(exec_context.isCancelled()))                                             \
        return OperatorStatus::CANCELLED;

OperatorStatus Operator::await()
{
    // `exec_context.is_cancelled` has been checked by `EventTask`.
    // If `exec_context.is_cancelled` is checked here, the overhead of `exec_context.is_cancelled` will be amplified by the high frequency of `await` calls.

    auto op_status = awaitImpl();
#ifndef NDEBUG
    assertOperatorStatus(op_status, {OperatorStatus::FINISHED, OperatorStatus::NEED_INPUT, OperatorStatus::HAS_OUTPUT});
#endif
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_operator_run_failpoint);

    // During the period when op_status changes from non-waiting to waiting, and from waiting to non-waiting, await will be called continuously.
    // Therefore, directly measuring this period is equivalent to measuring the time consumed by await.
    //
    // When op_status changes to waiting, profile_info.update has already been called, meaning that profile_info::StopWatch::last_ns has been updated to that moment in time.
    // When op_status changes to non-waiting, calling profile_info.update record the time of waiting.
    //
    //      profile_info.update()                   profile_info.update()
    //             ┌────────────────waiting time───────────┐
    // [non-waiting, waiting, waiting, waiting, .., waiting, non-waiting]

    if (op_status != OperatorStatus::WAITING)
    {
        exec_context.triggerAutoSpill();
        profile_info.update();
    }
    return op_status;
}

OperatorStatus Operator::executeIO()
{
    CHECK_IS_CANCELLED
    profile_info.anchor();
    auto op_status = executeIOImpl();
#ifndef NDEBUG
    assertOperatorStatus(op_status, {OperatorStatus::FINISHED, OperatorStatus::NEED_INPUT, OperatorStatus::HAS_OUTPUT});
#endif
    exec_context.triggerAutoSpill();
    profile_info.update();
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_operator_run_failpoint);
    return op_status;
}

void Operator::notify()
{
    profile_info.update();
    notifyImpl();
}

OperatorStatus SourceOp::read(Block & block)
{
    CHECK_IS_CANCELLED
    profile_info.anchor();
    assert(!block);
    auto op_status = readImpl(block);
#ifndef NDEBUG
    if (op_status == OperatorStatus::HAS_OUTPUT && block)
    {
        Block header = getHeader();
        assertBlocksHaveEqualStructure(block, header, getName());
    }
    assertOperatorStatus(op_status, {OperatorStatus::HAS_OUTPUT});
#endif
    exec_context.triggerAutoSpill();
    if (op_status == OperatorStatus::HAS_OUTPUT)
        profile_info.update(block);
    else
        profile_info.update();
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_operator_run_failpoint);
    return op_status;
}

OperatorStatus TransformOp::transform(Block & block)
{
    CHECK_IS_CANCELLED
    profile_info.anchor();
    auto op_status = transformImpl(block);
#ifndef NDEBUG
    if (op_status == OperatorStatus::HAS_OUTPUT && block)
    {
        Block header = getHeader();
        assertBlocksHaveEqualStructure(block, header, getName());
    }
    assertOperatorStatus(op_status, {OperatorStatus::NEED_INPUT, OperatorStatus::HAS_OUTPUT});
#endif
    exec_context.triggerAutoSpill();
    if (op_status == OperatorStatus::HAS_OUTPUT)
        profile_info.update(block);
    else
        profile_info.update();
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_operator_run_failpoint);
    return op_status;
}

OperatorStatus TransformOp::tryOutput(Block & block)
{
    CHECK_IS_CANCELLED
    profile_info.anchor();
    assert(!block);
    auto op_status = tryOutputImpl(block);
#ifndef NDEBUG
    if (op_status == OperatorStatus::HAS_OUTPUT && block)
    {
        Block header = getHeader();
        assertBlocksHaveEqualStructure(block, header, getName());
    }
    assertOperatorStatus(op_status, {OperatorStatus::NEED_INPUT, OperatorStatus::HAS_OUTPUT});
#endif
    exec_context.triggerAutoSpill();
    if (op_status == OperatorStatus::HAS_OUTPUT)
        profile_info.update(block);
    else
        profile_info.update();
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_operator_run_failpoint);
    return op_status;
}

OperatorStatus SinkOp::prepare()
{
    CHECK_IS_CANCELLED
    profile_info.anchor();
    auto op_status = prepareImpl();
#ifndef NDEBUG
    assertOperatorStatus(op_status, {OperatorStatus::NEED_INPUT});
#endif
    profile_info.update();
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_operator_run_failpoint);
    return op_status;
}

OperatorStatus SinkOp::write(Block && block)
{
    CHECK_IS_CANCELLED
    profile_info.anchor(block);
#ifndef NDEBUG
    if (block)
    {
        Block header = getHeader();
        assertBlocksHaveEqualStructure(block, header, getName());
    }
#endif
    auto op_status = writeImpl(std::move(block));
#ifndef NDEBUG
    assertOperatorStatus(op_status, {OperatorStatus::FINISHED, OperatorStatus::NEED_INPUT});
#endif
    exec_context.triggerAutoSpill();
    profile_info.update();
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_operator_run_failpoint);
    return op_status;
}

#undef CHECK_IS_CANCELLED

} // namespace DB
