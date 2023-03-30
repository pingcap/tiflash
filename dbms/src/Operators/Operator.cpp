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
#include <Flash/Executor/PipelineExecutorStatus.h>
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

#define CHECK_IS_CANCELLED                                                               \
    fiu_do_on(FailPoints::random_pipeline_model_cancel_failpoint, exec_status.cancel()); \
    if (unlikely(exec_status.isCancelled()))                                             \
        return OperatorStatus::CANCELLED;

OperatorStatus Operator::await()
{
    CHECK_IS_CANCELLED
    // TODO collect operator profile info here.
    auto op_status = awaitImpl();
#ifndef NDEBUG
    assertOperatorStatus(op_status, {OperatorStatus::NEED_INPUT, OperatorStatus::HAS_OUTPUT});
#endif
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_operator_run_failpoint);
    return op_status;
}

OperatorStatus Operator::executeIO()
{
    CHECK_IS_CANCELLED
    // TODO collect operator profile info here.
    auto op_status = executeIOImpl();
#ifndef NDEBUG
    assertOperatorStatus(op_status, {OperatorStatus::NEED_INPUT, OperatorStatus::HAS_OUTPUT});
#endif
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_operator_run_failpoint);
    return op_status;
}

OperatorStatus SourceOp::read(Block & block)
{
    CHECK_IS_CANCELLED
    // TODO collect operator profile info here.
    assert(!block);
    auto op_status = readImpl(block);
#ifndef NDEBUG
    if (block)
    {
        Block header = getHeader();
        assertBlocksHaveEqualStructure(block, header, getName());
    }
    assertOperatorStatus(op_status, {OperatorStatus::HAS_OUTPUT});
#endif
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_operator_run_failpoint);
    return op_status;
}

OperatorStatus TransformOp::transform(Block & block)
{
    CHECK_IS_CANCELLED
    // TODO collect operator profile info here.
    auto op_status = transformImpl(block);
#ifndef NDEBUG
    if (block)
    {
        Block header = getHeader();
        assertBlocksHaveEqualStructure(block, header, getName());
    }
    assertOperatorStatus(op_status, {OperatorStatus::NEED_INPUT, OperatorStatus::HAS_OUTPUT});
#endif
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_operator_run_failpoint);
    return op_status;
}

OperatorStatus TransformOp::tryOutput(Block & block)
{
    CHECK_IS_CANCELLED
    // TODO collect operator profile info here.
    assert(!block);
    auto op_status = tryOutputImpl(block);
#ifndef NDEBUG
    if (block)
    {
        Block header = getHeader();
        assertBlocksHaveEqualStructure(block, header, getName());
    }
    assertOperatorStatus(op_status, {OperatorStatus::NEED_INPUT, OperatorStatus::HAS_OUTPUT});
#endif
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_operator_run_failpoint);
    return op_status;
}

OperatorStatus SinkOp::prepare()
{
    CHECK_IS_CANCELLED
    // TODO collect operator profile info here.
    auto op_status = prepareImpl();
#ifndef NDEBUG
    assertOperatorStatus(op_status, {OperatorStatus::NEED_INPUT});
#endif
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_operator_run_failpoint);
    return op_status;
}

OperatorStatus SinkOp::write(Block && block)
{
    CHECK_IS_CANCELLED
#ifndef NDEBUG
    if (block)
    {
        Block header = getHeader();
        assertBlocksHaveEqualStructure(block, header, getName());
    }
#endif
    // TODO collect operator profile info here.
    auto op_status = writeImpl(std::move(block));
#ifndef NDEBUG
    assertOperatorStatus(op_status, {OperatorStatus::FINISHED, OperatorStatus::NEED_INPUT});
#endif
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_operator_run_failpoint);
    return op_status;
}

#undef CHECK_IS_CANCELLED

} // namespace DB
