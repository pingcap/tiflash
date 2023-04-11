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

#include <Common/Exception.h>
#include <Flash/Pipeline/Schedule/Tasks/PipelineTask.h>

#include <magic_enum.hpp>

namespace DB
{
PipelineTask::PipelineTask(
    MemoryTrackerPtr mem_tracker_,
    const String & req_id,
    PipelineExecutorStatus & exec_status_,
    const EventPtr & event_,
    PipelineExecPtr && pipeline_exec_)
    : EventTask(std::move(mem_tracker_), req_id, exec_status_, event_)
    , pipeline_exec(std::move(pipeline_exec_))
{
    assert(pipeline_exec);
    pipeline_exec->executePrefix();
}

void PipelineTask::finalizeImpl()
{
    assert(pipeline_exec);
    pipeline_exec->executeSuffix();
    pipeline_exec.reset();
}

#define HANDLE_NOT_RUNNING_STATUS         \
    case OperatorStatus::FINISHED:        \
    {                                     \
        return ExecTaskStatus::FINISHED;  \
    }                                     \
    case OperatorStatus::CANCELLED:       \
    {                                     \
        return ExecTaskStatus::CANCELLED; \
    }                                     \
    case OperatorStatus::IO:              \
    {                                     \
        return ExecTaskStatus::IO;        \
    }                                     \
    case OperatorStatus::WAITING:         \
    {                                     \
        return ExecTaskStatus::WAITING;   \
    }

#define UNEXPECTED_OP_STATUS(op_status, function_name) \
    throw Exception(fmt::format("Unexpected op state {} at {}", magic_enum::enum_name(op_status), (function_name)));

ExecTaskStatus PipelineTask::doExecuteImpl()
{
    assert(pipeline_exec);
    auto op_status = pipeline_exec->execute();
    switch (op_status)
    {
        HANDLE_NOT_RUNNING_STATUS
    // After `pipeline_exec->execute`, `NEED_INPUT` means that pipeline_exec need data to do the calculations and expect the next call to `execute`
    // And other states are unexpected.
    case OperatorStatus::NEED_INPUT:
        return ExecTaskStatus::RUNNING;
    default:
        UNEXPECTED_OP_STATUS(op_status, "PipelineTask::execute");
    }
}

ExecTaskStatus PipelineTask::doExecuteIOImpl()
{
    assert(pipeline_exec);
    auto op_status = pipeline_exec->executeIO();
    switch (op_status)
    {
        HANDLE_NOT_RUNNING_STATUS
    // After `pipeline_exec->executeIO`,
    // - `NEED_INPUT` means that pipeline_exec need data to do the calculations and expect the next call to `execute`
    // - `HAS_OUTPUT` means that pipeline_exec has data to do the calculations and expect the next call to `execute`
    // And other states are unexpected.
    case OperatorStatus::NEED_INPUT:
    case OperatorStatus::HAS_OUTPUT:
        return ExecTaskStatus::RUNNING;
    default:
        UNEXPECTED_OP_STATUS(op_status, "PipelineTask::execute");
    }
}

ExecTaskStatus PipelineTask::doAwaitImpl()
{
    assert(pipeline_exec);
    auto op_status = pipeline_exec->await();
    switch (op_status)
    {
        HANDLE_NOT_RUNNING_STATUS
    // After `pipeline_exec->await`, `HAS_OUTPUT` means that pipeline_exec has data to do the calculations and expect the next call to `execute`
    // And other states are unexpected.
    case OperatorStatus::HAS_OUTPUT:
        return ExecTaskStatus::RUNNING;
    default:
        UNEXPECTED_OP_STATUS(op_status, "PipelineTask::await");
    }
}

#undef HANDLE_NOT_RUNNING_STATUS
#undef UNEXPECTED_OP_STATUS

} // namespace DB
