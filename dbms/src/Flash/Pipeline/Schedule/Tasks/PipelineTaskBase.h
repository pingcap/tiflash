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

#pragma once

#include <Flash/Pipeline/Exec/PipelineExec.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>

#include <magic_enum.hpp>
#include <utility>

namespace DB
{
/// Map the execution result of PipelineExec to Task
/// As follows
/// - OperatorStatus::FINISHED/CANCELLED       ==>     ExecTaskStatus::FINISHED/CANCELLED
/// - OperatorStatus::IO_IN/IO_OUT             ==>     ExecTaskStatus::IO_IN/IO_OUT
/// - OperatorStatus::WAITING                  ==>     ExecTaskStatus::WAITING
/// - OperatorStatus::NEED_INPUT/HAS_OUTPUT    ==>     ExecTaskStatus::RUNNING

#define MAP_NOT_RUNNING_TASK_STATUS             \
    case OperatorStatus::FINISHED:              \
    {                                           \
        return ExecTaskStatus::FINISHED;        \
    }                                           \
    case OperatorStatus::CANCELLED:             \
    {                                           \
        return ExecTaskStatus::CANCELLED;       \
    }                                           \
    case OperatorStatus::IO_IN:                 \
    {                                           \
        return ExecTaskStatus::IO_IN;           \
    }                                           \
    case OperatorStatus::IO_OUT:                \
    {                                           \
        return ExecTaskStatus::IO_OUT;          \
    }                                           \
    case OperatorStatus::WAITING:               \
    {                                           \
        return ExecTaskStatus::WAITING;         \
    }                                           \
    case OperatorStatus::WAIT_FOR_NOTIFY:       \
    {                                           \
        return ExecTaskStatus::WAIT_FOR_NOTIFY; \
    }

#define UNEXPECTED_OP_STATUS(op_status, function_name) \
    throw Exception(fmt::format("Unexpected op state {} at {}", magic_enum::enum_name(op_status), (function_name)));

class PipelineTaskBase
{
public:
    explicit PipelineTaskBase(PipelineExecPtr && pipeline_exec_)
        : pipeline_exec_holder(std::move(pipeline_exec_))
        , pipeline_exec(pipeline_exec_holder.get())
    {
        RUNTIME_CHECK(pipeline_exec);
        pipeline_exec->executePrefix();
    }

protected:
    ExecTaskStatus runExecute()
    {
        assert(pipeline_exec);
        auto op_status = (pipeline_exec)->execute();
        switch (op_status)
        {
            MAP_NOT_RUNNING_TASK_STATUS
        /* After `pipeline_exec->execute`, `NEED_INPUT` means that pipeline_exec need data to do the calculations and expect the next call to `execute` */
        /* And other states are unexpected. */
        case OperatorStatus::NEED_INPUT:
            return ExecTaskStatus::RUNNING;
        default:
            UNEXPECTED_OP_STATUS(op_status, "PipelineExec::execute");
        }
    }

    ExecTaskStatus runExecuteIO()
    {
        assert(pipeline_exec);
        auto op_status = (pipeline_exec)->executeIO();
        switch (op_status)
        {
            MAP_NOT_RUNNING_TASK_STATUS
        /* After `pipeline_exec->executeIO`, */
        /* - `NEED_INPUT` means that pipeline_exec need data to do the calculations and expect the next call to `execute` */
        /* - `HAS_OUTPUT` means that pipeline_exec has data to do the calculations and expect the next call to `execute` */
        /* And other states are unexpected. */
        case OperatorStatus::NEED_INPUT:
        case OperatorStatus::HAS_OUTPUT:
            return ExecTaskStatus::RUNNING;
        default:
            UNEXPECTED_OP_STATUS(op_status, "PipelineExec::execute");
        }
    }

    ExecTaskStatus runAwait()
    {
        assert(pipeline_exec);
        auto op_status = (pipeline_exec)->await();
        switch (op_status)
        {
            MAP_NOT_RUNNING_TASK_STATUS
        /* After `pipeline_exec->await`, */
        /* - `NEED_INPUT` means that pipeline_exec need data to do the calculations and expect the next call to `execute` */
        /* - `HAS_OUTPUT` means that pipeline_exec has data to do the calculations and expect the next call to `execute` */
        /* And other states are unexpected. */
        case OperatorStatus::NEED_INPUT:
        case OperatorStatus::HAS_OUTPUT:
            return ExecTaskStatus::RUNNING;
        default:
            UNEXPECTED_OP_STATUS(op_status, "PipelineExec::await");
        }
    }

    void runFinalize(UInt64 extra_time)
    {
        assert(pipeline_exec);
        pipeline_exec->executeSuffix();
        pipeline_exec->finalizeProfileInfo(extra_time);
        pipeline_exec = nullptr;
        pipeline_exec_holder.reset();
    }

    void runNotify()
    {
        assert(pipeline_exec);
        pipeline_exec->notify();
    }

private:
    PipelineExecPtr pipeline_exec_holder;
    // To reduce the overheads of `pipeline_exec_holder.get()`
    PipelineExec * pipeline_exec;
};

#undef MAP_NOT_RUNNING_TASK_STATUS
#undef UNEXPECTED_OP_STATUS

} // namespace DB
