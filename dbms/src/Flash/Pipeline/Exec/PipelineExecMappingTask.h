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

#pragma once

#include <Flash/Pipeline/Exec/PipelineExec.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>

#include <magic_enum.hpp>

namespace DB
{
/// Map the execution result of PipelineExec to Task
/// As follows
/// - OperatorStatus::FINISHED/CANCELLED       ==>     ExecTaskStatus::FINISHED/CANCELLED
/// - OperatorStatus::IO_IN/IO_OUT             ==>     ExecTaskStatus::IO_IN/IO_OUT
/// - OperatorStatus::WAITING                  ==>     ExecTaskStatus::WAITING
/// - OperatorStatus::NEED_INPUT/HAS_OUTPUT    ==>     ExecTaskStatus::RUNNING

#define MAP_NOT_RUNNING_TASK_STATUS       \
    case OperatorStatus::FINISHED:        \
    {                                     \
        return ExecTaskStatus::FINISHED;  \
    }                                     \
    case OperatorStatus::CANCELLED:       \
    {                                     \
        return ExecTaskStatus::CANCELLED; \
    }                                     \
    case OperatorStatus::IO_IN:           \
    {                                     \
        return ExecTaskStatus::IO_IN;     \
    }                                     \
    case OperatorStatus::IO_OUT:          \
    {                                     \
        return ExecTaskStatus::IO_OUT;    \
    }                                     \
    case OperatorStatus::WAITING:         \
    {                                     \
        return ExecTaskStatus::WAITING;   \
    }

#define UNEXPECTED_OP_STATUS(op_status, function_name) \
    throw Exception(fmt::format("Unexpected op state {} at {}", magic_enum::enum_name(op_status), (function_name)));

#define MAPPING_TASK_EXECUTE(pipeline_exec)                                                                                                            \
    assert(pipeline_exec);                                                                                                                             \
    auto op_status = (pipeline_exec)->execute();                                                                                                       \
    switch (op_status)                                                                                                                                 \
    {                                                                                                                                                  \
        MAP_NOT_RUNNING_TASK_STATUS                                                                                                                    \
    /* After `pipeline_exec->execute`, `NEED_INPUT` means that pipeline_exec need data to do the calculations and expect the next call to `execute` */ \
    /* And other states are unexpected. */                                                                                                             \
    case OperatorStatus::NEED_INPUT:                                                                                                                   \
        return ExecTaskStatus::RUNNING;                                                                                                                \
    default:                                                                                                                                           \
        UNEXPECTED_OP_STATUS(op_status, "PipelineExec::execute");                                                                                      \
    }

#define MAPPING_TASK_EXECUTE_IO(pipeline_exec)                                                                           \
    assert(pipeline_exec);                                                                                               \
    auto op_status = (pipeline_exec)->executeIO();                                                                       \
    switch (op_status)                                                                                                   \
    {                                                                                                                    \
        MAP_NOT_RUNNING_TASK_STATUS                                                                                      \
    /* After `pipeline_exec->executeIO`, */                                                                              \
    /* - `NEED_INPUT` means that pipeline_exec need data to do the calculations and expect the next call to `execute` */ \
    /* - `HAS_OUTPUT` means that pipeline_exec has data to do the calculations and expect the next call to `execute` */  \
    /* And other states are unexpected. */                                                                               \
    case OperatorStatus::NEED_INPUT:                                                                                     \
    case OperatorStatus::HAS_OUTPUT:                                                                                     \
        return ExecTaskStatus::RUNNING;                                                                                  \
    default:                                                                                                             \
        UNEXPECTED_OP_STATUS(op_status, "PipelineExec::execute");                                                        \
    }

#define MAPPING_TASK_AWAIT(pipeline_exec)                                                                                \
    assert(pipeline_exec);                                                                                               \
    auto op_status = (pipeline_exec)->await();                                                                           \
    switch (op_status)                                                                                                   \
    {                                                                                                                    \
        MAP_NOT_RUNNING_TASK_STATUS                                                                                      \
    /* After `pipeline_exec->await`, */                                                                                  \
    /* - `NEED_INPUT` means that pipeline_exec need data to do the calculations and expect the next call to `execute` */ \
    /* - `HAS_OUTPUT` means that pipeline_exec has data to do the calculations and expect the next call to `execute` */  \
    /* And other states are unexpected. */                                                                               \
    case OperatorStatus::NEED_INPUT:                                                                                     \
    case OperatorStatus::HAS_OUTPUT:                                                                                     \
        return ExecTaskStatus::RUNNING;                                                                                  \
    default:                                                                                                             \
        UNEXPECTED_OP_STATUS(op_status, "PipelineExec::await");                                                          \
    }

} // namespace DB
