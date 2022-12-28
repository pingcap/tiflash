// Copyright 2022 PingCAP, Ltd.
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
#include <Common/MemoryTrackerSetter.h>
#include <Flash/Pipeline/Event.h>
#include <Flash/Pipeline/PipelineTask.h>

namespace DB
{
#define HANDLE_CANCELLED                  \
    if (unlikely(event->isCancelled()))   \
    {                                     \
        op_executor.reset();              \
        event->finishTask();              \
        event.reset();                    \
        return ExecTaskStatus::CANCELLED; \
    }

#define HANDLE_ERROR                                            \
    catch (...)                                                 \
    {                                                           \
        op_executor.reset();                                    \
        event->toError(getCurrentExceptionMessage(true, true)); \
        event->finishTask();                                    \
        event.reset();                                          \
        return ExecTaskStatus::ERROR;                           \
    }

#define HANDLE_FINISHED                  \
    case OperatorStatus::FINISHED:       \
    {                                    \
        op_executor.reset();             \
        event->finishTask();             \
        event.reset();                   \
        return ExecTaskStatus::FINISHED; \
    }

ExecTaskStatus PipelineTask::executeImpl()
{
    HANDLE_CANCELLED
    try
    {
        auto op_status = op_executor->execute();
        switch (op_status)
        {
            HANDLE_FINISHED
        case OperatorStatus::WAITING:
            return ExecTaskStatus::WAITING;
        case OperatorStatus::SPILLING:
            return ExecTaskStatus::SPILLING;
        case OperatorStatus::PASS:
        case OperatorStatus::MORE_INPUT:
            return ExecTaskStatus::RUNNING;
        default:
            __builtin_unreachable();
        }
    }
    HANDLE_ERROR
}

ExecTaskStatus PipelineTask::awaitImpl()
{
    HANDLE_CANCELLED
    try
    {
        auto op_status = op_executor->await();
        switch (op_status)
        {
            HANDLE_FINISHED
        case OperatorStatus::WAITING:
            return ExecTaskStatus::WAITING;
        case OperatorStatus::PASS:
            return ExecTaskStatus::RUNNING;
        default:
            __builtin_unreachable();
        }
    }
    HANDLE_ERROR
}

ExecTaskStatus PipelineTask::spillImpl()
{
    HANDLE_CANCELLED
    try
    {
        auto op_status = op_executor->spill();
        switch (op_status)
        {
            HANDLE_FINISHED
        case OperatorStatus::SPILLING:
            return ExecTaskStatus::SPILLING;
        case OperatorStatus::PASS:
            return ExecTaskStatus::RUNNING;
        default:
            __builtin_unreachable();
        }
    }
    HANDLE_ERROR
}

#undef HANDLE_CANCELLED
#undef HANDLE_ERROR
#undef HANDLE_FINISHED

} // namespace DB
