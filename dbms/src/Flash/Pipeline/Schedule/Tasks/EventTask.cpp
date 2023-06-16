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
#include <Flash/Pipeline/Schedule/Tasks/EventTask.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>

namespace DB
{
namespace FailPoints
{
extern const char random_pipeline_model_task_run_failpoint[];
extern const char random_pipeline_model_cancel_failpoint[];
extern const char exception_during_query_run[];
} // namespace FailPoints

#define EXECUTE(function)                                                                   \
    fiu_do_on(FailPoints::random_pipeline_model_cancel_failpoint, exec_status.cancel());    \
    if unlikely (exec_status.isCancelled())                                                 \
        return ExecTaskStatus::CANCELLED;                                                   \
    try                                                                                     \
    {                                                                                       \
        auto status = (function());                                                         \
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_task_run_failpoint); \
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_query_run);               \
        return status;                                                                      \
    }                                                                                       \
    catch (...)                                                                             \
    {                                                                                       \
        LOG_WARNING(log, "error occurred and cancel the query");                            \
        exec_status.onErrorOccurred(std::current_exception());                              \
        return ExecTaskStatus::ERROR;                                                       \
    }

EventTask::EventTask(
    PipelineExecutorStatus & exec_status_,
    const EventPtr & event_)
    : exec_status(exec_status_)
    , event(event_)
{
    RUNTIME_CHECK(event);
}

EventTask::EventTask(
    MemoryTrackerPtr mem_tracker_,
    const String & req_id,
    PipelineExecutorStatus & exec_status_,
    const EventPtr & event_)
    : Task(std::move(mem_tracker_), req_id)
    , exec_status(exec_status_)
    , event(event_)
{
    RUNTIME_CHECK(event);
}

void EventTask::finalizeImpl()
{
    try
    {
        doFinalizeImpl();
    }
    catch (...)
    {
        exec_status.onErrorOccurred(std::current_exception());
    }
    event->onTaskFinish(profile_info);
    event.reset();
}

ExecTaskStatus EventTask::executeImpl()
{
    EXECUTE(doExecuteImpl);
}

ExecTaskStatus EventTask::executeIOImpl()
{
    EXECUTE(doExecuteIOImpl);
}

ExecTaskStatus EventTask::awaitImpl()
{
    EXECUTE(doAwaitImpl);
}

UInt64 EventTask::getScheduleDuration() const
{
    return event->getScheduleDuration();
}

#undef EXECUTE

} // namespace DB
