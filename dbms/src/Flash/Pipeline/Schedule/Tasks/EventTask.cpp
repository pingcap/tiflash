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
#include <Flash/Pipeline/Schedule/Tasks/EventTask.h>

namespace DB
{
namespace FailPoints
{
extern const char random_pipeline_model_task_run_failpoint[];
} // namespace FailPoints

EventTask::EventTask(
    PipelineExecutorStatus & exec_status_,
    const EventPtr & event_)
    : exec_status(exec_status_)
    , event(event_)
{
    assert(event);
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
    assert(event);
}

EventTask::~EventTask()
{
    assert(event);
    event->onTaskFinish();
    event.reset();
}

void EventTask::finalize() noexcept
{
    try
    {
        RUNTIME_CHECK(!finalized);
        finalized = true;
        finalizeImpl();
    }
    catch (...)
    {
        exec_status.onErrorOccurred(std::current_exception());
    }
}

ExecTaskStatus EventTask::executeImpl() noexcept
{
    return doTaskAction([&] { return doExecuteImpl(); });
}

ExecTaskStatus EventTask::executeIOImpl() noexcept
{
    return doTaskAction([&] { return doExecuteIOImpl(); });
}

ExecTaskStatus EventTask::awaitImpl() noexcept
{
    return doTaskAction([&] { return doAwaitImpl(); });
}

ExecTaskStatus EventTask::doTaskAction(std::function<ExecTaskStatus()> && action)
{
    if (unlikely(exec_status.isCancelled()))
    {
        finalize();
        return ExecTaskStatus::CANCELLED;
    }
    try
    {
        auto status = action();
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_task_run_failpoint);
        switch (status)
        {
        case FINISH_STATUS:
            finalize();
        default:
            return status;
        }
    }
    catch (...)
    {
        finalize();
        assert(event);
        LOG_WARNING(log, "error occurred and cancel the query");
        exec_status.onErrorOccurred(std::current_exception());
        return ExecTaskStatus::ERROR;
    }
}

} // namespace DB
