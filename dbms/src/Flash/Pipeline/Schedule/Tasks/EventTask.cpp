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

#include <Flash/Pipeline/Schedule/Tasks/EventTask.h>

namespace DB
{
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
    event->onTaskFinish();
    event.reset();
}

ExecTaskStatus EventTask::executeImpl()
{
    return doTaskAction([&] { return doExecuteImpl(); });
}

ExecTaskStatus EventTask::executeIOImpl()
{
    return doTaskAction([&] { return doExecuteIOImpl(); });
}

ExecTaskStatus EventTask::awaitImpl()
{
    return doTaskAction([&] { return doAwaitImpl(); });
}
} // namespace DB
