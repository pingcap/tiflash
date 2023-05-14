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

#include <Common/FailPoint.h>
#include <Flash/Executor/PipelineExecutorStatus.h>
#include <Flash/Pipeline/Schedule/Events/Event.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>

namespace DB
{
namespace FailPoints
{
extern const char random_pipeline_model_task_run_failpoint[];
} // namespace FailPoints

// The base class of event related task.
class EventTask : public Task
{
public:
    EventTask(
        PipelineExecutorStatus & exec_status_,
        const EventPtr & event_);
    EventTask(
        MemoryTrackerPtr mem_tracker_,
        const String & req_id,
        PipelineExecutorStatus & exec_status_,
        const EventPtr & event_);

protected:
    ExecTaskStatus executeImpl() override;
    virtual ExecTaskStatus doExecuteImpl() = 0;

    ExecTaskStatus executeIOImpl() override;
    virtual ExecTaskStatus doExecuteIOImpl() { return ExecTaskStatus::RUNNING; };

    ExecTaskStatus awaitImpl() override;
    virtual ExecTaskStatus doAwaitImpl() { return ExecTaskStatus::RUNNING; };

    void finalizeImpl() override;
    virtual void doFinalizeImpl(){};

private:
    template <typename Action>
    ALWAYS_INLINE ExecTaskStatus doTaskAction(Action && action)
    {
        if (unlikely(exec_status.isCancelled()))
            return ExecTaskStatus::CANCELLED;

        try
        {
            auto status = action();
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_task_run_failpoint);
            return status;
        }
        catch (...)
        {
            LOG_WARNING(log, "error occurred and cancel the query");
            exec_status.onErrorOccurred(std::current_exception());
            return ExecTaskStatus::ERROR;
        }
    }

private:
    PipelineExecutorStatus & exec_status;
    EventPtr event;
};

} // namespace DB
