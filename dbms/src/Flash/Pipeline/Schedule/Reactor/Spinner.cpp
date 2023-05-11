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

#include <Flash/Pipeline/Schedule/Reactor/Spinner.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>

namespace DB
{
bool Spinner::awaitAndPushReadyTask(TaskPtr && task)
{
    assert(task);
    TRACE_MEMORY(task);
    auto status = task->await();
    switch (status)
    {
    case ExecTaskStatus::WAITING:
        return false;
    case ExecTaskStatus::RUNNING:
        task->profile_info.elapsedAwaitTime();
        cpu_tasks.push_back(std::move(task));
        return true;
    case ExecTaskStatus::IO:
        task->profile_info.elapsedAwaitTime();
        io_tasks.push_back(std::move(task));
        return true;
    case FINISH_STATUS:
        task->profile_info.elapsedAwaitTime();
        FINALIZE_TASK(task);
        return true;
    default:
        UNEXPECTED_STATUS(logger, status);
    }
}

void Spinner::submitReadyTasks()
{
    if (cpu_tasks.empty() && io_tasks.empty())
    {
        tryYield();
        return;
    }

    task_scheduler.submitToCPUTaskThreadPool(cpu_tasks);
    cpu_tasks.clear();

    task_scheduler.submitToIOTaskThreadPool(io_tasks);
    io_tasks.clear();

    spin_count = 0;
}

void Spinner::tryYield()
{
    ++spin_count;

    if (spin_count != 0 && spin_count % 64 == 0)
    {
        sched_yield();
        if (spin_count == 640)
        {
            spin_count = 0;
            sched_yield();
        }
    }
}
} // namespace DB
