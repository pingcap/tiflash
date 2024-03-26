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

#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>

namespace DB
{
// Must have lock to use this class
class PipeConditionVariable
{
public:
    void registerTask(TaskPtr && task)
    {
        GET_METRIC(tiflash_pipeline_scheduler, type_wait_for_notify_tasks_count).Increment();
        assert(task->getStatus() == ExecTaskStatus::WAIT_FOR_NOTIFY);
        tasks.push_back(std::move(task));
    }

    bool notifyOne()
    {
        if (!tasks.empty())
        {
            auto task = std::move(tasks.back());
            tasks.pop_back();
            task->notify();
            task->profile_info.elapsedWaitForNotifyTime();
            GET_METRIC(tiflash_pipeline_scheduler, type_wait_for_notify_tasks_count).Decrement();
            TaskScheduler::instance->submitToCPUTaskThreadPool(std::move(task));
            return true;
        }
        return false;
    }

    void notifyAll()
    {
        while (notifyOne()) {}
    }

private:
    std::vector<TaskPtr> tasks;
};
} // namespace DB
