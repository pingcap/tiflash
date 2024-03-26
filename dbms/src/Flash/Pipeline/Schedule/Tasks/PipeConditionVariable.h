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

#include <deque>

namespace DB
{
// Must have lock to use this class
class PipeConditionVariable
{
public:
    inline void registerTask(TaskPtr && task)
    {
        assert(task);
        assert(task->getStatus() == ExecTaskStatus::WAIT_FOR_NOTIFY);
        tasks.push_back(std::move(task));

#ifdef __APPLE__
        auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_wait_for_notify_tasks_count);
#else
        thread_local auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_wait_for_notify_tasks_count);
#endif
        metrics.Increment();
    }

    inline void notifyOne()
    {
        if (!tasks.empty())
        {
            auto task = std::move(tasks.front());
            tasks.pop_front();
            notifyTaskDirectly(std::move(task));

#ifdef __APPLE__
            auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_wait_for_notify_tasks_count);
#else
            thread_local auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_wait_for_notify_tasks_count);
#endif
            metrics.Decrement();
        }
    }

    inline void notifyAll()
    {
        size_t tasks_cnt = tasks.size();
        while (!tasks.empty())
        {
            auto task = std::move(tasks.front());
            tasks.pop_front();
            notifyTaskDirectly(std::move(task));
        }

#ifdef __APPLE__
        auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_wait_for_notify_tasks_count);
#else
        thread_local auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_wait_for_notify_tasks_count);
#endif
        metrics.Decrement(tasks_cnt);
    }

    static inline void notifyTaskDirectly(TaskPtr && task)
    {
        assert(task);
        task->notify();
        task->profile_info.elapsedWaitForNotifyTime();
        assert(TaskScheduler::instance);
        TaskScheduler::instance->submitToCPUTaskThreadPool(std::move(task));
    }

private:
    std::deque<TaskPtr> tasks;
};
} // namespace DB
