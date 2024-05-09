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
class PipeConditionVariable
{
public:
    inline void registerTask(TaskPtr && task)
    {
        assert(task);
        assert(task->getStatus() == ExecTaskStatus::WAIT_FOR_NOTIFY);
        {
            std::lock_guard lock(mu);
            tasks.push_back(std::move(task));
        }

        thread_local auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_wait_for_notify_tasks_count);
        metrics.Increment();
    }

    inline void notifyOne()
    {
        TaskPtr task;
        {
            std::lock_guard lock(mu);
            if (tasks.empty())
                return;
            task = std::move(tasks.front());
            tasks.pop_front();
        }
        assert(task);
        notifyTaskDirectly(std::move(task));

        thread_local auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_wait_for_notify_tasks_count);
        metrics.Decrement();
    }

    inline void notifyAll()
    {
        std::deque<TaskPtr> cur_tasks;
        {
            std::lock_guard lock(mu);
            std::swap(cur_tasks, tasks);
        }
        size_t tasks_cnt = cur_tasks.size();
        while (!cur_tasks.empty())
        {
            notifyTaskDirectly(std::move(cur_tasks.front()));
            cur_tasks.pop_front();
        }

        thread_local auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_wait_for_notify_tasks_count);
        metrics.Decrement(tasks_cnt);
    }

    static inline void notifyTaskDirectly(TaskPtr && task)
    {
        assert(task);
        task->notify();
        assert(TaskScheduler::instance);
        if (unlikely(task->getStatus() == ExecTaskStatus::WAITING))
        {
            TaskScheduler::instance->submitToWaitReactor(std::move(task));
        }
        else
        {
            assert(task->getStatus() == ExecTaskStatus::RUNNING);
            TaskScheduler::instance->submitToCPUTaskThreadPool(std::move(task));
        }
    }

private:
    std::mutex mu;
    std::deque<TaskPtr> tasks;
};
} // namespace DB
