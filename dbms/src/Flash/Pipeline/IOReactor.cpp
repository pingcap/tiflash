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

#include <Common/MemoryTrackerSetter.h>
#include <Common/setThreadName.h>
#include <Flash/Pipeline/IOReactor.h>
#include <Flash/Pipeline/TaskScheduler.h>
#include <assert.h>
#include <common/logger_useful.h>
#include <errno.h>
#include <immintrin.h>

namespace DB
{
namespace
{
bool handle(std::vector<TaskPtr> & ready_tasks, TaskPtr && task)
{
    assert(task);

    assert(nullptr == current_memory_tracker);
    // Hold the shared_ptr of memory tracker.
    // To avoid the current_memory_tracker being an illegal pointer.
    auto memory_tracker = task->getMemTracker();
    MemoryTrackerSetter memory_tracker_setter{true, memory_tracker.get()};

    auto status = task->await();
    switch (status)
    {
    case ExecTaskStatus::WAITING:
        return false;
    case ExecTaskStatus::RUNNING:
        ready_tasks.push_back(std::move(task));
        return true;
    case ExecTaskStatus::FINISHED:
    case ExecTaskStatus::ERROR:
    case ExecTaskStatus::CANCELLED:
        task.reset();
        return true;
    default:
        __builtin_unreachable();
    }
}
} // namespace

IOReactor::IOReactor(TaskScheduler & scheduler_)
    : scheduler(scheduler_)
{
    thread = std::thread(&IOReactor::loop, this);
}

void IOReactor::close()
{
    is_shutdown = true;
    cond.notify_one();
}

void IOReactor::submit(TaskPtr && task)
{
    assert(task);
    {
        std::lock_guard lock(mutex);
        waiting_tasks.emplace_back(std::move(task));
    }
    cond.notify_one();
}

void IOReactor::submit(std::list<TaskPtr> & tasks)
{
    if (tasks.empty())
        return;
    {
        std::lock_guard lock(mutex);
        waiting_tasks.splice(waiting_tasks.end(), tasks);
    }
    cond.notify_one();
}

IOReactor::~IOReactor()
{
    thread.join();
    LOG_INFO(logger, "stop io reactor loop");
}

void IOReactor::loop()
{
    setThreadName("IOReactor");
    LOG_INFO(logger, "start io reactor loop");
    std::list<TaskPtr> local_waiting_tasks;
    int spin_count = 0;
    std::vector<TaskPtr> ready_tasks;
    while (!is_shutdown)
    {
        assert(ready_tasks.empty());
        if (local_waiting_tasks.empty())
        {
            std::unique_lock lock(mutex);
            while (!is_shutdown && waiting_tasks.empty())
                cond.wait(lock);
            if (is_shutdown)
                break;
            assert(!waiting_tasks.empty());
            local_waiting_tasks.splice(local_waiting_tasks.end(), waiting_tasks);
        }
        else
        {
            std::lock_guard lock(mutex);
            if (!waiting_tasks.empty())
                local_waiting_tasks.splice(local_waiting_tasks.end(), waiting_tasks);
        }

        auto task_it = local_waiting_tasks.begin();
        while (task_it != local_waiting_tasks.end())
        {
            if (handle(ready_tasks, std::move(*task_it)))
                task_it = local_waiting_tasks.erase(task_it);
            else
                ++task_it;
        }

        if (ready_tasks.empty())
        {
            spin_count += 1;
        }
        else
        {
            spin_count = 0;
            scheduler.task_executor.submit(ready_tasks);
            ready_tasks.clear();
        }

        if (spin_count != 0 && spin_count % 64 == 0)
        {
#ifdef __x86_64__
            _mm_pause();
#else
            // TODO: Maybe there's a better intrinsic like _mm_pause on non-x86_64 architecture.
            sched_yield();
#endif
        }
        if (spin_count == 640)
        {
            spin_count = 0;
            sched_yield();
        }
    }
    LOG_INFO(logger, "io reactor loop finished");
}
} // namespace DB
