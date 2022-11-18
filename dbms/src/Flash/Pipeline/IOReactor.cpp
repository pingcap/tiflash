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

#include <Common/setThreadName.h>
#include <Flash/Pipeline/IOReactor.h>
#include <Flash/Pipeline/TaskScheduler.h>
#include <errno.h>

namespace DB
{
IOReactor::IOReactor(TaskScheduler & scheduler_)
    : scheduler(scheduler_)
{
    io_thread = std::thread(&IOReactor::loop, this);
}

void IOReactor::finish()
{
    if (is_shutdown.load() == false)
    {
        is_shutdown.store(true, std::memory_order_release);
        cond.notify_one();
    }
}

void IOReactor::submit(TaskPtr && task)
{
    {
        std::lock_guard lock(mutex);
        blocked_tasks.emplace_back(std::move(task));
    }
    cond.notify_one();
}

void IOReactor::loop()
{
    setThreadName("IOReactor");
    LOG_INFO(logger, "start io event loop");
    std::list<TaskPtr> local_blocked_tasks;
    int spin_count = 0;
    std::vector<TaskPtr> ready_tasks;
    while (!is_shutdown.load(std::memory_order_acquire))
    {
        if (local_blocked_tasks.empty())
        {
            std::unique_lock<std::mutex> lock(mutex);
            while (!is_shutdown.load(std::memory_order_acquire) && blocked_tasks.empty())
                cond.wait(lock);
            if (is_shutdown.load(std::memory_order_acquire))
                break;
            assert(!blocked_tasks.empty());
            local_blocked_tasks.splice(local_blocked_tasks.end(), blocked_tasks);
        }
        else
        {
            std::lock_guard lock(mutex);
            if (!blocked_tasks.empty())
                local_blocked_tasks.splice(local_blocked_tasks.end(), blocked_tasks);
        }

        auto task_it = local_blocked_tasks.begin();
        while (task_it != local_blocked_tasks.end())
        {
            if ((*task_it)->isBlocked())
            {
                ++task_it;
            }
            else
            {
                ready_tasks.emplace_back(std::move(*task_it));
                task_it = local_blocked_tasks.erase(task_it);
            }
        }

        if (ready_tasks.empty())
        {
            spin_count += 1;
        }
        else
        {
            spin_count = 0;
            scheduler.submitCPU(ready_tasks);
            assert(ready_tasks.empty());
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
    LOG_INFO(logger, "io loop finished");
}

IOReactor::~IOReactor()
{
    io_thread.join();
    LOG_INFO(logger, "stop io event loop");
}
} // namespace DB
