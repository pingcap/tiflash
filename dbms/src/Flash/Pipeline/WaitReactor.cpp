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
#include <Flash/Pipeline/TaskHelper.h>
#include <Flash/Pipeline/TaskScheduler.h>
#include <Flash/Pipeline/WaitReactor.h>
#include <assert.h>
#include <common/likely.h>
#include <common/logger_useful.h>
#include <errno.h>
#include <immintrin.h>

namespace DB
{
namespace
{
// return true if the task is not in waiting status.
bool handle(std::vector<TaskPtr> & ready_tasks, TaskPtr && task)
{
    assert(task);
    TRACE_MEMORY(task);
    auto status = task->await();
    switch (status)
    {
    case ExecTaskStatus::WAITING:
        return false;
    case ExecTaskStatus::RUNNING:
        ready_tasks.push_back(std::move(task));
        return true;
    case FINISH_STATUS:
        task.reset();
        return true;
    default:
        __builtin_unreachable();
    }
}

class Spinner
{
public:
    void reset()
    {
        spin_count = 0;
    }

    void tryYield()
    {
        ++spin_count;

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

private:
    int16_t spin_count = 0;
};
} // namespace

WaitReactor::WaitReactor(TaskScheduler & scheduler_)
    : scheduler(scheduler_)
{
    thread = std::thread(&WaitReactor::loop, this);
}

void WaitReactor::close()
{
    {
        std::lock_guard lock(mu);
        is_closed = true;
    }
    cv.notify_one();
}

void WaitReactor::submit(TaskPtr && task)
{
    assert(task);
    {
        std::lock_guard lock(mu);
        waiting_tasks.emplace_back(std::move(task));
    }
    cv.notify_one();
}

void WaitReactor::submit(std::list<TaskPtr> & tasks)
{
    if (tasks.empty())
        return;
    {
        std::lock_guard lock(mu);
        waiting_tasks.splice(waiting_tasks.end(), tasks);
    }
    cv.notify_one();
}

WaitReactor::~WaitReactor()
{
    thread.join();
    LOG_INFO(logger, "stop wait reactor loop");
}

bool WaitReactor::take(std::list<TaskPtr> & local_waiting_tasks)
{
    {
        std::unique_lock lock(mu);
        while (true)
        {
            if (unlikely(is_closed))
                return false;
            if (!waiting_tasks.empty() || !local_waiting_tasks.empty())
                break;
            cv.wait(lock);
        }

        local_waiting_tasks.splice(local_waiting_tasks.end(), waiting_tasks);
    }
    assert(!local_waiting_tasks.empty());
    return true;
}

void WaitReactor::loop()
{
    assert(nullptr == current_memory_tracker);
    setThreadName("WaitReactor");
    LOG_INFO(logger, "start wait reactor loop");

    Spinner spinner;
    std::list<TaskPtr> local_waiting_tasks;
    std::vector<TaskPtr> ready_tasks;
    while (likely(take(local_waiting_tasks)))
    {
        assert(ready_tasks.empty());

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
            spinner.tryYield();
        }
        else
        {
            scheduler.task_executor.submit(ready_tasks);
            ready_tasks.clear();
            spinner.reset();
        }
    }

    LOG_INFO(logger, "wait reactor loop finished");
}
} // namespace DB
