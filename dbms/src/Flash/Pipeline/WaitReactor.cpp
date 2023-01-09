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
class Spinner
{
public:
    explicit Spinner(TaskExecutor & task_executor_)
        : task_executor(task_executor_)
    {}

    // return true if the task is not in waiting status.
    bool await(TaskPtr && task)
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

    void submitAndTryYield()
    {
        if (ready_tasks.empty())
        {
            tryYield();
        }
        else
        {
            task_executor.submit(ready_tasks);
            reset();
        }
    }

private:
    void reset()
    {
        ready_tasks.clear();
        spin_count = 0;
    }

    void tryYield()
    {
        assert(ready_tasks.empty());
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
    TaskExecutor & task_executor;

    int16_t spin_count = 0;

    std::vector<TaskPtr> ready_tasks;
};
} // namespace

WaitReactor::WaitReactor(TaskScheduler & scheduler_)
    : scheduler(scheduler_)
{
    thread = std::thread(&WaitReactor::loop, this);
}

void WaitReactor::close()
{
    wait_queue.close();
}

void WaitReactor::waitForStop()
{
    thread.join();
    LOG_INFO(logger, "wait reactor is stopped");
}

void WaitReactor::submit(TaskPtr && task)
{
    wait_queue.submit(std::move(task));
}

void WaitReactor::submit(std::list<TaskPtr> & tasks)
{
    wait_queue.submit(tasks);
}

void WaitReactor::loop() noexcept
{
    setThreadName("WaitReactor");
    LOG_INFO(logger, "start wait reactor loop");
    ASSERT_MEMORY_TRACKER

    Spinner spinner{scheduler.task_executor};
    std::list<TaskPtr> local_waiting_tasks;
    while (likely(wait_queue.take(local_waiting_tasks)))
    {
        auto task_it = local_waiting_tasks.begin();
        while (task_it != local_waiting_tasks.end())
        {
            if (spinner.await(std::move(*task_it)))
                task_it = local_waiting_tasks.erase(task_it);
            else
                ++task_it;
            ASSERT_MEMORY_TRACKER
        }

        spinner.submitAndTryYield();
    }

    LOG_INFO(logger, "wait reactor loop finished");
}
} // namespace DB
