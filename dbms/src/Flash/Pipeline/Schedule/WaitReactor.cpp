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

#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Flash/Pipeline/Schedule/Task/TaskHelper.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/WaitReactor.h>
#include <assert.h>
#include <common/likely.h>
#include <common/logger_useful.h>
#include <errno.h>

namespace DB
{
namespace
{
class Spinner
{
public:
    Spinner(TaskThreadPool & task_thread_pool_, const LoggerPtr & logger_)
        : task_thread_pool(task_thread_pool_)
        , logger(logger_->getChild("Spinner"))
    {}

    // return true if the task is not in waiting status.
    bool awaitAndPushReadyTask(TaskPtr && task)
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
            UNEXPECTED_STATUS(logger, status);
        }
    }

    // return false if there are no ready task to submit.
    bool submitReadyTasks()
    {
        if (ready_tasks.empty())
            return false;

        task_thread_pool.submit(ready_tasks);
        ready_tasks.clear();
        spin_count = 0;
        return true;
    }

    void tryYield()
    {
        assert(ready_tasks.empty());
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

private:
    TaskThreadPool & task_thread_pool;

    LoggerPtr logger;

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

    Spinner spinner{scheduler.task_thread_pool, logger};
    std::list<TaskPtr> local_waiting_tasks;
    // Get the incremental tasks from wait queue.
    while (likely(wait_queue.take(local_waiting_tasks)))
    {
        auto task_it = local_waiting_tasks.begin();
        while (task_it != local_waiting_tasks.end())
        {
            if (spinner.awaitAndPushReadyTask(std::move(*task_it)))
                task_it = local_waiting_tasks.erase(task_it);
            else
                ++task_it;
            ASSERT_MEMORY_TRACKER
        }

        if (!spinner.submitReadyTasks())
            spinner.tryYield();
    }

    LOG_INFO(logger, "wait reactor loop finished");
}
} // namespace DB
