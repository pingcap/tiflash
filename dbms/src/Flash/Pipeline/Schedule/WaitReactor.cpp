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

#include <Common/Exception.h>
#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>
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
    Spinner(TaskScheduler & task_scheduler_, const LoggerPtr & logger_)
        : task_scheduler(task_scheduler_)
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
        case ExecTaskStatus::RUNNING:
            running_tasks.push_back(std::move(task));
            return true;
        case ExecTaskStatus::IO:
            io_tasks.push_back(std::move(task));
            return true;
        case ExecTaskStatus::WAITING:
            return false;
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
        if (running_tasks.empty() && io_tasks.empty())
            return false;

        task_scheduler.submitToCPUTaskThreadPool(running_tasks);
        running_tasks.clear();

        task_scheduler.submitToIOTaskThreadPool(io_tasks);
        io_tasks.clear();

        spin_count = 0;
        return true;
    }

    void tryYield()
    {
        assert(running_tasks.empty());
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
    TaskScheduler & task_scheduler;

    LoggerPtr logger;

    int16_t spin_count = 0;

    std::vector<TaskPtr> running_tasks;
    std::vector<TaskPtr> io_tasks;
};
} // namespace

WaitReactor::WaitReactor(TaskScheduler & scheduler_)
    : scheduler(scheduler_)
{
    GET_METRIC(tiflash_pipeline_scheduler, type_waiting_tasks_count).Set(0);
    thread = std::thread(&WaitReactor::loop, this);
}

void WaitReactor::close()
{
    waiting_task_list.close();
}

void WaitReactor::waitForStop()
{
    thread.join();
    LOG_INFO(logger, "wait reactor is stopped");
}

void WaitReactor::submit(TaskPtr && task) noexcept
{
    waiting_task_list.submit(std::move(task));
}

void WaitReactor::submit(std::list<TaskPtr> & tasks) noexcept
{
    waiting_task_list.submit(tasks);
}

void WaitReactor::loop() noexcept
{
    setThreadName("WaitReactor");
    LOG_INFO(logger, "start wait reactor loop");
    ASSERT_MEMORY_TRACKER

    Spinner spinner{scheduler, logger};
    std::list<TaskPtr> local_waiting_tasks;
    // Get the incremental tasks from waiting_task_list.
    // return false if waiting_task_list has been closed.
    auto take_from_waiting_task_list = [&]() {
        return local_waiting_tasks.empty()
            ? waiting_task_list.take(local_waiting_tasks)
            // If the local waiting tasks are not empty, there is no need to be blocked here
            // and we can continue to process the leftover tasks in the local waiting tasks
            : waiting_task_list.tryTake(local_waiting_tasks);
    };
    while (take_from_waiting_task_list())
    {
        assert(!local_waiting_tasks.empty());
        auto task_it = local_waiting_tasks.begin();
        while (task_it != local_waiting_tasks.end())
        {
            if (spinner.awaitAndPushReadyTask(std::move(*task_it)))
                task_it = local_waiting_tasks.erase(task_it);
            else
                ++task_it;
            ASSERT_MEMORY_TRACKER
        }

        GET_METRIC(tiflash_pipeline_scheduler, type_waiting_tasks_count).Set(local_waiting_tasks.size());

        if (!spinner.submitReadyTasks())
            spinner.tryYield();
    }

    LOG_INFO(logger, "wait reactor loop finished");
}
} // namespace DB
