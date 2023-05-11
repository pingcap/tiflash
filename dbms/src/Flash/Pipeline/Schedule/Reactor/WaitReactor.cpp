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
#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>
#include <Flash/Pipeline/Schedule/Reactor/WaitReactor.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>
#include <common/logger_useful.h>
#include <errno.h>

namespace DB
{
WaitReactor::WaitReactor(TaskScheduler & scheduler_)
    : scheduler(scheduler_)
    , spinner{scheduler, logger}
{
    GET_METRIC(tiflash_pipeline_scheduler, type_waiting_tasks_count).Set(0);
    thread = std::thread(&WaitReactor::loop, this);
}

void WaitReactor::finish()
{
    waiting_task_list.finish();
}

void WaitReactor::waitForStop()
{
    thread.join();
    LOG_INFO(logger, "wait reactor is stopped");
}

void WaitReactor::submit(TaskPtr && task)
{
    waiting_task_list.submit(std::move(task));
}

void WaitReactor::submit(std::list<TaskPtr> & tasks)
{
    waiting_task_list.submit(tasks);
}

bool WaitReactor::takeFromWaitingTaskList(std::list<TaskPtr> & local_waiting_tasks)
{
    return local_waiting_tasks.empty()
        ? waiting_task_list.take(local_waiting_tasks)
        // If the local waiting tasks are not empty, there is no need to be blocked here
        // and we can continue to process the leftover tasks in the local waiting tasks
        : waiting_task_list.tryTake(local_waiting_tasks);
}

void WaitReactor::react(std::list<TaskPtr> & local_waiting_tasks)
{
    for (auto task_it = local_waiting_tasks.begin(); task_it != local_waiting_tasks.end();)
    {
        if (spinner.awaitAndCollectReadyTask(std::move(*task_it)))
            task_it = local_waiting_tasks.erase(task_it);
        else
            ++task_it;
        ASSERT_MEMORY_TRACKER
    }
    GET_METRIC(tiflash_pipeline_scheduler, type_waiting_tasks_count).Set(local_waiting_tasks.size());

    spinner.submitReadyTasks();
}

void WaitReactor::loop()
{
    try
    {
        doLoop();
    }
    CATCH_AND_TERMINATE(logger)
}

void WaitReactor::doLoop()
{
    setThreadName("WaitReactor");
    LOG_INFO(logger, "start wait reactor loop");
    ASSERT_MEMORY_TRACKER

    std::list<TaskPtr> local_waiting_tasks;
    while (takeFromWaitingTaskList(local_waiting_tasks))
        react(local_waiting_tasks);
    // Handle remaining tasks.
    while (!local_waiting_tasks.empty())
        react(local_waiting_tasks);

    LOG_INFO(logger, "wait reactor loop finished");
}
} // namespace DB
