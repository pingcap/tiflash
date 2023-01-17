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
#include <Flash/Pipeline/Schedule/Task/TaskHelper.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <assert.h>
#include <common/likely.h>

#include <magic_enum.hpp>

namespace DB
{
TaskScheduler::TaskScheduler(const TaskSchedulerConfig & config)
    : task_thread_pool(*this, config.task_thread_pool_size)
    , wait_reactor(*this)
    , spill_thread_pool(*this, config.spill_thread_pool_size)
{
}

TaskScheduler::~TaskScheduler()
{
    task_thread_pool.close();
    wait_reactor.close();
    spill_thread_pool.close();

    task_thread_pool.waitForStop();
    wait_reactor.waitForStop();
    spill_thread_pool.waitForStop();
}

void TaskScheduler::submit(std::vector<TaskPtr> & tasks)
{
    if (unlikely(tasks.empty()))
        return;

    // The memory tracker is set by the caller.
    std::vector<TaskPtr> running_tasks;
    std::list<TaskPtr> waiting_tasks;
    for (auto & task : tasks)
    {
        assert(task);
        // A quick check to avoid an unnecessary round into `running_tasks` then being scheduled out immediately.
        auto status = task->await();
        switch (status)
        {
        case ExecTaskStatus::WAITING:
            waiting_tasks.push_back(std::move(task));
            break;
        case ExecTaskStatus::RUNNING:
            running_tasks.push_back(std::move(task));
            break;
        case FINISH_STATUS:
            task.reset();
            break;
        default:
            UNEXPECTED_STATUS(logger, status);
        }
    }
    tasks.clear();
    task_thread_pool.submit(running_tasks);
    wait_reactor.submit(waiting_tasks);
}

std::unique_ptr<TaskScheduler> TaskScheduler::instance;
} // namespace DB
