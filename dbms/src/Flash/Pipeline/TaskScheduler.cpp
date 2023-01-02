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

#include <Flash/Pipeline/TaskHelper.h>
#include <Flash/Pipeline/TaskScheduler.h>
#include <assert.h>
#include <common/likely.h>

namespace DB
{
TaskScheduler::TaskScheduler(const TaskSchedulerConfig & config)
    : task_executor(*this, config.task_executor_thread_num)
    , wait_reactor(*this)
    , spill_executor(*this, config.spill_executor_thread_num)
{
}

TaskScheduler::~TaskScheduler()
{
    task_executor.close();
    wait_reactor.close();
    spill_executor.close();
}

void TaskScheduler::submit(std::vector<TaskPtr> & tasks)
{
    if (unlikely(tasks.empty()))
        return;

    std::vector<TaskPtr> running_tasks;
    std::list<TaskPtr> waiting_tasks;
    for (auto & task : tasks)
    {
        assert(task);
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
            __builtin_unreachable();
        }
    }
    tasks.clear();
    task_executor.submit(running_tasks);
    wait_reactor.submit(waiting_tasks);
}

std::unique_ptr<TaskScheduler> TaskScheduler::instance;
} // namespace DB
