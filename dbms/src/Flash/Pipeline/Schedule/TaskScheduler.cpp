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
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>
#include <assert.h>
#include <common/likely.h>

#include <magic_enum.hpp>

namespace DB
{
TaskScheduler::TaskScheduler(const TaskSchedulerConfig & config)
    : cpu_task_thread_pool(*this, config.cpu_task_thread_pool_config)
    , io_task_thread_pool(*this, config.io_task_thread_pool_config)
    , wait_reactor(*this)
{}

TaskScheduler::~TaskScheduler()
{
    cpu_task_thread_pool.finish();
    io_task_thread_pool.finish();
    wait_reactor.finish();

    cpu_task_thread_pool.waitForStop();
    io_task_thread_pool.waitForStop();
    wait_reactor.waitForStop();
}

void TaskScheduler::submit(TaskPtr && task)
{
    auto task_status = task->getStatus();
    switch (task_status)
    {
    case ExecTaskStatus::RUNNING:
        submitToCPUTaskThreadPool(std::move(task));
        break;
    case ExecTaskStatus::IO_IN:
    case ExecTaskStatus::IO_OUT:
        submitToIOTaskThreadPool(std::move(task));
        break;
    case ExecTaskStatus::WAITING:
        submitToWaitReactor(std::move(task));
        break;
    default:
        throw Exception(fmt::format("Unexpected task status: {}", magic_enum::enum_name(task_status)));
    }
}

void TaskScheduler::submit(std::vector<TaskPtr> & tasks)
{
    if (unlikely(tasks.empty()))
        return;

    std::vector<TaskPtr> cpu_tasks;
    std::vector<TaskPtr> io_tasks;
    std::list<TaskPtr> await_tasks;
    for (auto & task : tasks)
    {
        auto task_status = task->getStatus();
        switch (task_status)
        {
        case ExecTaskStatus::RUNNING:
            cpu_tasks.push_back(std::move(task));
            break;
        case ExecTaskStatus::IO_IN:
        case ExecTaskStatus::IO_OUT:
            io_tasks.push_back(std::move(task));
            break;
        case ExecTaskStatus::WAITING:
            await_tasks.push_back(std::move(task));
            break;
        default:
            throw Exception(fmt::format("Unexpected task status: {}", magic_enum::enum_name(task_status)));
        }
    }
    if (!cpu_tasks.empty())
        submitToCPUTaskThreadPool(cpu_tasks);
    if (!io_tasks.empty())
        submitToIOTaskThreadPool(io_tasks);
    if (!await_tasks.empty())
        wait_reactor.submit(await_tasks);
}

void TaskScheduler::submitToWaitReactor(TaskPtr && task)
{
    wait_reactor.submit(std::move(task));
}

void TaskScheduler::submitToCPUTaskThreadPool(TaskPtr && task)
{
    cpu_task_thread_pool.submit(std::move(task));
}

void TaskScheduler::submitToCPUTaskThreadPool(std::vector<TaskPtr> & tasks)
{
    cpu_task_thread_pool.submit(tasks);
}

void TaskScheduler::submitToIOTaskThreadPool(TaskPtr && task)
{
    io_task_thread_pool.submit(std::move(task));
}

void TaskScheduler::submitToIOTaskThreadPool(std::vector<TaskPtr> & tasks)
{
    io_task_thread_pool.submit(tasks);
}

void TaskScheduler::cancel(const String & query_id, const String & resource_group_name)
{
    cpu_task_thread_pool.cancel(query_id, resource_group_name);
    io_task_thread_pool.cancel(query_id, resource_group_name);
}

std::unique_ptr<TaskScheduler> TaskScheduler::instance;

} // namespace DB
