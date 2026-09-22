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
#include <Common/getNumberOfCPUCores.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>
#include <assert.h>
#include <common/likely.h>

#include <algorithm>
#include <cmath>
#include <magic_enum.hpp>

namespace DB
{
namespace
{
KeyspaceCpuLimiterPtr createKeyspaceCpuLimiter(const TaskSchedulerConfig & config)
{
    const auto cpu_limit_ratio = config.cpu_task_thread_pool_config.keyspace_cpu_limit_ratio;
    const auto pool_limit_ratio = config.cpu_task_thread_pool_config.keyspace_pool_limit_ratio;
    RUNTIME_CHECK(cpu_limit_ratio >= 0.0 && cpu_limit_ratio <= 1.0, cpu_limit_ratio);
    RUNTIME_CHECK(pool_limit_ratio >= 0.0 && pool_limit_ratio <= 1.0, pool_limit_ratio);

    size_t max_active_tasks = 0;
    UInt64 cpu_quota_per_second_ns = 0;
    const auto logical_cpu_cores = static_cast<double>(getNumberOfLogicalCPUCores());
    if (pool_limit_ratio > 0.0)
    {
        max_active_tasks = std::max<size_t>(1, static_cast<size_t>(std::floor(logical_cpu_cores * pool_limit_ratio)));
    }
    if (cpu_limit_ratio > 0.0)
    {
        cpu_quota_per_second_ns = std::max<UInt64>(
            1,
            static_cast<UInt64>(
                cpu_limit_ratio * logical_cpu_cores
                * static_cast<double>(std::chrono::seconds(1).count() * 1'000'000'000ULL)));
    }
    return std::make_shared<KeyspaceCpuLimiter>(max_active_tasks, cpu_quota_per_second_ns);
}
} // namespace

TaskScheduler::TaskScheduler(const TaskSchedulerConfig & config)
    : keyspace_cpu_limiter(createKeyspaceCpuLimiter(config))
    , cpu_task_thread_pool(*this, config.cpu_task_thread_pool_config, keyspace_cpu_limiter)
    , io_task_thread_pool(*this, config.io_task_thread_pool_config, keyspace_cpu_limiter)
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

void TaskScheduler::cancel(const TaskCancelInfo & cancel_info)
{
    cpu_task_thread_pool.cancel(cancel_info);
    io_task_thread_pool.cancel(cancel_info);
}

std::unique_ptr<TaskScheduler> TaskScheduler::instance;

} // namespace DB
