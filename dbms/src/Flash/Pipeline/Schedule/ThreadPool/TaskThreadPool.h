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

#pragma once

#include <Common/Logger.h>
#include <Flash/Pipeline/Schedule/TaskQueues/KeyspaceCpuLimiter.h>
#include <Flash/Pipeline/Schedule/TaskQueues/TaskQueue.h>
#include <Flash/Pipeline/Schedule/TaskQueues/TaskQueueType.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>
#include <Flash/Pipeline/Schedule/ThreadPool/TaskThreadPoolMetrics.h>

#include <magic_enum.hpp>
#include <thread>
#include <vector>

namespace DB
{
class TaskScheduler;

struct ThreadPoolConfig
{
    // NOLINTNEXTLINE(google-explicit-constructor)
    ThreadPoolConfig(size_t pool_size_)
        : pool_size(pool_size_)
    {}

    ThreadPoolConfig(
        size_t pool_size_,
        TaskQueueType queue_type_,
        double keyspace_cpu_limit_ratio_ = 0.0,
        double keyspace_pool_limit_ratio_ = 0.0)
        : pool_size(pool_size_)
        , queue_type(queue_type_)
        , keyspace_cpu_limit_ratio(keyspace_cpu_limit_ratio_)
        , keyspace_pool_limit_ratio(keyspace_pool_limit_ratio_)
    {}

    size_t pool_size;
    TaskQueueType queue_type = TaskQueueType::DEFAULT;
    // Only read from the CPU pool configuration. CPU and IO pipeline tasks
    // share the same per-keyspace limits.
    double keyspace_cpu_limit_ratio = 0.0;
    double keyspace_pool_limit_ratio = 0.0;

    String toString() const
    {
        return fmt::format(
            "[pool_size: {}, queue_type: {}, keyspace_cpu_limit_ratio: {}, keyspace_pool_limit_ratio: {}]",
            pool_size,
            magic_enum::enum_name(queue_type),
            keyspace_cpu_limit_ratio,
            keyspace_pool_limit_ratio);
    }
};

template <typename Impl>
class TaskThreadPool
{
public:
    TaskThreadPool(
        TaskScheduler & scheduler_,
        const ThreadPoolConfig & config,
        const KeyspaceCpuLimiterPtr & keyspace_cpu_limiter);

    // After finish is called, the submitted task will be finalized directly.
    // And the remaining tasks in task_queue will be taken out and executed normally.
    void finish();

    void waitForStop();

    void submit(TaskPtr && task);

    void submit(std::vector<TaskPtr> & tasks);

    void cancel(const TaskCancelInfo & cancel_info);

private:
    void loop(size_t thread_no);
    void doLoop(size_t thread_no);

    void handleTask(TaskPtr & task);

private:
    TaskQueuePtr task_queue;

    LoggerPtr logger = Logger::get(Impl::NAME);

    TaskScheduler & scheduler;

    std::vector<std::thread> threads;

    TaskThreadPoolMetrics<Impl::is_cpu> metrics;

    KeyspaceCpuLimiterPtr keyspace_cpu_limiter;
};

} // namespace DB
