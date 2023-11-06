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
#include <Flash/Pipeline/Schedule/TaskQueues/TaskQueue.h>
#include <Flash/Pipeline/Schedule/TaskQueues/TaskQueueType.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
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

    ThreadPoolConfig(size_t pool_size_, TaskQueueType queue_type_)
        : pool_size(pool_size_)
        , queue_type(queue_type_)
    {}

    size_t pool_size;
    TaskQueueType queue_type = TaskQueueType::DEFAULT;

    String toString() const
    {
        return fmt::format("[pool_size: {}, queue_type: {}]", pool_size, magic_enum::enum_name(queue_type));
    }
};

template <typename Impl>
class TaskThreadPool
{
public:
    TaskThreadPool(TaskScheduler & scheduler_, const ThreadPoolConfig & config);

    // After finish is called, the submitted task will be finalized directly.
    // And the remaing tasks in task_queue will be taken out and executed normally.
    void finish();

    void waitForStop();

    void submit(TaskPtr && task);

    void submit(std::vector<TaskPtr> & tasks);

    void cancel(const String & query_id, const String & resource_group_name);

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
};

} // namespace DB
