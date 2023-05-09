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

#pragma once

#include <Common/Logger.h>
#include <Flash/Pipeline/Schedule/TaskQueues/TaskQueue.h>
#include <Flash/Pipeline/Schedule/TaskThreadPoolMetrics.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>

#include <thread>
#include <vector>

namespace DB
{
class TaskScheduler;

template <typename Impl>
class TaskThreadPool
{
public:
    TaskThreadPool(TaskScheduler & scheduler_, size_t thread_num);

    // After finish is called, the submitted task will be finalized directly.
    // And the remaing tasks will be executed normally.
    void finish();

    void waitForStop();

    void submit(TaskPtr && task);

    void submit(std::vector<TaskPtr> & tasks);

private:
    void loop(size_t thread_no);
    void doLoop(size_t thread_no);

    void handleTask(TaskPtr & task);

private:
    typename Impl::QueueType task_queue;

    LoggerPtr logger = Logger::get(Impl::NAME);

    TaskScheduler & scheduler;

    std::vector<std::thread> threads;

    TaskThreadPoolMetrics<Impl::is_cpu> metrics;
};
} // namespace DB
