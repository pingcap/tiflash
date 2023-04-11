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
#include <Flash/Pipeline/Schedule/TaskThreadPool.h>
#include <Flash/Pipeline/Schedule/TaskThreadPoolImpl.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Flash/Pipeline/Schedule/WaitReactor.h>

namespace DB
{
struct TaskSchedulerConfig
{
    size_t cpu_task_thread_pool_size;
    size_t io_task_thread_pool_size;
};

/**
 * ┌────────────────────────────┐
 * │      task scheduler        │
 * │                            │
 * │    ┌───────────────────┐   │
 * │ ┌──┤io task thread pool◄─┐ │
 * │ │  └──────▲──┬─────────┘ │ │
 * │ │         │  │           │ │
 * │ │ ┌───────┴──▼─────────┐ │ │
 * │ │ │cpu task thread pool│ │ │
 * │ │ └───────▲──┬─────────┘ │ │
 * │ │         │  │           │ │
 * │ │    ┌────┴──▼────┐      │ │
 * │ └────►wait reactor├──────┘ │
 * │      └────────────┘        │
 * │                            │
 * └────────────────────────────┘
 * 
 * A globally shared execution scheduler, used by pipeline executor.
 * - cpu task thread pool: for operator cpu intensive compute.
 * - io task thread pool: for operator io intensive block.
 * - wait reactor: for polling asynchronous io status, etc.
 */
class TaskScheduler
{
public:
    explicit TaskScheduler(const TaskSchedulerConfig & config);

    ~TaskScheduler();

    void submit(std::vector<TaskPtr> & tasks) noexcept;

    void submitToWaitReactor(TaskPtr && task);
    void submitToCPUTaskThreadPool(TaskPtr && task);
    void submitToCPUTaskThreadPool(std::vector<TaskPtr> & tasks);
    void submitToIOTaskThreadPool(TaskPtr && task);
    void submitToIOTaskThreadPool(std::vector<TaskPtr> & tasks);

    static std::unique_ptr<TaskScheduler> instance;

private:
    TaskThreadPool<CPUImpl> cpu_task_thread_pool;

    TaskThreadPool<IOImpl> io_task_thread_pool;

    WaitReactor wait_reactor;

    LoggerPtr logger = Logger::get();

    friend class TaskThreadPool<CPUImpl>;
    friend class WaitReactor;
};
} // namespace DB
