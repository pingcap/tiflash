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
#include <Flash/Pipeline/Schedule/SpillThreadPool.h>
#include <Flash/Pipeline/Schedule/Task/Task.h>
#include <Flash/Pipeline/Schedule/TaskThreadPool.h>
#include <Flash/Pipeline/Schedule/WaitReactor.h>

namespace DB
{
struct TaskSchedulerConfig
{
    size_t task_thread_pool_size;
    size_t spill_thread_pool_size;
};

/**
 * ┌─────────────────────┐
 * │  task scheduler     │
 * │                     │
 * │ ┌─────────────────┐ │
 * │ │spill thread pool│ │
 * │ └──────▲──┬───────┘ │
 * │        │  │         │
 * │ ┌──────┴──▼──────┐  │
 * │ │task thread pool│  │
 * │ └──────▲──┬──────┘  │
 * │        │  │         │
 * │   ┌────┴──▼────┐    │
 * │   │wait reactor│    │
 * │   └────────────┘    │
 * │                     │
 * └─────────────────────┘
 * 
 * A globally shared execution scheduler, used by pipeline executor.
 * - task thread pool: for operator compute.
 * - spill thread pool: for spilling disk.
 * - wait reactor: for polling asynchronous io status, etc.
 */
class TaskScheduler
{
public:
    explicit TaskScheduler(const TaskSchedulerConfig & config);

    ~TaskScheduler();

    void submit(std::vector<TaskPtr> & tasks);

    static std::unique_ptr<TaskScheduler> instance;

private:
    TaskThreadPool task_thread_pool;

    WaitReactor wait_reactor;

    SpillThreadPool spill_thread_pool;

    LoggerPtr logger = Logger::get("TaskScheduler");

    friend class TaskThreadPool;
    friend class WaitReactor;
    friend class SpillThreadPool;
};
} // namespace DB
