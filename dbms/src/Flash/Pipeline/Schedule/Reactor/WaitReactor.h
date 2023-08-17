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
#include <Flash/Pipeline/Schedule/Reactor/WaitingTaskList.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>

#include <list>
#include <thread>

namespace DB
{
class TaskScheduler;

// hold the `Task *` to avoid the cost of `TaskPtr->get()`
using WaitingTask = std::pair<TaskPtr, Task *>;
using WaitingTasks = std::list<WaitingTask>;

/// Used for batch calling task.await and submitting the tasks that have been removed from the waiting state to task thread pools.
/// When there is no non-waiting state task for a long time, it will try to let the current thread rest for a period of time to give the CPU to other threads.
class WaitReactor
{
public:
    explicit WaitReactor(TaskScheduler & scheduler_);

    // After finish is called, the submitted task will be finalized directly.
    // And the remaing tasks in waiting_task_list will be taken out and executed normally.
    void finish();

    void waitForStop();

    void submit(TaskPtr && task);

    void submit(std::list<TaskPtr> & tasks);

private:
    void loop();
    void doLoop();

    // Get the incremental tasks from waiting_task_list.
    // return false if waiting_task_list is empty and has finished.
    inline bool takeFromWaitingTaskList(WaitingTasks & local_waiting_tasks);

    inline void react(WaitingTasks & local_waiting_tasks);

    inline bool awaitAndCollectReadyTask(WaitingTask && task);

    inline void submitReadyTasks();

    inline void tryYield();

private:
    LoggerPtr logger = Logger::get();

    TaskScheduler & scheduler;

    std::thread thread;

    WaitingTaskList waiting_task_list;

    int16_t spin_count = 0;
    std::vector<TaskPtr> cpu_tasks;
    std::vector<TaskPtr> io_tasks;
};
} // namespace DB
