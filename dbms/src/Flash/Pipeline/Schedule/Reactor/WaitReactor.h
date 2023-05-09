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
#include <Flash/Pipeline/Schedule/Reactor/Spinner.h>
#include <Flash/Pipeline/Schedule/Reactor/WaitingTaskList.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>

#include <list>
#include <thread>

namespace DB
{
class TaskScheduler;

class WaitReactor
{
public:
    explicit WaitReactor(TaskScheduler & scheduler_);

    // After finish is called, the submitted task will be finalized directly.
    // And the remaing tasks will be executed normally.
    void finish();

    void waitForStop();

    void submit(TaskPtr && task);

    void submit(std::list<TaskPtr> & tasks);

private:
    void loop();
    void doLoop();

    // Get the incremental tasks from waiting_task_list.
    // return false if waiting_task_list is empty and has finished.
    bool takeFromWaitingTaskList(std::list<TaskPtr> & local_waiting_tasks);

    void react(std::list<TaskPtr> & local_waiting_tasks);

private:
    WaitingTaskList waiting_task_list;

    LoggerPtr logger = Logger::get();

    TaskScheduler & scheduler;

    Spinner spinner;

    std::thread thread;
};
} // namespace DB
