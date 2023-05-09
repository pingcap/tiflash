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
#include <Flash/Pipeline/Schedule/Tasks/Task.h>

#include <vector>

namespace DB
{
class TaskScheduler;

class Spinner
{
public:
    Spinner(TaskScheduler & task_scheduler_, const LoggerPtr & logger_)
        : task_scheduler(task_scheduler_)
        , logger(logger_->getChild("Spinner"))
    {}

    // return true if the task is not in waiting status.
    bool awaitAndPushReadyTask(TaskPtr && task);

    void submitReadyTasks();

    void tryYield();

private:
    TaskScheduler & task_scheduler;

    LoggerPtr logger;

    int16_t spin_count = 0;

    std::vector<TaskPtr> cpu_tasks;
    std::vector<TaskPtr> io_tasks;
};
} // namespace DB
