// Copyright 2022 PingCAP, Ltd.
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
#include <Flash/Pipeline/Task.h>
#include <Flash/Pipeline/TaskQueue.h>

#include <thread>
#include <vector>

namespace DB
{
class TaskScheduler;

class TaskExecutor
{
public:
    TaskExecutor(TaskScheduler & scheduler_, size_t thread_num);
    ~TaskExecutor();

    void submit(TaskPtr && task);

    void submit(std::vector<TaskPtr> & tasks);

    void close();

private:
    void loop();

    void handleTask(TaskPtr & task);

private:
    TaskScheduler & scheduler;

    std::vector<std::thread> threads;

    TaskQueuePtr task_queue = std::make_unique<FIFOTaskQueue>();

    LoggerPtr logger = Logger::get("TaskExecutor");
};
} // namespace DB
