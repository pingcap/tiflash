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

#include <Flash/Pipeline/Task.h>

#include <thread>

namespace DB
{
class TaskScheduler;

class TaskRunner
{
public:
    explicit TaskRunner(TaskScheduler & scheduler_);
    ~TaskRunner();

    void handleTask(TaskPtr && task);

private:
    void loop();

private:
    TaskScheduler & scheduler;

    std::thread cpu_thread;

    LoggerPtr logger = Logger::get("TaskRunner");
};
using TaskRunnerPtr = std::unique_ptr<TaskRunner>;
} // namespace DB
