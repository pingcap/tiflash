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
#include <Flash/Pipeline/TaskExecutor.h>

namespace DB
{
struct TaskSchedulerConfig
{
    size_t task_executor_thread_num;
    size_t spill_executor_thread_num;
    size_t io_reactor_thread_num;
};

class TaskScheduler
{
public:
    explicit TaskScheduler(const TaskSchedulerConfig & config);

    void submit(std::vector<TaskPtr> & tasks);

    void close();

    static std::unique_ptr<TaskScheduler> instance;

private:
    TaskExecutor task_executor;

    LoggerPtr logger = Logger::get("TaskScheduler");
};
} // namespace DB
