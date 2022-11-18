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
#include <Flash/Pipeline/IOReactor.h>
#include <Flash/Pipeline/TaskRunner.h>

#include <atomic>
#include <list>
#include <mutex>
#include <deque>

namespace DB
{
struct TaskCounter
{
    TaskCounter(size_t count): counter(count) {}

    void finishOne();

    void waitAllFinished();

    std::atomic_int64_t counter = 0;

    mutable std::mutex global_mutex;
    std::condition_variable cv;
};

class TaskScheduler
{
public:
    TaskScheduler(size_t thread_num, std::vector<TaskPtr> & tasks);
    void submitCPU(std::vector<TaskPtr> & tasks);
    void submitCPU(TaskPtr && tasks);
    void submitIO(TaskPtr && task);
    bool popTask(TaskPtr & task);
    void finishOneTask();
    void waitForFinish();

private:
    IOReactor io_reactor;

    mutable std::mutex global_mutex;
    std::condition_variable cv;
    bool is_closed = false;
    std::deque<TaskPtr> task_queue;

    std::vector<TaskRunnerPtr> task_runners;

    TaskCounter task_counter;

    LoggerPtr logger = Logger::get("TaskScheduler");
};
} // namespace DB
