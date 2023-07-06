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

#include <memory>
#include <vector>

namespace DB
{
class TaskQueue
{
public:
    virtual ~TaskQueue() = default;

    virtual void submit(TaskPtr && task) = 0;

    virtual void submit(std::vector<TaskPtr> & tasks) = 0;

    // return false if the queue is empty and finished.
    virtual bool take(TaskPtr & task) = 0;

    // Update the execution metrics of the task taken from the queue.
    // Used to adjust the priority of tasks within a queue.
    virtual void updateStatistics(const TaskPtr & task, ExecTaskStatus exec_task_status, UInt64 inc_ns) = 0;

    virtual bool empty() const = 0;

    // After finish is called, the submitted task will be finalized directly and will not be taken.
    // And the tasks in the queue can still be taken normally.
    virtual void finish() = 0;

protected:
    LoggerPtr logger = Logger::get();
};
using TaskQueuePtr = std::unique_ptr<TaskQueue>;

} // namespace DB
