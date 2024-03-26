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


#include <Flash/Pipeline/Schedule/TaskQueues/TaskQueue.h>
#include <Flash/Pipeline/Schedule/TaskQueues/TaskQueueType.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>

namespace DB
{
struct CPUImpl
{
    static constexpr auto NAME = "cpu_pool";

    static constexpr bool is_cpu = true;

    static bool isTargetStatus(ExecTaskStatus status) { return status == ExecTaskStatus::RUNNING; }

    static ReturnStatus exec(TaskPtr & task) { return task->execute(); }

    static TaskQueuePtr newTaskQueue(TaskQueueType type);
};

struct IOImpl
{
    static constexpr auto NAME = "io_pool";

    static constexpr bool is_cpu = false;

    static bool isTargetStatus(ExecTaskStatus status)
    {
        return status == ExecTaskStatus::IO_IN || status == ExecTaskStatus::IO_OUT;
    }

    static ReturnStatus exec(TaskPtr & task) { return task->executeIO(); }

    static TaskQueuePtr newTaskQueue(TaskQueueType type);
};
} // namespace DB
