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

#include <Flash/Pipeline/Schedule/TaskQueues/FiFOTaskQueue.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>

namespace DB
{
struct CPUImpl
{
    static constexpr auto NAME = "cpu intensive";

    static constexpr bool is_cpu = true;

<<<<<<< HEAD:dbms/src/Flash/Pipeline/Schedule/TaskThreadPoolImpl.h
    static constexpr auto TargetStatus = ExecTaskStatus::RUNNING;
=======
    static bool isTargetStatus(ExecTaskStatus status) { return status == ExecTaskStatus::RUNNING; }
>>>>>>> 6638f2067b (Fix license and format coding style (#7962)):dbms/src/Flash/Pipeline/Schedule/ThreadPool/TaskThreadPoolImpl.h

    static ExecTaskStatus exec(TaskPtr & task) { return task->execute(); }

    using QueueType = std::unique_ptr<FIFOTaskQueue>;

    static QueueType newTaskQueue()
    {
        return std::make_unique<FIFOTaskQueue>();
    }
};

struct IOImpl
{
    static constexpr auto NAME = "io intensive";

    static constexpr bool is_cpu = false;

    static constexpr auto TargetStatus = ExecTaskStatus::IO;

    static ExecTaskStatus exec(TaskPtr & task) { return task->executeIO(); }

    using QueueType = std::unique_ptr<FIFOTaskQueue>;

    static QueueType newTaskQueue()
    {
        return std::make_unique<FIFOTaskQueue>();
    }
};
} // namespace DB
