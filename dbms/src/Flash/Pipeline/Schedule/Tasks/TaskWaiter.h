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

#include <Flash/Pipeline/Schedule/Tasks/Task.h>

#include <atomic>
#include <mutex>
#include <vector>

namespace DB
{
// A resource notification and an input stop race to take the same task. The losing notification
// must not submit it again, or consume a notification intended for another resource waiter.
class TaskWaiter
{
public:
    void setTask(TaskPtr && task_);
    bool notify();

private:
    std::mutex mutex;
    TaskPtr task;
};

// A persistent stop signal scoped by the pipeline builder, independent of query cancellation.
// Weak registrations do not keep completed tasks or their input resources alive.
class TaskStopSource
{
public:
    bool isStopped() const { return stopped.load(std::memory_order_acquire); }
    void stop();
    void registerWaiter(const TaskWaiterPtr & waiter);

private:
    std::atomic_bool stopped{false};
    std::mutex mutex;
    std::vector<std::weak_ptr<TaskWaiter>> waiters;
};
} // namespace DB
