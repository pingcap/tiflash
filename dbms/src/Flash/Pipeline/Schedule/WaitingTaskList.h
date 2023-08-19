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

#include <list>
#include <mutex>

namespace DB
{
class WaitingTaskList
{
public:
    /// return false if the waiting task list had been closed.
    // this function will wait until `!waiting_tasks.empty()`
    bool take(std::list<TaskPtr> & local_waiting_tasks) noexcept;
    // this function will return immediately.
    bool tryTake(std::list<TaskPtr> & local_waiting_tasks) noexcept;

    void submit(TaskPtr && task) noexcept;

    void submit(std::list<TaskPtr> & tasks) noexcept;

    void close();

private:
    std::mutex mu;
    std::condition_variable cv;
    std::list<TaskPtr> waiting_tasks;
    bool is_closed = false;
};
} // namespace DB
