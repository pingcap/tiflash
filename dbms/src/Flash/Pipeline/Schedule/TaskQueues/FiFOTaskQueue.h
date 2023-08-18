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

#include <deque>
#include <mutex>

namespace DB
{
class FIFOTaskQueue : public TaskQueue
{
public:
    void submit(TaskPtr && task) noexcept override;

    void submit(std::vector<TaskPtr> & tasks) noexcept override;

    bool take(TaskPtr & task) noexcept override;

    bool empty() noexcept override;

    void close() override;

private:
    std::mutex mu;
    std::condition_variable cv;
    bool is_closed = false;
    std::deque<TaskPtr> task_queue;
};
} // namespace DB
