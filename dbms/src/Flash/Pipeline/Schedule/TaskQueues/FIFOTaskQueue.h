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

#include <Flash/Pipeline/Schedule/TaskQueues/TaskQueue.h>

#include <deque>
#include <mutex>

namespace DB
{
class FIFOTaskQueue : public TaskQueue
{
public:
    ~FIFOTaskQueue() override;

    void submit(TaskPtr && task) override;

    void submit(std::vector<TaskPtr> & tasks) override;

    bool take(TaskPtr & task) override;

    void updateStatistics(const TaskPtr &, ExecTaskStatus, UInt64) override {}

    bool empty() const override;

    void finish() override;

private:
    mutable std::mutex mu;
    std::condition_variable cv;
    std::atomic_bool is_finished = false;
    std::deque<TaskPtr> task_queue;
};
} // namespace DB
