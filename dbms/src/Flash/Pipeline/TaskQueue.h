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

#include <deque>
#include <memory>
#include <mutex>
#include <vector>

namespace DB
{
class TaskQueue;
using TaskQueuePtr = std::unique_ptr<TaskQueue>;

class TaskQueue
{
public:
    virtual ~TaskQueue() = default;

    virtual void submit(TaskPtr && task) = 0;

    virtual void submit(std::vector<TaskPtr> & tasks) = 0;

    // return false if the queue had been closed.
    virtual bool take(TaskPtr & task) = 0;

    virtual bool empty() = 0;

    virtual void close() = 0;
};

// TODO support more kind of TaskQueue, such as
// - multi-level feedback queue
// - resource group queue

class FIFOTaskQueue : public TaskQueue
{
public:
    void submit(TaskPtr && task) override;

    void submit(std::vector<TaskPtr> & tasks) override;

    bool take(TaskPtr & task) override;

    bool empty() override;

    void close() override;

private:
    mutable std::mutex mu;
    std::condition_variable cv;
    bool is_closed = false;
    std::deque<TaskPtr> task_queue;

    LoggerPtr logger = Logger::get("FIFOTaskQueue");
};
} // namespace DB
