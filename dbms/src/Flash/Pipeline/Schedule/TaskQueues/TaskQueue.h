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
// TODO support more kind of TaskQueue, such as
// - multi-level feedback queue
// - resource group queue
class TaskQueue
{
public:
    virtual ~TaskQueue() = default;

    virtual void submit(TaskPtr && task) noexcept = 0;

    virtual void submit(std::vector<TaskPtr> & tasks) noexcept = 0;

    // return false if the queue had been closed.
    virtual bool take(TaskPtr & task) noexcept = 0;

    virtual bool empty() noexcept = 0;

    virtual void close() = 0;

protected:
    LoggerPtr logger = Logger::get();
};
using TaskQueuePtr = std::unique_ptr<TaskQueue>;

} // namespace DB
