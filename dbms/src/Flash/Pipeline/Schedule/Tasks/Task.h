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

#include <Common/Logger.h>
#include <Common/MemoryTracker.h>
#include <memory.h>

namespace DB
{
/**
 *    CANCELLED/ERROR/FINISHED
 *               ▲
 *               │
 *  ┌────────────────────────┐
 *  │     ┌──►RUNNING◄──┐    │
 *  │     │             │    │
 *  │     ▼             ▼    │
 *  │ WATITING◄────────►IO   │
 *  └────────────────────────┘
 */
enum class ExecTaskStatus
{
    INIT,
    WAITING,
    RUNNING,
    IO,
    FINISHED,
    ERROR,
    CANCELLED,
};

class Task
{
public:
    Task();

    Task(MemoryTrackerPtr mem_tracker_, const String & req_id);

    virtual ~Task() = default;

    MemoryTrackerPtr getMemTracker() const
    {
        return mem_tracker;
    }

    ExecTaskStatus execute() noexcept;

    ExecTaskStatus executeIO() noexcept;

    ExecTaskStatus await() noexcept;

protected:
    virtual ExecTaskStatus executeImpl() noexcept = 0;
    virtual ExecTaskStatus executeIOImpl() noexcept { return ExecTaskStatus::RUNNING; }
    // Avoid allocating memory in `await` if possible.
    virtual ExecTaskStatus awaitImpl() noexcept { return ExecTaskStatus::RUNNING; }

private:
    void switchStatus(ExecTaskStatus to) noexcept;

protected:
    MemoryTrackerPtr mem_tracker;
    LoggerPtr log;

private:
    ExecTaskStatus exec_status{ExecTaskStatus::INIT};
};
using TaskPtr = std::unique_ptr<Task>;

} // namespace DB
