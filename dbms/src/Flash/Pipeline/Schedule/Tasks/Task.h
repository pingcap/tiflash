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
    FINALIZE,
};

class Task
{
public:
    Task();

    Task(MemoryTrackerPtr mem_tracker_, const String & req_id);

    virtual ~Task();

    MemoryTrackerPtr getMemTracker() const
    {
        return mem_tracker;
    }

    ExecTaskStatus execute();

    ExecTaskStatus executeIO();

    ExecTaskStatus await();

    void finalize();

public:
    LoggerPtr log;

protected:
    virtual ExecTaskStatus executeImpl() = 0;
    virtual ExecTaskStatus executeIOImpl() { return ExecTaskStatus::RUNNING; }
    // Avoid allocating memory in `await` if possible.
    virtual ExecTaskStatus awaitImpl() { return ExecTaskStatus::RUNNING; }

    // Used to release held resources, just like `Event::finishImpl`.
    virtual void finalizeImpl() {}

private:
    void switchStatus(ExecTaskStatus to);

    void assertNormalStatus(ExecTaskStatus expect);

protected:
    MemoryTrackerPtr mem_tracker;

private:
    ExecTaskStatus exec_status{ExecTaskStatus::INIT};
};
using TaskPtr = std::unique_ptr<Task>;

} // namespace DB
