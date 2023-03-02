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

#include <Common/FailPoint.h>
#include <Common/MemoryTracker.h>
#include <memory.h>

namespace DB
{
namespace FailPoints
{
extern const char random_pipeline_model_task_construct_failpoint[];
} // namespace FailPoints

/**
 *    CANCELLED/ERROR/FINISHED
 *               ▲
 *               │
 *  ┌────────────────────────┐
 *  │ WATITING◄─────►RUNNING │
 *  └────────────────────────┘
 */
enum class ExecTaskStatus
{
    WAITING,
    RUNNING,
    FINISHED,
    ERROR,
    CANCELLED,
};

class Task
{
public:
    explicit Task(MemoryTrackerPtr mem_tracker_)
        : mem_tracker(std::move(mem_tracker_))
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_task_construct_failpoint);
    }

    virtual ~Task() = default;

    MemoryTrackerPtr getMemTracker()
    {
        return mem_tracker;
    }

    ExecTaskStatus execute() noexcept
    {
        assert(getMemTracker().get() == current_memory_tracker);
        return executeImpl();
    }
    // Avoid allocating memory in `await` if possible.
    ExecTaskStatus await() noexcept
    {
        assert(getMemTracker().get() == current_memory_tracker);
        return awaitImpl();
    }

protected:
    virtual ExecTaskStatus executeImpl() noexcept = 0;
    virtual ExecTaskStatus awaitImpl() noexcept { return ExecTaskStatus::RUNNING; }

private:
    MemoryTrackerPtr mem_tracker;
};
using TaskPtr = std::unique_ptr<Task>;

} // namespace DB
