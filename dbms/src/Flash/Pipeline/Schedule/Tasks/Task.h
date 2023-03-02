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
#include <Common/Logger.h>
#include <Common/MemoryTracker.h>
#include <common/logger_useful.h>
#include <memory.h>

#include <magic_enum.hpp>

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
    INIT,
    WAITING,
    RUNNING,
    FINISHED,
    ERROR,
    CANCELLED,
};

class Task
{
public:
    Task()
        : mem_tracker(nullptr)
        , log(Logger::get())
    {}

    Task(MemoryTrackerPtr mem_tracker_, const String & req_id)
        : mem_tracker(std::move(mem_tracker_))
        , log(Logger::get(req_id))
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_task_construct_failpoint);
    }

    virtual ~Task() = default;

    MemoryTrackerPtr getMemTracker() const
    {
        return mem_tracker;
    }

    ExecTaskStatus execute() noexcept
    {
        assert(getMemTracker().get() == current_memory_tracker);
        switchStatus(executeImpl());
        return exec_status;
    }

    ExecTaskStatus await() noexcept
    {
        assert(getMemTracker().get() == current_memory_tracker);
        switchStatus(awaitImpl());
        return exec_status;
    }

protected:
    virtual ExecTaskStatus executeImpl() noexcept = 0;
    // Avoid allocating memory in `await` if possible.
    virtual ExecTaskStatus awaitImpl() noexcept { return ExecTaskStatus::RUNNING; }

private:
    void switchStatus(ExecTaskStatus to) noexcept
    {
        if (exec_status != to)
        {
            LOG_TRACE(log, "switch status: {} --> {}", magic_enum::enum_name(exec_status), magic_enum::enum_name(to));
            exec_status = to;
        }
    }

protected:
    MemoryTrackerPtr mem_tracker;
    LoggerPtr log;

private:
    ExecTaskStatus exec_status{ExecTaskStatus::INIT};
};
using TaskPtr = std::unique_ptr<Task>;

} // namespace DB
