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

#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <common/logger_useful.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>

#include <magic_enum.hpp>

namespace DB
{
namespace FailPoints
{
extern const char random_pipeline_model_task_construct_failpoint[];
} // namespace FailPoints

namespace
{
    
}

    Task::Task()
        : mem_tracker(nullptr)
        , log(Logger::get())
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_task_construct_failpoint);
    }

    Task::Task(MemoryTrackerPtr mem_tracker_, const String & req_id)
        : mem_tracker(std::move(mem_tracker_))
        , log(Logger::get(req_id))
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_task_construct_failpoint);
    }

    ExecTaskStatus Task::execute() noexcept
    {
        assert(getMemTracker().get() == current_memory_tracker);
        if (switchStatus(executeImpl()))
        {
            
        }
        return exec_status;
    }

    ExecTaskStatus Task::await() noexcept
    {
        assert(getMemTracker().get() == current_memory_tracker);
        if (switchStatus(awaitImpl()))
        {
            
        }
        return exec_status;
    }

    bool Task::switchStatus(ExecTaskStatus to) noexcept
    {
        if (exec_status != to)
        {
            LOG_TRACE(log, "switch status: {} --> {}", magic_enum::enum_name(exec_status), magic_enum::enum_name(to));
            exec_status = to;
            return true;
        }
        return false;
    }
} // namespace DB
