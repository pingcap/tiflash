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
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <common/logger_useful.h>

#include <magic_enum.hpp>

namespace DB
{
namespace FailPoints
{
extern const char random_pipeline_model_task_construct_failpoint[];
} // namespace FailPoints

namespace
{
// TODO supports more detailed status transfer metrics, such as from waiting to running.
void addToStatusMetrics(ExecTaskStatus to)
{
#define M(expect_status, metric_name)                                                \
    case (expect_status):                                                            \
    {                                                                                \
        GET_METRIC(tiflash_pipeline_task_change_to_status, metric_name).Increment(); \
        break;                                                                       \
    }

    switch (to)
    {
        M(ExecTaskStatus::INIT, type_to_init)
        M(ExecTaskStatus::WAITING, type_to_waiting)
        M(ExecTaskStatus::RUNNING, type_to_running)
        M(ExecTaskStatus::IO, type_to_io)
        M(ExecTaskStatus::FINISHED, type_to_finished)
        M(ExecTaskStatus::ERROR, type_to_error)
        M(ExecTaskStatus::CANCELLED, type_to_cancelled)
    default:
        throw Exception(fmt::format("Unknown task status: {}.", magic_enum::enum_name(to)), ErrorCodes::LOGICAL_ERROR);
    }

#undef M
}
} // namespace

#define CHECK_FINISHED                                        \
    if unlikely (exec_status == ExecTaskStatus::FINISHED      \
                 || exec_status == ExecTaskStatus::ERROR      \
                 || exec_status == ExecTaskStatus::CANCELLED) \
        return exec_status;

Task::Task()
    : mem_tracker(nullptr)
    , log(Logger::get())
{
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_task_construct_failpoint);
    GET_METRIC(tiflash_pipeline_task_change_to_status, type_to_init).Increment();
}

Task::Task(MemoryTrackerPtr mem_tracker_, const String & req_id)
    : mem_tracker(std::move(mem_tracker_))
    , log(Logger::get(req_id))
{
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_task_construct_failpoint);
    GET_METRIC(tiflash_pipeline_task_change_to_status, type_to_init).Increment();
}

ExecTaskStatus Task::execute() noexcept
{
    CHECK_FINISHED
    assert(getMemTracker().get() == current_memory_tracker);
    assertStatus(ExecTaskStatus::RUNNING);
    switchStatus(executeImpl());
    return exec_status;
}

ExecTaskStatus Task::executeIO() noexcept
{
    CHECK_FINISHED
    assert(getMemTracker().get() == current_memory_tracker);
    assertStatus(ExecTaskStatus::IO);
    switchStatus(executeIOImpl());
    return exec_status;
}

ExecTaskStatus Task::await() noexcept
{
    CHECK_FINISHED
    assert(getMemTracker().get() == current_memory_tracker);
    assertStatus(ExecTaskStatus::WAITING);
    switchStatus(awaitImpl());
    return exec_status;
}

void Task::finalize() noexcept
{
    RUNTIME_ASSERT(
        exec_status == ExecTaskStatus::FINISHED
            || exec_status == ExecTaskStatus::ERROR
            || exec_status == ExecTaskStatus::CANCELLED,
        log,
        "finalize must be called in FINISHED/ERROR/CANCELLED status, but actual status is {}",
        magic_enum::enum_name(exec_status));

    // To make sure that `finalize` only called once.
    exec_status = ExecTaskStatus::FINALIZE;
    finalizeImpl();
}

void Task::switchStatus(ExecTaskStatus to)
{
    if (exec_status != to)
    {
        LOG_TRACE(log, "switch status: {} --> {}", magic_enum::enum_name(exec_status), magic_enum::enum_name(to));
        addToStatusMetrics(to);
        exec_status = to;
    }
}

void Task::assertStatus(ExecTaskStatus expect)
{
    RUNTIME_ASSERT(
        exec_status == expect || exec_status == ExecTaskStatus::INIT,
        log,
        "actual status is {}, but expect status are {} and {}",
        magic_enum::enum_name(exec_status),
        magic_enum::enum_name(expect),
        magic_enum::enum_name(ExecTaskStatus::INIT));
}
} // namespace DB
