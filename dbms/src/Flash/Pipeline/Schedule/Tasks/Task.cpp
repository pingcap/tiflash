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

    // It is impossible for any task to change to init status.
    switch (to)
    {
        M(ExecTaskStatus::WAITING, type_to_waiting)
        M(ExecTaskStatus::RUNNING, type_to_running)
        M(ExecTaskStatus::IO, type_to_io)
        M(ExecTaskStatus::FINISHED, type_to_finished)
        M(ExecTaskStatus::ERROR, type_to_error)
        M(ExecTaskStatus::CANCELLED, type_to_cancelled)
        M(ExecTaskStatus::FINALIZE, type_to_finalize)
    default:
        RUNTIME_ASSERT(false, "unexpected task status: {}.", magic_enum::enum_name(to));
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
    : log(Logger::get())
    , mem_tracker(nullptr)
{
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_task_construct_failpoint);
    GET_METRIC(tiflash_pipeline_task_change_to_status, type_to_init).Increment();
}

Task::Task(MemoryTrackerPtr mem_tracker_, const String & req_id)
    : log(Logger::get(req_id))
    , mem_tracker(std::move(mem_tracker_))
{
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_task_construct_failpoint);
    GET_METRIC(tiflash_pipeline_task_change_to_status, type_to_init).Increment();
}

Task::~Task()
{
    RUNTIME_ASSERT(
        exec_status == ExecTaskStatus::FINALIZE,
        log,
        "The state of the Task must be {} before it is destructed, but it is actually {}",
        magic_enum::enum_name(ExecTaskStatus::FINALIZE),
        magic_enum::enum_name(exec_status));
}

ExecTaskStatus Task::execute()
{
    CHECK_FINISHED
    assert(getMemTracker().get() == current_memory_tracker);
    assertNormalStatus(ExecTaskStatus::RUNNING);
    switchStatus(executeImpl());
    return exec_status;
}

ExecTaskStatus Task::executeIO()
{
    CHECK_FINISHED
    assert(getMemTracker().get() == current_memory_tracker);
    assertNormalStatus(ExecTaskStatus::IO);
    switchStatus(executeIOImpl());
    return exec_status;
}

ExecTaskStatus Task::await()
{
    CHECK_FINISHED
    assert(getMemTracker().get() == current_memory_tracker);
    assertNormalStatus(ExecTaskStatus::WAITING);
    switchStatus(awaitImpl());
    return exec_status;
}

void Task::finalize()
{
    // To make sure that `finalize` only called once.
    RUNTIME_ASSERT(
        exec_status != ExecTaskStatus::FINALIZE,
        log,
        "finalize can only be called once.");
    switchStatus(ExecTaskStatus::FINALIZE);

    finalizeImpl();
#ifndef NDEBUG
    LOG_TRACE(log, "task finalize with profile info: {}", profile_info.toJson());
#endif // !NDEBUG
}

#undef CHECK_FINISHED

void Task::switchStatus(ExecTaskStatus to)
{
    if (exec_status != to)
    {
#ifndef NDEBUG
        LOG_TRACE(log, "switch status: {} --> {}", magic_enum::enum_name(exec_status), magic_enum::enum_name(to));
#endif // !NDEBUG
        addToStatusMetrics(to);
        exec_status = to;
    }
}

void Task::assertNormalStatus(ExecTaskStatus expect)
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
