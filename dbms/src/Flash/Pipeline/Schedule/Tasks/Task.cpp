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
ALWAYS_INLINE void addToStatusMetrics(ExecTaskStatus to)
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

Task::Task()
    : log(Logger::get())
    , mem_tracker_holder(nullptr)
    , mem_tracker_ptr(nullptr)
{
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_task_construct_failpoint);
    GET_METRIC(tiflash_pipeline_task_change_to_status, type_to_init).Increment();
}

Task::Task(MemoryTrackerPtr mem_tracker_, const String & req_id)
    : log(Logger::get(req_id))
    , mem_tracker_holder(std::move(mem_tracker_))
    , mem_tracker_ptr(mem_tracker_holder.get())
{
    assert(mem_tracker_holder.get() == mem_tracker_ptr);
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_task_construct_failpoint);
    GET_METRIC(tiflash_pipeline_task_change_to_status, type_to_init).Increment();
}

Task::~Task()
{
    if unlikely (task_status != ExecTaskStatus::FINALIZE)
    {
        LOG_WARNING(
            log,
            "The state of the Task should be {} before it is destructed, but it is actually {}",
            magic_enum::enum_name(ExecTaskStatus::FINALIZE),
            magic_enum::enum_name(task_status));
    }
}

#define CHECK_FINISHED                                        \
    if unlikely (task_status == ExecTaskStatus::FINISHED      \
                 || task_status == ExecTaskStatus::ERROR      \
                 || task_status == ExecTaskStatus::CANCELLED) \
        return task_status;

ExecTaskStatus Task::execute()
{
    CHECK_FINISHED
    assert(mem_tracker_ptr == current_memory_tracker);
    assert(task_status == ExecTaskStatus::RUNNING || task_status == ExecTaskStatus::INIT);
    switchStatus(executeImpl());
    return task_status;
}

ExecTaskStatus Task::executeIO()
{
    CHECK_FINISHED
    assert(mem_tracker_ptr == current_memory_tracker);
    assert(task_status == ExecTaskStatus::IO || task_status == ExecTaskStatus::INIT);
    switchStatus(executeIOImpl());
    return task_status;
}

ExecTaskStatus Task::await()
{
    CHECK_FINISHED
    assert(mem_tracker_ptr == current_memory_tracker);
    assert(task_status == ExecTaskStatus::WAITING || task_status == ExecTaskStatus::INIT);
    switchStatus(awaitImpl());
    return task_status;
}

void Task::finalize()
{
    // To make sure that `finalize` only called once.
    RUNTIME_ASSERT(
        task_status != ExecTaskStatus::FINALIZE,
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
    if (task_status != to)
    {
#ifndef NDEBUG
        LOG_TRACE(log, "switch status: {} --> {}", magic_enum::enum_name(task_status), magic_enum::enum_name(to));
#endif // !NDEBUG
        addToStatusMetrics(to);
        task_status = to;
    }
}
} // namespace DB
