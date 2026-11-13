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

#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <common/logger_useful.h>

#include <ext/scope_guard.h>
#include <magic_enum.hpp>
#if defined(__linux__)
#include <sys/syscall.h>
#include <unistd.h>
#else
#include <pthread.h>
#endif

namespace DB
{
namespace FailPoints
{
extern const char random_pipeline_model_task_construct_failpoint[];
extern const char random_pipeline_model_task_run_failpoint[];
extern const char random_pipeline_model_cancel_failpoint[];
extern const char exception_during_query_run[];
} // namespace FailPoints

namespace
{
UInt64 currentDiagnosticThreadId()
{
#if defined(__linux__)
    return static_cast<UInt64>(syscall(SYS_gettid));
#elif defined(__APPLE__)
    UInt64 id = 0;
    pthread_threadid_np(nullptr, &id);
    return id;
#else
    return static_cast<UInt64>(pthread_getthreadid_np());
#endif
}
// TODO supports more detailed status transfer metrics, such as from waiting to running.
ALWAYS_INLINE void addToStatusMetrics(ExecTaskStatus to)
{
#define M(expect_status, metric_name)                                                                                \
    case (expect_status):                                                                                            \
    {                                                                                                                \
        thread_local auto & metrics_##metric_name = GET_METRIC(tiflash_pipeline_task_change_to_status, metric_name); \
        (metrics_##metric_name).Increment();                                                                         \
        break;                                                                                                       \
    }

    switch (to)
    {
        M(ExecTaskStatus::WAITING, type_to_waiting)
        M(ExecTaskStatus::WAIT_FOR_NOTIFY, type_to_wait_for_notify)
        M(ExecTaskStatus::RUNNING, type_to_running)
        M(ExecTaskStatus::IO_IN, type_to_io)
        M(ExecTaskStatus::IO_OUT, type_to_io)
        M(ExecTaskStatus::FINISHED, type_to_finished)
        M(ExecTaskStatus::ERROR, type_to_error)
        M(ExecTaskStatus::CANCELLED, type_to_cancelled)
    default:
        RUNTIME_ASSERT(false, "unexpected task status: {}.", magic_enum::enum_name(to));
    }

#undef M
}
} // namespace

Task::Task(PipelineExecutorContext & exec_context_, const String & req_id, ExecTaskStatus init_status)
    : log(Logger::get(req_id))
    , exec_context(exec_context_)
    , mem_tracker_holder(exec_context_.getMemoryTracker())
    , mem_tracker_ptr(mem_tracker_holder.get())
    , task_status(init_status)
    , diagnostic_status(init_status)
{
    assert(mem_tracker_holder.get() == mem_tracker_ptr);
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_task_construct_failpoint);
    addToStatusMetrics(task_status);

    exec_context.registerTask(this);
}

Task::Task(PipelineExecutorContext & exec_context_)
    : Task(exec_context_, "")
{}

Task::~Task()
{
    if unlikely (!is_finalized)
    {
        LOG_WARNING(
            log,
            "Task should be finalized before destructing, but not, the status at this time is {}. The possible reason "
            "is that an error was reported during task creation",
            magic_enum::enum_name(task_status));
    }

    // In order to ensure that `PipelineExecutorContext` will not be destructed before `Task` is destructed.
    exec_context.unregisterTask(this);
}

#define EXECUTE(function)                                                                   \
    static thread_local const auto thread_id = currentDiagnosticThreadId();                 \
    diagnostic_thread_id.store(thread_id, std::memory_order_relaxed);                       \
    SCOPE_EXIT({ diagnostic_thread_id.store(0, std::memory_order_relaxed); });              \
    fiu_do_on(FailPoints::random_pipeline_model_cancel_failpoint, exec_context.cancel());   \
    if unlikely (exec_context.isCancelled())                                                \
    {                                                                                       \
        switchStatus(ExecTaskStatus::CANCELLED);                                            \
        return task_status;                                                                 \
    }                                                                                       \
    try                                                                                     \
    {                                                                                       \
        auto status = (function());                                                         \
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_task_run_failpoint); \
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_query_run);               \
        switchStatus(status);                                                               \
        return task_status;                                                                 \
    }                                                                                       \
    catch (...)                                                                             \
    {                                                                                       \
        LOG_WARNING(log, "error occurred and cancel the query");                            \
        clearNotifyFuture();                                                                \
        exec_context.onErrorOccurred(std::current_exception());                             \
        switchStatus(ExecTaskStatus::ERROR);                                                \
        return task_status;                                                                 \
    }

void Task::onErrorOccurred(const String & err_msg)
{
    exec_context.onErrorOccurred(err_msg);
}

ExecTaskStatus Task::execute()
{
    assert(current_notify_future == nullptr);
    assert(mem_tracker_ptr == current_memory_tracker);
    assert(task_status == ExecTaskStatus::RUNNING);
    EXECUTE(executeImpl);
}

ExecTaskStatus Task::executeIO()
{
    assert(current_notify_future == nullptr);
    assert(mem_tracker_ptr == current_memory_tracker);
    assert(task_status == ExecTaskStatus::IO_IN || task_status == ExecTaskStatus::IO_OUT);
    EXECUTE(executeIOImpl);
}

ExecTaskStatus Task::await()
{
    assert(current_notify_future == nullptr);
    // Because await only performs polling checks and does not involve computing/memory tracker memory allocation,
    // await will not invoke MemoryTracker, so current_memory_tracker must be nullptr here.
    assert(current_memory_tracker == nullptr);
    assert(task_status == ExecTaskStatus::WAITING);
    EXECUTE(awaitImpl);
}

#undef EXECUTE

void Task::notify()
{
    diagnostic_thread_id.store(currentDiagnosticThreadId(), std::memory_order_relaxed);
    SCOPE_EXIT({ diagnostic_thread_id.store(0, std::memory_order_relaxed); });
    assert(task_status == ExecTaskStatus::WAIT_FOR_NOTIFY);
    auto next_status = notifyImpl();
    // If the query has been canceled,
    // move the task to WaitReactor to quickly trigger the cancel process.
    if (unlikely(exec_context.isCancelled()))
        next_status = ExecTaskStatus::WAITING;
    switchStatus(next_status);
    profile_info.elapsedWaitForNotifyTime();
}

void Task::finalize()
{
    diagnostic_thread_id.store(currentDiagnosticThreadId(), std::memory_order_relaxed);
    SCOPE_EXIT({ diagnostic_thread_id.store(0, std::memory_order_relaxed); });
    assert(current_notify_future == nullptr);
    // To make sure that `finalize` only called once.
    RUNTIME_ASSERT(!is_finalized, log, "finalize can only be called once.");
    is_finalized = true;

    try
    {
        finalizeImpl();
    }
    catch (...)
    {
        exec_context.onErrorOccurred(std::current_exception());
    }

    profile_info.reportMetrics();
#ifndef NDEBUG
    LOG_TRACE(
        log,
        "task(resource group: {}) finalize with profile info: {}",
        getQueryExecContext().getResourceGroupName(),
        profile_info.toJson());
#endif // !NDEBUG
}

void Task::switchStatus(ExecTaskStatus to)
{
    if (task_status != to)
    {
#ifndef NDEBUG
        LOG_TRACE(log, "switch status: {} --> {}", magic_enum::enum_name(task_status), magic_enum::enum_name(to));
#endif // !NDEBUG
        addToStatusMetrics(to);
        task_status = to;
        diagnostic_status.store(to, std::memory_order_relaxed);
    }
}

void Task::describeOperators(String operators)
{
    exec_context.describeTask(this, std::move(operators));
}

const String & Task::getQueryId() const
{
    return exec_context.getQueryId();
}

const String & Task::getResourceGroupName() const
{
    return exec_context.getResourceGroupName();
}

const KeyspaceID & Task::getKeyspaceID() const
{
    return exec_context.getKeyspaceID();
}
} // namespace DB
