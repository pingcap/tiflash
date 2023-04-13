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

#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/TaskThreadPool.h>
#include <Flash/Pipeline/Schedule/TaskThreadPoolImpl.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>
#include <common/likely.h>
#include <common/logger_useful.h>

#include <ext/scope_guard.h>

namespace DB
{
template <typename Impl>
TaskThreadPool<Impl>::TaskThreadPool(TaskScheduler & scheduler_, size_t thread_num)
    : task_queue(Impl::newTaskQueue())
    , scheduler(scheduler_)
{
    RUNTIME_CHECK(thread_num > 0);
    threads.reserve(thread_num);
    for (size_t i = 0; i < thread_num; ++i)
        threads.emplace_back(&TaskThreadPool::loop, this, i);
}

template <typename Impl>
void TaskThreadPool<Impl>::close()
{
    task_queue->close();
}

template <typename Impl>
void TaskThreadPool<Impl>::waitForStop()
{
    for (auto & thread : threads)
        thread.join();
    LOG_INFO(logger, "task thread pool is stopped");
}

template <typename Impl>
void TaskThreadPool<Impl>::loop(size_t thread_no) noexcept
{
    metrics.incThreadCnt();
    SCOPE_EXIT({ metrics.decThreadCnt(); });

    auto thread_no_str = fmt::format("thread_no={}", thread_no);
    auto thread_logger = logger->getChild(thread_no_str);
    setThreadName(thread_no_str.c_str());
    LOG_INFO(thread_logger, "start loop");
    ASSERT_MEMORY_TRACKER

    TaskPtr task;
    while (likely(task_queue->take(task)))
    {
        metrics.decPendingTask();
        handleTask(task, thread_logger);
        assert(!task);
        ASSERT_MEMORY_TRACKER
    }

    LOG_INFO(thread_logger, "loop finished");
}

template <typename Impl>
void TaskThreadPool<Impl>::handleTask(TaskPtr & task, const LoggerPtr & log) noexcept
{
    assert(task);
    TRACE_MEMORY(task);

    metrics.incExecutingTask();

    Stopwatch stopwatch{CLOCK_MONOTONIC_COARSE};
    ExecTaskStatus status;
    while (true)
    {
        status = Impl::exec(task);
        auto execute_time_ns = stopwatch.elapsed();
        // The executing task should yield if it takes more than `YIELD_MAX_TIME_SPENT_NS`.
        if (status != Impl::TargetStatus || execute_time_ns >= YIELD_MAX_TIME_SPENT_NS)
        {
            metrics.updateTaskMaxtimeOnRound(execute_time_ns);
            break;
        }
    }

    metrics.decExecutingTask();
    switch (status)
    {
    case ExecTaskStatus::RUNNING:
        scheduler.submitToCPUTaskThreadPool(std::move(task));
        break;
    case ExecTaskStatus::IO:
        scheduler.submitToIOTaskThreadPool(std::move(task));
        break;
    case ExecTaskStatus::WAITING:
        scheduler.submitToWaitReactor(std::move(task));
        break;
    case FINISH_STATUS:
        task.reset();
        break;
    default:
        UNEXPECTED_STATUS(log, status);
    }
}

template <typename Impl>
void TaskThreadPool<Impl>::submit(TaskPtr && task) noexcept
{
    metrics.incPendingTask(1);
    task_queue->submit(std::move(task));
}

template <typename Impl>
void TaskThreadPool<Impl>::submit(std::vector<TaskPtr> & tasks) noexcept
{
    metrics.incPendingTask(tasks.size());
    task_queue->submit(tasks);
}

template class TaskThreadPool<CPUImpl>;
template class TaskThreadPool<IOImpl>;

} // namespace DB
