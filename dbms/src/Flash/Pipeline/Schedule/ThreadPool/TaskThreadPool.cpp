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
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>
#include <Flash/Pipeline/Schedule/ThreadPool/TaskThreadPool.h>
#include <Flash/Pipeline/Schedule/ThreadPool/TaskThreadPoolImpl.h>
#include <common/likely.h>
#include <common/logger_useful.h>

#include <ext/scope_guard.h>

namespace DB
{
template <typename Impl>
TaskThreadPool<Impl>::TaskThreadPool(TaskScheduler & scheduler_, const ThreadPoolConfig & config)
    : task_queue(Impl::newTaskQueue(config.queue_type))
    , scheduler(scheduler_)
{
    RUNTIME_CHECK(config.pool_size > 0);
    threads.reserve(config.pool_size);
    for (size_t i = 0; i < config.pool_size; ++i)
        threads.emplace_back(&TaskThreadPool::loop, this, i);
}

template <typename Impl>
void TaskThreadPool<Impl>::finish()
{
    task_queue->finish();
}

template <typename Impl>
void TaskThreadPool<Impl>::waitForStop()
{
    for (auto & thread : threads)
        thread.join();
    LOG_INFO(logger, "task thread pool is stopped");
}

template <typename Impl>
void TaskThreadPool<Impl>::loop(size_t thread_no)
{
    try
    {
        doLoop(thread_no);
    }
    CATCH_AND_TERMINATE(logger)
}

template <typename Impl>
void TaskThreadPool<Impl>::doLoop(size_t thread_no)
{
    metrics.incThreadCnt();
    SCOPE_EXIT({ metrics.decThreadCnt(); });

    auto thread_no_str = fmt::format("thread_no={}", thread_no);
    auto thread_logger = logger->getChild(thread_no_str);
    setThreadName(thread_no_str.c_str());
    LOG_INFO(thread_logger, "start loop");

    TaskPtr task;
    while (likely(task_queue->take(task)))
    {
        metrics.decPendingTask();
        handleTask(task);
        assert(!task);
    }

    LOG_INFO(thread_logger, "loop finished");
}

template <typename Impl>
void TaskThreadPool<Impl>::handleTask(TaskPtr & task)
{
    assert(task);
    task->startTraceMemory();

    metrics.incExecutingTask();
    metrics.elapsedPendingTime(task);

    ExecTaskStatus status;
    UInt64 total_time_spent = 0;
    while (true)
    {
        status = Impl::exec(task);
        auto inc_time_spent = task->profile_info.elapsedFromPrev();
        task_queue->updateStatistics(task, inc_time_spent);
        total_time_spent += inc_time_spent;
        // The executing task should yield if it takes more than `YIELD_MAX_TIME_SPENT_NS`.
        if (status != Impl::TargetStatus || total_time_spent >= YIELD_MAX_TIME_SPENT_NS)
        {
            metrics.updateTaskMaxtimeOnRound(total_time_spent);
            break;
        }
    }
    metrics.addExecuteTime(task, total_time_spent);
    metrics.decExecutingTask();
    switch (status)
    {
    case ExecTaskStatus::RUNNING:
        task->endTraceMemory();
        scheduler.submitToCPUTaskThreadPool(std::move(task));
        break;
    case ExecTaskStatus::IO:
        task->endTraceMemory();
        scheduler.submitToIOTaskThreadPool(std::move(task));
        break;
    case ExecTaskStatus::WAITING:
        task->endTraceMemory();
        scheduler.submitToWaitReactor(std::move(task));
        break;
    case FINISH_STATUS:
        task->finalize();
        task->endTraceMemory();
        task.reset();
        break;
    default:
        UNEXPECTED_STATUS(task->log, status);
    }
}

template <typename Impl>
void TaskThreadPool<Impl>::submit(TaskPtr && task)
{
    metrics.incPendingTask(1);
    task_queue->submit(std::move(task));
}

template <typename Impl>
void TaskThreadPool<Impl>::submit(std::vector<TaskPtr> & tasks)
{
    metrics.incPendingTask(tasks.size());
    task_queue->submit(tasks);
}

template class TaskThreadPool<CPUImpl>;
template class TaskThreadPool<IOImpl>;

} // namespace DB
