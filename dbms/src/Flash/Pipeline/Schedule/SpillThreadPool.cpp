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
#include <Flash/Pipeline/Schedule/SpillThreadPool.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>
#include <assert.h>
#include <common/likely.h>
#include <common/logger_useful.h>
#include <errno.h>

namespace DB
{
SpillThreadPool::SpillThreadPool(TaskScheduler & scheduler_, size_t thread_num)
    : scheduler(scheduler_)
{
    RUNTIME_CHECK(thread_num > 0);
    threads.reserve(thread_num);
    for (size_t i = 0; i < thread_num; ++i)
        threads.emplace_back(&SpillThreadPool::loop, this, i);
}

void SpillThreadPool::close()
{
    task_queue->close();
}

void SpillThreadPool::waitForStop()
{
    for (auto & thread : threads)
        thread.join();
    LOG_INFO(logger, "spill thread pool is stopped");
}

void SpillThreadPool::handleTask(TaskPtr && task, const LoggerPtr & log)
{
    assert(task);
    TRACE_MEMORY(task);

    Stopwatch stopwatch{CLOCK_MONOTONIC_COARSE};
    ExecTaskStatus status;
    while (true)
    {
        status = task->spill();
        // The spilling task should yield if it takes more than `YIELD_MAX_TIME_SPENT_NS`.
        if (status != ExecTaskStatus::SPILLING || stopwatch.elapsed() >= YIELD_MAX_TIME_SPENT_NS)
            break;
    }

    switch (status)
    {
    case ExecTaskStatus::RUNNING:
        scheduler.task_thread_pool.submit(std::move(task));
        break;
    case ExecTaskStatus::WAITING:
        scheduler.wait_reactor.submit(std::move(task));
        break;
    case ExecTaskStatus::SPILLING:
        submit(std::move(task));
        break;
    case FINISH_STATUS:
        task.reset();
        break;
    default:
        UNEXPECTED_STATUS(log, status);
    }
}

void SpillThreadPool::loop(size_t thread_no) noexcept
{
    auto thread_no_str = fmt::format("thread_no={}", thread_no);
    auto thread_logger = logger->getChild(thread_no_str);
    setThreadName(thread_no_str.c_str());
    LOG_INFO(thread_logger, "start loop");
    ASSERT_MEMORY_TRACKER

    TaskPtr task;
    while (likely(task_queue->take(task)))
    {
        handleTask(std::move(task), thread_logger);
        assert(!task);
        ASSERT_MEMORY_TRACKER
    }

    LOG_INFO(thread_logger, "loop finished");
}

void SpillThreadPool::submit(TaskPtr && task)
{
    task_queue->submit(std::move(task));
}

void SpillThreadPool::submit(std::vector<TaskPtr> & tasks)
{
    task_queue->submit(tasks);
}
} // namespace DB
