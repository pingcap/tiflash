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
#include <Flash/Pipeline/Schedule/Task/TaskHelper.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <assert.h>
#include <common/likely.h>
#include <common/logger_useful.h>
#include <errno.h>

#include <magic_enum.hpp>

namespace DB
{
SpillThreadPool::SpillThreadPool(TaskScheduler & scheduler_, size_t thread_num)
    : scheduler(scheduler_)
{
    RUNTIME_CHECK(thread_num > 0);
    threads.reserve(thread_num);
    for (size_t i = 0; i < thread_num; ++i)
        threads.emplace_back(&SpillThreadPool::loop, this);
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

void SpillThreadPool::submit(TaskPtr && task)
{
    task_queue->submit(std::move(task));
}

void SpillThreadPool::handleTask(TaskPtr && task)
{
    assert(task);
    TRACE_MEMORY(task);

    Stopwatch stopwatch{CLOCK_MONOTONIC_COARSE};
    ExecTaskStatus status;
    while (true)
    {
        status = task->spill();
        if (status != ExecTaskStatus::SPILLING || stopwatch.elapsed() >= YIELD_MAX_TIME_SPENT)
            break;
    }

    switch (status)
    {
    case ExecTaskStatus::RUNNING:
        scheduler.task_thread_pool.submit(std::move(task));
        break;
    case ExecTaskStatus::SPILLING:
        submit(std::move(task));
        break;
    case FINISH_STATUS:
        task.reset();
        break;
    default:
        RUNTIME_ASSERT(false, logger, "Unexpected task state {}", magic_enum::enum_name(status));
    }
}

void SpillThreadPool::loop() noexcept
{
    setThreadName("SpillThreadPool");
    LOG_INFO(logger, "start spill thread pool loop");
    ASSERT_MEMORY_TRACKER

    TaskPtr task;
    while (likely(task_queue->take(task)))
    {
        handleTask(std::move(task));
        assert(!task);
        ASSERT_MEMORY_TRACKER
    }

    LOG_INFO(logger, "spill thread pool loop finished");
}
} // namespace DB
