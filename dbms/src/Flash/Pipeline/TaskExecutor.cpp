// Copyright 2022 PingCAP, Ltd.
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
#include <Common/MemoryTrackerSetter.h>
#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>
#include <Flash/Pipeline/TaskExecutor.h>
#include <Flash/Pipeline/TaskScheduler.h>
#include <common/likely.h>
#include <common/logger_useful.h>
#include <errno.h>

namespace DB
{
TaskExecutor::TaskExecutor(TaskScheduler & scheduler_, size_t thread_num)
    : scheduler(scheduler_)
{
    RUNTIME_CHECK(thread_num > 0);
    threads.reserve(thread_num);
    for (size_t i = 0; i < thread_num; ++i)
        threads.emplace_back(&TaskExecutor::loop, this);
}

TaskExecutor::~TaskExecutor()
{
    for (auto & thread : threads)
        thread.join();
    LOG_INFO(logger, "stop task executor");
}

void TaskExecutor::loop()
{
    setThreadName("TaskExecutor");
    LOG_INFO(logger, "start task executor loop");
    TaskPtr task;
    while (likely(popTask(task)))
    {
        handleTask(task);
    }
    LOG_INFO(logger, "task executor loop finished");
}

void TaskExecutor::handleTask(TaskPtr & task)
{
    assert(task);

    assert(nullptr == current_memory_tracker);
    // Hold the shared_ptr of memory tracker.
    // To avoid the current_memory_tracker being an illegal pointer.
    auto memory_tracker = task->getMemTracker();
    MemoryTrackerSetter memory_tracker_setter{true, memory_tracker.get()};

    int64_t time_spent = 0;
    while (true)
    {
        Stopwatch stopwatch{CLOCK_MONOTONIC_COARSE};
        assert(task);
        auto status = task->execute();
        switch (status)
        {
        case ExecTaskStatus::RUNNING:
        {
            time_spent += stopwatch.elapsed();
            static constexpr int64_t YIELD_MAX_TIME_SPENT = 100'000'000L;
            if (time_spent >= YIELD_MAX_TIME_SPENT)
            {
                submit(std::move(task));
                return;
            }
            break;
        }
        case ExecTaskStatus::WAITING:
            scheduler.io_reactor.submit(std::move(task));
            return;
        case ExecTaskStatus::SPILLING:
            scheduler.spill_executor.submit(std::move(task));
            return;
        case ExecTaskStatus::FINISHED:
        case ExecTaskStatus::ERROR:
        case ExecTaskStatus::CANCELLED:
            task.reset();
            return;
        default:
            __builtin_unreachable();
        }
    }
}

bool TaskExecutor::popTask(TaskPtr & task)
{
    {
        std::unique_lock lock(mu);
        while (true)
        {
            if (unlikely(is_closed))
                return false;
            if (!task_queue.empty())
                break;
            cv.wait(lock);
        }

        task = std::move(task_queue.front());
        task_queue.pop_front();
    }
    return true;
}

void TaskExecutor::close()
{
    {
        std::lock_guard lock(mu);
        is_closed = true;
    }
    cv.notify_all();
}

void TaskExecutor::submit(TaskPtr && task)
{
    assert(task);
    {
        std::lock_guard lock(mu);
        task_queue.push_back(std::move(task));
    }
    cv.notify_one();
}

void TaskExecutor::submit(std::vector<TaskPtr> & tasks)
{
    if (tasks.empty())
        return;

    std::lock_guard lock(mu);
    for (auto & task : tasks)
    {
        assert(task);
        task_queue.push_back(std::move(task));
        cv.notify_one();
    }
}
} // namespace DB
