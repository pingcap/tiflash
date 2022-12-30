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
#include <Common/setThreadName.h>
#include <Flash/Pipeline/SpillExecutor.h>
#include <Flash/Pipeline/TaskScheduler.h>
#include <assert.h>
#include <common/likely.h>
#include <common/logger_useful.h>
#include <errno.h>

namespace DB
{
SpillExecutor::SpillExecutor(TaskScheduler & scheduler_, size_t thread_num)
    : scheduler(scheduler_)
{
    if (thread_num > 0)
    {
        submit_func = [&](TaskPtr && task) {
            submitToThreadSharedQueue(std::move(task));
        };
        threads.reserve(thread_num);
        for (size_t i = 0; i < thread_num; ++i)
            threads.emplace_back(&SpillExecutor::loop, this);
    }
    else
    {
        // If thread_num is 0, we will execute spill in the calling thread.
        submit_func = [&](TaskPtr && task) {
            handleTask(std::move(task));
        };
    }
}

SpillExecutor::~SpillExecutor()
{
    for (auto & thread : threads)
        thread.join();
    LOG_INFO(logger, "stop spill executor");
}

void SpillExecutor::submit(TaskPtr && task)
{
    assert(task);
    submit_func(std::move(task));
}

void SpillExecutor::handleTask(TaskPtr && task)
{
    assert(task);
    TRACE_MEMORY(task);
    auto status = task->spill();
    switch (status)
    {
    case ExecTaskStatus::RUNNING:
        scheduler.task_executor.submit(std::move(task));
        break;
    case ExecTaskStatus::SPILLING:
        submit(std::move(task));
        break;
    case ExecTaskStatus::FINISHED:
    case ExecTaskStatus::ERROR:
    case ExecTaskStatus::CANCELLED:
        task.reset();
        break;
    default:
        __builtin_unreachable();
    }
}

void SpillExecutor::submitToThreadSharedQueue(TaskPtr && task)
{
    task_queue->submit(std::move(task));
}

void SpillExecutor::loop()
{
    setThreadName("SpillExecutor");
    LOG_INFO(logger, "start spill executor loop");
    TaskPtr task;
    while (likely(task_queue->take(task)))
    {
        handleTask(std::move(task));
        assert(!task);
    }
    LOG_INFO(logger, "spill executor loop finished");
}

void SpillExecutor::close()
{
    task_queue->close();
}
} // namespace DB
