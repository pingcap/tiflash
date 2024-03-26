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

#include <Common/CPUAffinityManager.h>
#include <Common/Exception.h>
#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>
#include <Flash/Pipeline/Schedule/Reactor/WaitReactor.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>
#include <common/logger_useful.h>
#include <errno.h>
#include <sched.h>

namespace DB
{
WaitReactor::WaitReactor(TaskScheduler & scheduler_)
    : scheduler(scheduler_)
{
    GET_METRIC(tiflash_pipeline_scheduler, type_waiting_tasks_count).Set(0);
    thread = std::thread(&WaitReactor::loop, this);
}

bool WaitReactor::awaitAndCollectReadyTask(WaitingTask && task)
{
    assert(task.first);
    auto * task_ptr = task.second;
    auto return_status = task_ptr->await();
    switch (return_status.status)
    {
    case ExecTaskStatus::WAITING:
        return false;
    case ExecTaskStatus::RUNNING:
        task_ptr->profile_info.elapsedAwaitTime();
        cpu_tasks.push_back(std::move(task.first));
        return true;
    case ExecTaskStatus::IO_IN:
    case ExecTaskStatus::IO_OUT:
        task_ptr->profile_info.elapsedAwaitTime();
        io_tasks.push_back(std::move(task.first));
        return true;
    case ExecTaskStatus::WAIT_FOR_NOTIFY:
        assert(return_status.future);
        return_status.future->registerTask(std::move(task.first));
        return true;
    case FINISH_STATUS:
        task_ptr->profile_info.elapsedAwaitTime();
        task_ptr->startTraceMemory();
        task_ptr->finalize();
        task_ptr->endTraceMemory();
        task.first.reset();
        return true;
    default:
        UNEXPECTED_STATUS(logger, return_status.status);
    }
}

void WaitReactor::submitReadyTasks()
{
    if (cpu_tasks.empty() && io_tasks.empty())
    {
        tryYield();
        return;
    }

    scheduler.submitToCPUTaskThreadPool(cpu_tasks);
    cpu_tasks.clear();

    scheduler.submitToIOTaskThreadPool(io_tasks);
    io_tasks.clear();

    spin_count = 0;
}

void WaitReactor::tryYield()
{
    ++spin_count;

    // spin_count % 16 == 0
    if ((spin_count & 0xf) == 0)
    {
#if defined(__x86_64__)
        _mm_pause();
#elif defined __aarch64__
        // A "yield" instruction in aarch64 is essentially a nop, and does
        // not cause enough delay to help backoff. "isb" is a barrier that,
        // especially inside a loop, creates a small delay without consuming
        // ALU resources.  Experiments shown that adding the isb instruction
        // improves stability and reduces result jitter. Adding more delay
        // to the UT_RELAX_CPU than a single isb reduces performance.
        // clang-format off
        asm volatile("isb" ::: "memory");
        // clang-format on
#else
        // TODO: Maybe there's a better intrinsic like _mm_pause on non-x86_64 architecture.
        sched_yield();
#endif
        if (spin_count == 160)
        {
            spin_count = 0;
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(2ms);
        }
    }
}

void WaitReactor::finish()
{
    waiting_task_list.finish();
}

void WaitReactor::waitForStop()
{
    thread.join();
    LOG_INFO(logger, "wait reactor is stopped");
}

void WaitReactor::submit(TaskPtr && task)
{
    waiting_task_list.submit(std::move(task));
}

void WaitReactor::submit(std::list<TaskPtr> & tasks)
{
    waiting_task_list.submit(tasks);
}

bool WaitReactor::takeFromWaitingTaskList(WaitingTasks & local_waiting_tasks)
{
    std::list<TaskPtr> tmp_list;
    bool ret = local_waiting_tasks.empty()
        ? waiting_task_list.take(tmp_list)
        // If the local waiting tasks are not empty, there is no need to be blocked here
        // and we can continue to process the leftover tasks in the local waiting tasks
        : waiting_task_list.tryTake(tmp_list);
    if unlikely (!ret)
        return false;

    for (auto & task : tmp_list)
    {
        auto * task_ptr = task.get();
        local_waiting_tasks.emplace_back(std::move(task), std::move(task_ptr));
    }
    return true;
}

void WaitReactor::react(WaitingTasks & local_waiting_tasks)
{
    for (auto task_it = local_waiting_tasks.begin(); task_it != local_waiting_tasks.end();)
    {
        if (awaitAndCollectReadyTask(std::move(*task_it)))
            task_it = local_waiting_tasks.erase(task_it);
        else
            ++task_it;
    }

#ifdef __APPLE__
    auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_waiting_tasks_count);
#else
    thread_local auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_waiting_tasks_count);
#endif
    metrics.Set(local_waiting_tasks.size());

    submitReadyTasks();
}

void WaitReactor::loop()
{
    try
    {
        CPUAffinityManager::getInstance().bindSelfQueryThread();
        doLoop();
    }
    CATCH_AND_TERMINATE(logger)
}

void WaitReactor::doLoop()
{
    setThreadName("WaitReactor");
    LOG_INFO(logger, "start wait reactor loop");

    WaitingTasks local_waiting_tasks;
    while (likely(takeFromWaitingTaskList(local_waiting_tasks)))
        react(local_waiting_tasks);
    // Handle remaining tasks.
    while (!local_waiting_tasks.empty())
        react(local_waiting_tasks);

    LOG_INFO(logger, "wait reactor loop finished");
}
} // namespace DB
