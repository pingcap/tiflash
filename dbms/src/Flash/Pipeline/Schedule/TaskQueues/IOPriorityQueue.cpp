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

#include <Flash/Pipeline/Schedule/TaskQueues/IOPriorityQueue.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>
#include <common/likely.h>

#include <ext/scope_guard.h>

namespace DB
{
namespace
{
void moveCancelledTasks(std::list<TaskPtr> & normal_queue, std::deque<TaskPtr> & cancel_queue, const String & query_id)
{
    assert(!query_id.empty());
    for (auto it = normal_queue.begin(); it != normal_queue.end();)
    {
        if (query_id == (*it)->getQueryId())
        {
            cancel_queue.push_back(std::move(*it));
            it = normal_queue.erase(it);
        }
        else
        {
            ++it;
        }
    }
}
} // namespace

IOPriorityQueue::~IOPriorityQueue()
{
    drainTaskQueueWithoutLock();
}

bool IOPriorityQueue::take(TaskPtr & task)
{
    std::unique_lock lock(mu);
    const bool limiter_enabled = keyspace_cpu_limiter && keyspace_cpu_limiter->isEnabled();
    while (true)
    {
        // Remaining tasks will be drained in destructor.
        if (unlikely(is_finished))
            return false;

        // Snapshot before inspecting the queues. A release can happen without
        // holding `mu`; taking the snapshot afterwards could miss that wakeup.
        const auto previous_change_id = limiter_enabled ? keyspace_cpu_limiter->getChangeId() : 0;

        if (popTask(cancel_task_queue, task))
            return true;

        bool io_out_first = ratio_of_out_to_in * total_io_in_time_microsecond >= total_io_out_time_microsecond;
        auto & first_queue = io_out_first ? io_out_task_queue : io_in_task_queue;
        auto & next_queue = io_out_first ? io_in_task_queue : io_out_task_queue;
        bool cpu_quota_rejected = false;
        if (tryTakeTaskWithoutLock(first_queue, task, cpu_quota_rejected))
            return true;
        if (tryTakeTaskWithoutLock(next_queue, task, cpu_quota_rejected))
            return true;
        if (limiter_enabled)
        {
            const bool has_pending_tasks
                = !io_out_task_queue.empty() || !io_in_task_queue.empty() || !cancel_task_queue.empty();
            if (has_pending_tasks)
            {
                lock.unlock();
                // Wait for the earliest quota refill, or for a release or a
                // submission to update the generation. Without CPU quota the
                // refill wait is unbounded, which keeps the plain wait.
                keyspace_cpu_limiter->waitForChange(
                    previous_change_id,
                    keyspace_cpu_limiter->getRefillWaitDuration(cpu_quota_rejected));
                lock.lock();
            }
            else
            {
                cv.wait(lock);
            }
        }
        else
        {
            cv.wait(lock);
        }
    }
}

bool IOPriorityQueue::tryTakeTaskWithoutLock(std::list<TaskPtr> & task_queue, TaskPtr & task, bool & cpu_quota_rejected)
{
    if (!keyspace_cpu_limiter || !keyspace_cpu_limiter->isEnabled())
        return popTask(task_queue, task);

    for (auto it = task_queue.begin(); it != task_queue.end(); ++it)
    {
        const auto keyspace_id = (*it)->getKeyspaceID();
        if (!keyspace_cpu_limiter->tryAcquire(keyspace_id, &cpu_quota_rejected))
            continue;

        bool owner_bound = false;
        SCOPE_EXIT({
            if (!owner_bound)
                keyspace_cpu_limiter->release(keyspace_id);
        });
        task = std::move(*it);
        task_queue.erase(it);
        keyspace_cpu_limiter->bindOwner(keyspace_id, task.get());
        owner_bound = true;
        return true;
    }
    return false;
}

void IOPriorityQueue::drainTaskQueueWithoutLock()
{
    TaskPtr task;
    while (popTask(cancel_task_queue, task))
    {
        if (keyspace_cpu_limiter)
            keyspace_cpu_limiter->release(task.get());
        FINALIZE_TASK(task);
    }
    while (popTask(io_out_task_queue, task))
    {
        if (keyspace_cpu_limiter)
            keyspace_cpu_limiter->release(task.get());
        FINALIZE_TASK(task);
    }
    while (popTask(io_in_task_queue, task))
    {
        if (keyspace_cpu_limiter)
            keyspace_cpu_limiter->release(task.get());
        FINALIZE_TASK(task);
    }
}

void IOPriorityQueue::updateStatistics(const TaskPtr & task, ExecTaskStatus exec_task_status, UInt64 inc_ns)
{
    switch (exec_task_status)
    {
    case ExecTaskStatus::IO_IN:
        total_io_in_time_microsecond += (inc_ns / 1000);
        break;
    case ExecTaskStatus::IO_OUT:
        total_io_out_time_microsecond += (inc_ns / 1000);
        break;
    default:; // ignore not io status.
    }

    if (keyspace_cpu_limiter && keyspace_cpu_limiter->isEnabled())
    {
        // A cancelled task can be returned from cancel_task_queue without ever
        // acquiring a reservation; release() is owner-aware and therefore a no-op.
        keyspace_cpu_limiter->release(task.get());
        notifyWaiters();
    }
}

bool IOPriorityQueue::empty() const
{
    std::lock_guard lock(mu);
    return cancel_task_queue.empty() && io_out_task_queue.empty() && io_in_task_queue.empty();
}

void IOPriorityQueue::finish()
{
    {
        std::lock_guard lock(mu);
        is_finished = true;
    }
    notifyWaiters();
}

void IOPriorityQueue::submitTaskWithoutLock(TaskPtr && task)
{
    if unlikely (cancel_query_id_cache.contains(task->getQueryId()))
    {
        cancel_task_queue.push_back(std::move(task));
        return;
    }

    auto status = task->getStatus();
    switch (status)
    {
    case ExecTaskStatus::IO_IN:
        io_in_task_queue.push_back(std::move(task));
        break;
    case ExecTaskStatus::IO_OUT:
        io_out_task_queue.push_back(std::move(task));
        break;
    default:
        throw Exception(fmt::format(
            "Unexpected status: {}, IOPriorityQueue only accepts tasks with IO status",
            magic_enum::enum_name(status)));
    }
}

void IOPriorityQueue::submit(TaskPtr && task)
{
    if unlikely (is_finished)
    {
        FINALIZE_TASK(task);
        return;
    }

    {
        std::lock_guard lock(mu);
        submitTaskWithoutLock(std::move(task));
    }
    notifyOneWaiter();
}

void IOPriorityQueue::submit(std::vector<TaskPtr> & tasks)
{
    if (tasks.empty())
        return;

    if unlikely (is_finished)
    {
        FINALIZE_TASKS(tasks);
        return;
    }

    {
        std::lock_guard lock(mu);
        for (auto & task : tasks)
            submitTaskWithoutLock(std::move(task));
    }
    if (tasks.size() == 1)
        notifyOneWaiter();
    else
        notifyWaiters();
}

void IOPriorityQueue::cancel(const TaskCancelInfo & cancel_info)
{
    if unlikely (cancel_info.query_id.empty())
        return;

    {
        std::lock_guard lock(mu);
        if (cancel_query_id_cache.add(cancel_info.query_id))
            collectCancelledTasks(cancel_task_queue, cancel_info.query_id);
    }
    notifyWaiters();
}

void IOPriorityQueue::collectCancelledTasks(std::deque<TaskPtr> & cancel_queue, const String & query_id)
{
    moveCancelledTasks(io_in_task_queue, cancel_queue, query_id);
    moveCancelledTasks(io_out_task_queue, cancel_queue, query_id);
}

void IOPriorityQueue::notifyOneWaiter()
{
    cv.notify_one();
    if (keyspace_cpu_limiter)
        keyspace_cpu_limiter->notifyAll();
}

void IOPriorityQueue::notifyWaiters()
{
    cv.notify_all();
    if (keyspace_cpu_limiter)
        keyspace_cpu_limiter->notifyAll();
}
} // namespace DB
