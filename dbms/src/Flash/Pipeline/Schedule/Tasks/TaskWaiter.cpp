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

#include <Flash/Pipeline/Schedule/Tasks/PipeConditionVariable.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskWaiter.h>

#include <algorithm>

namespace DB
{
void TaskWaiter::setTask(TaskPtr && task_)
{
    assert(task_);
    const auto type = task_->getNotifyType();
    std::lock_guard lock(mutex);
    assert(!task);
    thread_local auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_wait_for_notify_tasks_count);
    metrics.Increment();
    PipeConditionVariable::updateTaskNotifyWaitMetrics(type, 1);
    task = std::move(task_);
}

bool TaskWaiter::notify()
{
    TaskPtr ready_task;
    {
        std::lock_guard lock(mutex);
        if (!task)
            return false;
        ready_task = std::move(task);
    }
    thread_local auto & metrics = GET_METRIC(tiflash_pipeline_scheduler, type_wait_for_notify_tasks_count);
    metrics.Decrement();
    PipeConditionVariable::updateTaskNotifyWaitMetrics(ready_task->getNotifyType(), -1);
    PipeConditionVariable::notifyTaskDirectly(std::move(ready_task));
    return true;
}

void TaskStopSource::stop()
{
    if (stopped.exchange(true, std::memory_order_acq_rel))
        return;
    std::vector<std::weak_ptr<TaskWaiter>> ready_waiters;
    {
        std::lock_guard lock(mutex);
        ready_waiters.swap(waiters);
    }
    for (const auto & weak_waiter : ready_waiters)
        if (auto waiter = weak_waiter.lock())
            waiter->notify();
}

void TaskStopSource::registerWaiter(const TaskWaiterPtr & waiter)
{
    {
        std::lock_guard lock(mutex);
        if (!isStopped())
        {
            // Amortize pruning across registrations. A cohort of N live waiters takes O(N) work,
            // while repeated waits keep at most roughly twice the last observed live cohort plus 64 entries.
            if (waiters.size() >= prune_at)
            {
                waiters.erase(
                    std::remove_if(waiters.begin(), waiters.end(), [](const auto & old) { return old.expired(); }),
                    waiters.end());
                prune_at = std::max<size_t>(64, waiters.size() * 2);
            }
            waiters.emplace_back(waiter);
            return;
        }
    }
    waiter->notify();
}
} // namespace DB
