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

#include <Flash/Pipeline/Schedule/TaskQueues/MultiLevelFeedbackQueue.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>
#include <assert.h>
#include <common/likely.h>

#include <ext/scope_guard.h>

namespace DB
{
namespace
{
void moveCancelledTasks(UnitQueue & unit_queue, std::deque<TaskPtr> & cancel_queue, const String & query_id)
{
    assert(!query_id.empty());
    auto & normal_queue = unit_queue.task_queue;
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

void UnitQueue::take(TaskPtr & task)
{
    assert(!task);
    assert(!empty());
    task = std::move(task_queue.front());
    task_queue.pop_front();
    assert(task);
}

bool UnitQueue::empty() const
{
    return task_queue.empty();
}

void UnitQueue::submit(TaskPtr && task)
{
    assert(task);
    task_queue.push_back(std::move(task));
}

double UnitQueue::normalizedTimeMicrosecond()
{
    return accu_consume_time_microsecond / info.factor_for_normal;
}

template <typename TimeGetter>
MultiLevelFeedbackQueue<TimeGetter>::~MultiLevelFeedbackQueue()
{
    drainTaskQueueWithoutLock();
}

template <typename TimeGetter>
MultiLevelFeedbackQueue<TimeGetter>::MultiLevelFeedbackQueue(KeyspaceCpuLimiterPtr keyspace_cpu_limiter_)
    : keyspace_cpu_limiter(std::move(keyspace_cpu_limiter_))
{
    UInt64 time_slices[QUEUE_SIZE];
    UInt64 time_slice = 0;
    for (size_t i = 0; i < QUEUE_SIZE; ++i)
    {
        time_slice += LEVEL_TIME_SLICE_BASE_NS * (i + 1);
        time_slices[i] = time_slice;
    }

    static constexpr double RATIO_OF_ADJACENT_QUEUE = 1.2;
    double factors[QUEUE_SIZE];
    double factor = 1;
    for (int i = QUEUE_SIZE - 1; i >= 0; --i)
    {
        // Initialize factor for every unit queue.
        // Higher priority queues have more execution time,
        // so they should have a larger factor.
        factors[i] = factor;
        factor *= RATIO_OF_ADJACENT_QUEUE;
    }

    for (size_t i = 0; i < QUEUE_SIZE; ++i)
        level_queues[i] = std::make_unique<UnitQueue>(time_slices[i], factors[i]);
}

template <typename TimeGetter>
void MultiLevelFeedbackQueue<TimeGetter>::computeQueueLevel(const TaskPtr & task)
{
    auto time_spent = TimeGetter::get(task);
    // level will only increment.
    for (size_t i = task->mlfq_level; i < QUEUE_SIZE; ++i)
    {
        if (time_spent < getUnitQueueInfo(i).time_slice)
        {
            task->mlfq_level = i;
            return;
        }
    }
    task->mlfq_level = QUEUE_SIZE - 1;
}

template <typename TimeGetter>
void MultiLevelFeedbackQueue<TimeGetter>::submitTaskWithoutLock(TaskPtr && task)
{
    if unlikely (cancel_query_id_cache.contains(task->getQueryId()))
        cancel_task_queue.push_back(std::move(task));
    else
        level_queues[task->mlfq_level]->submit(std::move(task));
}

template <typename TimeGetter>
void MultiLevelFeedbackQueue<TimeGetter>::submit(TaskPtr && task)
{
    if unlikely (is_finished)
    {
        FINALIZE_TASK(task);
        return;
    }

    computeQueueLevel(task);
    {
        std::lock_guard lock(mu);
        submitTaskWithoutLock(std::move(task));
    }
    assert(!task);
    notifyWaiters();
}

template <typename TimeGetter>
void MultiLevelFeedbackQueue<TimeGetter>::submit(std::vector<TaskPtr> & tasks)
{
    if (tasks.empty())
        return;

    if unlikely (is_finished)
    {
        FINALIZE_TASKS(tasks);
        return;
    }

    for (auto & task : tasks)
        computeQueueLevel(task);

    {
        std::lock_guard lock(mu);
        for (auto & task : tasks)
            submitTaskWithoutLock(std::move(task));
    }
    notifyWaiters();
}

template <typename TimeGetter>
bool MultiLevelFeedbackQueue<TimeGetter>::take(TaskPtr & task)
{
    assert(!task);
    std::unique_lock lock(mu);
    const bool limiter_enabled = keyspace_cpu_limiter && keyspace_cpu_limiter->isEnabled();
    while (true)
    {
        // Remaining tasks will be drained in destructor.
        if (unlikely(is_finished))
            return false;

        if (popTask(cancel_task_queue, task))
            return true;

        // A level can contain tasks from several keyspaces. Try levels in
        // MLFQ order until one has a task with an available reservation.
        std::array<bool, QUEUE_SIZE> inspected{};
        for (size_t count = 0; count < QUEUE_SIZE; ++count)
        {
            int queue_idx = -1;
            double target_accu_time_microsecond = 0;
            for (size_t i = 0; i < QUEUE_SIZE; ++i)
            {
                const auto & cur_queue = level_queues[i];
                if (!inspected[i] && !cur_queue->empty())
                {
                    double local_target_time_microsecond = cur_queue->normalizedTimeMicrosecond();
                    if (queue_idx < 0 || local_target_time_microsecond < target_accu_time_microsecond)
                    {
                        target_accu_time_microsecond = local_target_time_microsecond;
                        queue_idx = static_cast<int>(i);
                    }
                }
            }

            if (queue_idx < 0)
                break;

            inspected[queue_idx] = true;
            if (tryTakeTaskWithoutLock(*level_queues[queue_idx], task))
                return true;
        }

        if (limiter_enabled)
        {
            const auto previous_change_id = keyspace_cpu_limiter->getChangeId();
            lock.unlock();
            keyspace_cpu_limiter->waitForChange(previous_change_id, keyspace_cpu_limiter->getRefillWaitDuration());
            lock.lock();
        }
        else
        {
            cv.wait(lock);
        }
    }
}

template <typename TimeGetter>
bool MultiLevelFeedbackQueue<TimeGetter>::tryTakeTaskWithoutLock(UnitQueue & unit_queue, TaskPtr & task)
{
    if (!keyspace_cpu_limiter || !keyspace_cpu_limiter->isEnabled())
    {
        unit_queue.take(task);
        return true;
    }

    for (auto it = unit_queue.task_queue.begin(); it != unit_queue.task_queue.end(); ++it)
    {
        const auto keyspace_id = (*it)->getKeyspaceID();
        if (!keyspace_cpu_limiter->tryAcquire(keyspace_id))
            continue;

        bool owner_bound = false;
        SCOPE_EXIT({
            if (!owner_bound)
                keyspace_cpu_limiter->release(keyspace_id);
        });
        task = std::move(*it);
        unit_queue.task_queue.erase(it);
        keyspace_cpu_limiter->bindOwner(keyspace_id, task.get());
        owner_bound = true;
        return true;
    }
    return false;
}

template <typename TimeGetter>
void MultiLevelFeedbackQueue<TimeGetter>::drainTaskQueueWithoutLock()
{
    TaskPtr task;
    while (popTask(cancel_task_queue, task))
    {
        if (keyspace_cpu_limiter)
            keyspace_cpu_limiter->release(task.get());
        FINALIZE_TASK(task);
    }

    for (size_t i = 0; i < QUEUE_SIZE; ++i)
    {
        auto & cur_queue = level_queues[i];
        while (!cur_queue->empty())
        {
            cur_queue->take(task);
            if (keyspace_cpu_limiter)
                keyspace_cpu_limiter->release(task.get());
            FINALIZE_TASK(task);
        }
    }
}

template <typename TimeGetter>
void MultiLevelFeedbackQueue<TimeGetter>::updateStatistics(const TaskPtr & task, ExecTaskStatus, UInt64 inc_ns)
{
    assert(task);
    level_queues[task->mlfq_level]->accu_consume_time_microsecond += (inc_ns / 1000);
    if (keyspace_cpu_limiter)
    {
        keyspace_cpu_limiter->release(task.get());
        notifyWaiters();
    }
}

template <typename TimeGetter>
bool MultiLevelFeedbackQueue<TimeGetter>::empty() const
{
    std::lock_guard lock(mu);
    for (const auto & queue : level_queues)
    {
        if (!queue->empty())
            return false;
    }
    return cancel_task_queue.empty();
}

template <typename TimeGetter>
void MultiLevelFeedbackQueue<TimeGetter>::finish()
{
    {
        std::lock_guard lock(mu);
        is_finished = true;
    }
    notifyWaiters();
}

template <typename TimeGetter>
const UnitQueueInfo & MultiLevelFeedbackQueue<TimeGetter>::getUnitQueueInfo(size_t level)
{
    assert(level < QUEUE_SIZE);
    return level_queues[level]->info;
}

template <typename TimeGetter>
void MultiLevelFeedbackQueue<TimeGetter>::cancel(const TaskCancelInfo & cancel_info)
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

template <typename TimeGetter>
void MultiLevelFeedbackQueue<TimeGetter>::collectCancelledTasks(
    std::deque<TaskPtr> & cancel_queue,
    const String & query_id)
{
    for (const auto & queue : level_queues)
        moveCancelledTasks(*queue, cancel_queue, query_id);
}

template <typename TimeGetter>
void MultiLevelFeedbackQueue<TimeGetter>::notifyWaiters()
{
    cv.notify_all();
    if (keyspace_cpu_limiter)
        keyspace_cpu_limiter->notifyAll();
}

template class MultiLevelFeedbackQueue<CPUTimeGetter>;
template class MultiLevelFeedbackQueue<IOTimeGetter>;

} // namespace DB
