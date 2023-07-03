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

#include <Flash/Pipeline/Schedule/TaskQueues/MultiLevelFeedbackQueue.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>
#include <assert.h>
#include <common/likely.h>

namespace DB
{
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
    for (const auto & unit_queue : level_queues)
        RUNTIME_ASSERT(unit_queue->empty(), logger, "all task should be taken before it is destructed");
}

template <typename TimeGetter>
MultiLevelFeedbackQueue<TimeGetter>::MultiLevelFeedbackQueue()
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
        level_queues[task->mlfq_level]->submit(std::move(task));
    }
    assert(!task);
    cv.notify_one();
}

template <typename TimeGetter>
void MultiLevelFeedbackQueue<TimeGetter>::submit(std::vector<TaskPtr> & tasks)
{
    if unlikely (is_finished)
    {
        FINALIZE_TASKS(tasks);
        return;
    }

    if (tasks.empty())
        return;

    for (auto & task : tasks)
        computeQueueLevel(task);

    std::lock_guard lock(mu);
    for (auto & task : tasks)
    {
        level_queues[task->mlfq_level]->submit(std::move(task));
        cv.notify_one();
    }
}

template <typename TimeGetter>
bool MultiLevelFeedbackQueue<TimeGetter>::take(TaskPtr & task)
{
    assert(!task);
    {
        // -1 means no candidates; else has candidate.
        int queue_idx = -1;
        double target_accu_time_microsecond = 0;
        std::unique_lock lock(mu);
        while (true)
        {
            // Find the queue with the smallest execution time.
            for (size_t i = 0; i < QUEUE_SIZE; ++i)
            {
                // we just search for queue has element
                const auto & cur_queue = level_queues[i];
                if (!cur_queue->empty())
                {
                    double local_target_time_microsecond = cur_queue->normalizedTimeMicrosecond();
                    if (queue_idx < 0 || local_target_time_microsecond < target_accu_time_microsecond)
                    {
                        target_accu_time_microsecond = local_target_time_microsecond;
                        queue_idx = i;
                    }
                }
            }

            if (queue_idx >= 0)
                break;
            if (unlikely(is_finished))
                return false;
            cv.wait(lock);
        }
        level_queues[queue_idx]->take(task);
    }

    assert(task);
    return true;
}

template <typename TimeGetter>
void MultiLevelFeedbackQueue<TimeGetter>::updateStatistics(const TaskPtr & task, ExecTaskStatus, UInt64 inc_ns)
{
    assert(task);
    level_queues[task->mlfq_level]->accu_consume_time_microsecond += (inc_ns / 1000);
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
    return true;
}

template <typename TimeGetter>
void MultiLevelFeedbackQueue<TimeGetter>::finish()
{
    {
        std::lock_guard lock(mu);
        is_finished = true;
    }
    cv.notify_all();
}

template <typename TimeGetter>
const UnitQueueInfo & MultiLevelFeedbackQueue<TimeGetter>::getUnitQueueInfo(size_t level)
{
    assert(level < QUEUE_SIZE);
    return level_queues[level]->info;
}

template class MultiLevelFeedbackQueue<CPUTimeGetter>;
template class MultiLevelFeedbackQueue<IOTimeGetter>;

} // namespace DB
