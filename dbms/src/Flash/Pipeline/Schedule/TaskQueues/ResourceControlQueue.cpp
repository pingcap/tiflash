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

#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Executor/toRU.h>
#include <Flash/Pipeline/Schedule/TaskQueues/IOPriorityQueue.h>
#include <Flash/Pipeline/Schedule/TaskQueues/MultiLevelFeedbackQueue.h>
#include <Flash/Pipeline/Schedule/TaskQueues/ResourceControlQueue.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>

namespace DB
{
template <typename NestedQueueType>
void ResourceControlQueue<NestedQueueType>::submit(TaskPtr && task)
{
    std::lock_guard lock(mu);
    submitWithoutLock(std::move(task));
}

template <typename NestedQueueType>
void ResourceControlQueue<NestedQueueType>::submit(std::vector<TaskPtr> & tasks)
{
    std::lock_guard lock(mu);
    for (auto & task : tasks)
    {
        submitWithoutLock(std::move(task));
    }
}

template <typename NestedQueueType>
void ResourceControlQueue<NestedQueueType>::submitWithoutLock(TaskPtr && task)
{
    if unlikely (is_finished)
    {
        FINALIZE_TASK(task);
        return;
    }
    const auto & query_id = task->getQueryId();
    if unlikely (cancel_query_id_cache.contains(query_id))
    {
        cancel_task_queue.push_back(std::move(task));
        return;
    }

    // name can be empty, it means resource control is disabled.
    const std::string & name = task->getResourceGroupName();

    auto iter = resource_group_task_queues.find(name);
    if (iter == resource_group_task_queues.end())
    {
        auto task_queue = std::make_shared<NestedQueueType>();

        task_queue->submit(std::move(task));
        resource_group_infos.push({name, LocalAdmissionController::global_instance->getPriority(name), task_queue});
        resource_group_task_queues.insert({name, task_queue});
    }
    else
    {
        iter->second->submit(std::move(task));
    }
    cv.notify_one();
}

template <typename NestedQueueType>
bool ResourceControlQueue<NestedQueueType>::take(TaskPtr & task)
{
    assert(task == nullptr);
    std::unique_lock lock(mu);
    while (true)
    {
        if unlikely (is_finished)
            return false;

        if (popTask(cancel_task_queue, task))
            return true;

        while (!resource_group_infos.empty())
        {
            const ResourceGroupInfo & group_info = resource_group_infos.top();
            const bool ru_exhausted = LocalAdmissionController::isRUExhausted(group_info.priority);

            LOG_TRACE(
                logger,
                "trying to schedule task of resource group {}, priority: {}, ru exhausted: {}, is_finished: {}, "
                "task_queue.empty(): {}",
                group_info.name,
                group_info.priority,
                ru_exhausted,
                is_finished,
                group_info.task_queue->empty());

            // When highest priority of resource group is less than zero, means RU of all resource groups are exhausted.
            // Should not take any task from nested task queue for this situation.
            if (ru_exhausted)
                break;

            if (group_info.task_queue->empty())
            {
                // Nested task queue is empty, continue and try next resource group.
                resource_group_infos.pop();
                size_t erase_num = resource_group_task_queues.erase(group_info.name);
                RUNTIME_CHECK_MSG(
                    erase_num == 1,
                    "cannot erase corresponding TaskQueue for task of resource group {}",
                    group_info.name);
            }
            else
            {
                // Take task from nested task queue, and should always take succeed.
                // Because this task queue should not be finished inside lock_guard.
                RUNTIME_CHECK(group_info.task_queue->take(task));
                assert(task != nullptr);
                break;
            }
        }

        if (task != nullptr)
            return true;

        // Wakeup when:
        // 1. finish() is called.
        // 2. refill_token_callback is called by LAC.
        // 3. resource group priority changed because of task consuming RU.(maybe useless?)
        cv.wait(lock);
        updateResourceGroupInfosWithoutLock();
    }
}

template <typename NestedQueueType>
void ResourceControlQueue<NestedQueueType>::updateStatistics(const TaskPtr & task, ExecTaskStatus, size_t inc_value)
{
    assert(task);
    const std::string & name = task->getResourceGroupName();

    std::lock_guard lock(mu);
    auto iter = resource_group_statistic.find(name);
    if (iter == resource_group_statistic.end())
    {
        UInt64 accumulated_cpu_time = inc_value;
        if (accumulated_cpu_time >= YIELD_MAX_TIME_SPENT_NS)
        {
            updateResourceGroupStatisticWithoutLock(name, accumulated_cpu_time);
            accumulated_cpu_time = 0;
        }
        resource_group_statistic.insert({name, accumulated_cpu_time});
    }
    else
    {
        iter->second += inc_value;
        if (iter->second >= YIELD_MAX_TIME_SPENT_NS)
        {
            updateResourceGroupStatisticWithoutLock(name, iter->second);
            iter->second = 0;
        }
    }
}

template <typename NestedQueueType>
void ResourceControlQueue<NestedQueueType>::updateResourceGroupStatisticWithoutLock(
    const std::string & name,
    UInt64 consumed_cpu_time)
{
    auto ru = toRU(consumed_cpu_time);
    LOG_TRACE(logger, "resource group {} will consume {} RU(or {} cpu time in ns)", name, ru, consumed_cpu_time);
    LocalAdmissionController::global_instance->consumeResource(name, ru, consumed_cpu_time);
    updateResourceGroupInfosWithoutLock();

    // Notify priority info is updated.
    cv.notify_one();
}

template <typename NestedQueueType>
void ResourceControlQueue<NestedQueueType>::updateResourceGroupInfosWithoutLock()
{
    std::priority_queue<ResourceGroupInfo> new_resource_group_infos;
    while (!resource_group_infos.empty())
    {
        const ResourceGroupInfo & group_info = resource_group_infos.top();

        auto new_priority = LocalAdmissionController::global_instance->getPriority(group_info.name);
        new_resource_group_infos.push({group_info.name, new_priority, group_info.task_queue});
        resource_group_infos.pop();
    }
    resource_group_infos = new_resource_group_infos;
}

template <typename NestedQueueType>
bool ResourceControlQueue<NestedQueueType>::empty() const
{
    std::lock_guard lock(mu);

    if (!cancel_task_queue.empty())
        return false;

    if (resource_group_task_queues.empty())
        return true;

    for (const auto & task_queue_iter : resource_group_task_queues)
    {
        if (!task_queue_iter.second->empty())
            return false;
    }
    return true;
}

template <typename NestedQueueType>
void ResourceControlQueue<NestedQueueType>::finish()
{
    std::lock_guard lock(mu);
    is_finished = true;
    for (auto & ele : resource_group_task_queues)
        ele.second->finish();

    cv.notify_all();
}

template <typename NestedQueueType>
void ResourceControlQueue<NestedQueueType>::cancel(const String & query_id, const String & resource_group_name)
{
    if unlikely (query_id.empty())
        return;

    std::lock_guard lock(mu);
    if (cancel_query_id_cache.add(query_id))
    {
        auto iter = resource_group_task_queues.find(resource_group_name);
        if (iter != resource_group_task_queues.end())
        {
            iter->second->collectCancelledTasks(cancel_task_queue, query_id);
        }
    }
}

template class ResourceControlQueue<CPUMultiLevelFeedbackQueue>;
// For now, io_task_thread_pool is not managed by ResourceControl mechanism.
template class ResourceControlQueue<IOPriorityQueue>;
} // namespace DB
