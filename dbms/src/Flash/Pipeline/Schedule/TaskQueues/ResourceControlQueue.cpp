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

    // name can be empty, it means resource control is disabled.
    const std::string & name = task->getResourceGroupName();

    auto iter = resource_group_task_queues.find(name);
    if (iter == resource_group_task_queues.end())
    {
        auto task_queue = std::make_shared<NestedQueueType>();
        task_queue->submit(std::move(task));
        resource_group_infos.push({LocalAdmissionController::global_instance->getPriority(name), task_queue, name});
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
        while (!resource_group_infos.empty())
        {
            if unlikely (is_finished)
            {
                // resource_group_infos and resource_group_task_queues will be cleaned in destructor,
                // and all Tasks in nested task queue will be drained in destructor of TaskQueue.
                return false;
            }

            if (tryTakeCancelTaskWithoutLock(task))
                break;

            ResourceGroupInfo group_info = resource_group_infos.top();
            const std::string & name = std::get<InfoIndexResourceGroupName>(group_info);
            const auto & priority = LocalAdmissionController::global_instance->getPriority(name);
            std::shared_ptr<NestedQueueType> & task_queue = std::get<InfoIndexPipelineTaskQueue>(group_info);

            LOG_TRACE(logger, "trying to schedule task of resource group {}, priority: {}, is_finished: {}, task_queue.empty(): {}", name, priority, is_finished, task_queue->empty());

            // When highest priority of resource group is less than zero, means RU of all resource groups are exhausted.
            // Should not take any task from nested task queue for this situation.
            if (priority <= 0)
                break;

            if (task_queue->empty() || !task_queue->take(task))
            {
                LOG_TRACE(logger, "take task from nested task_queue of resource group {} failed. task_queue.empty(): {}", name, task_queue->empty());
                // Got here only when task_queue is empty or finished, we try next resource group.
                // If new task of this resource gorup is submited, the resource_group info will be added again.
                resource_group_infos.pop();
                size_t erase_num = resource_group_task_queues.erase(name);
                RUNTIME_CHECK_MSG(erase_num == 1, "cannot erase corresponding TaskQueue for task of resource group {}", name);
            }
            else
            {
                LOG_TRACE(logger, "schedule task of resource group {} succeed, cur cpu time of MPPTask: {}", name, task->getQueryExecContext().getQueryProfileInfo().getCPUExecuteTimeNs());
                assert(task != nullptr);
                break;
            }
        }

        if (task != nullptr)
            return true;

        // Check is_finished again for situation when resource_group_infos never insert any resource group.
        if unlikely (is_finished)
            return false;

        // Other TaskQueue like MultiLevelFeedbackQueue and IOPriorityQueue will wake up when new task submit or is_finished become true.
        // But for ResourceControlQueue, when all resource groups's RU are exhausted, will go to sleep, and should wakeup when RU is updated.
        // But LAC has no way to notify ResourceControlQueue for now, so ResourceControlQueue should wakeup to check if RU is updated or not.
        cv.wait_for(lock, DEFAULT_WAIT_INTERVAL_WHEN_RUN_OUT_OF_RU);
        updateResourceGroupInfosWithoutLock();
    }
}

template <typename NestedQueueType>
bool ResourceControlQueue<NestedQueueType>::tryTakeCancelTaskWithoutLock(TaskPtr & task)
{
    if (cancel_query_ids.empty())
        return false;

    for (auto iter = cancel_query_ids.begin(); iter != cancel_query_ids.end(); ++iter)
    {
        if (!iter->second->isCancelQueueEmpty())
        {
            std::shared_ptr<NestedQueueType> & task_queue = iter->second;
            assert(!task_queue->empty());
            task_queue->take(task);
            break;
        }
        // todo: remove item from cancel_query_ids when this query is cancelled successfully,
        // otherwise we may need to iterate all nested task queue to check if cancel task is empty or not.
    }
    return task != nullptr;
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
void ResourceControlQueue<NestedQueueType>::updateResourceGroupStatisticWithoutLock(const std::string & name, UInt64 consumed_cpu_time)
{
    auto ru = toRU(consumed_cpu_time);
    LOG_DEBUG(logger, "resource group {} will consume {} RU(or {} cpu time in ns)", name, ru, consumed_cpu_time);
    LocalAdmissionController::global_instance->consumeResource(name, ru, consumed_cpu_time);
    updateResourceGroupInfosWithoutLock();

    // Notify priority info is updated.
    cv.notify_one();
}

template <typename NestedQueueType>
void ResourceControlQueue<NestedQueueType>::updateResourceGroupInfosWithoutLock()
{
    ResourceGroupInfoQueue new_resource_group_infos{compator};
    while (!resource_group_infos.empty())
    {
        ResourceGroupInfo group_info = resource_group_infos.top();
        resource_group_infos.pop();

        const auto & name = std::get<InfoIndexResourceGroupName>(group_info);
        auto new_priority = LocalAdmissionController::global_instance->getPriority(name);
        new_resource_group_infos.push(std::make_tuple(new_priority, std::get<InfoIndexPipelineTaskQueue>(group_info), name));
    }
    resource_group_infos = new_resource_group_infos;
}

template <typename NestedQueueType>
bool ResourceControlQueue<NestedQueueType>::empty() const
{
    std::lock_guard lock(mu);

    if (resource_group_task_queues.empty())
        return true;

    bool empty = true;
    for (const auto & task_queue_iter : resource_group_task_queues)
    {
        if (!task_queue_iter.second->empty())
            empty = false;
    }
    return empty;
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
    std::lock_guard lock(mu);
    auto iter = cancel_query_ids.find(query_id);
    if (iter != cancel_query_ids.end())
        return;

    if (resource_group_task_queues.find(resource_group_name) == resource_group_task_queues.end())
    {
        std::shared_ptr<NestedQueueType> & task_queue = iter->second;
        task_queue->cancel(query_id, "");
        cancel_query_ids[query_id] = task_queue;
    }
}

template class ResourceControlQueue<CPUMultiLevelFeedbackQueue>;
// For now, io_task_thread_pool is not managed by ResourceControl mechanism.
template class ResourceControlQueue<IOPriorityQueue>;
} // namespace DB
