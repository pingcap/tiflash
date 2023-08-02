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

#include <Flash/Executor/toRU.h>
#include <Flash/Pipeline/Schedule/TaskQueues/IOPriorityQueue.h>
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
        return;

    // name can be empty, it means resource control is disabled.
    const std::string & name = task->getResourceGroupName();
    const KeyspaceID & keyspace_id = task->getKeyspaceID();

    auto iter = pipeline_tasks.find(name);
    if (iter == pipeline_tasks.end())
    {
        auto task_queue = std::make_shared<NestedQueueType>();
        task_queue->submit(std::move(task));
        resource_group_infos.push({LocalAdmissionController::global_instance->getPriority(name, keyspace_id), task_queue, name, keyspace_id});
        pipeline_tasks.insert({name, task_queue});
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
    std::shared_ptr<NestedQueueType> task_queue;
    {
        std::unique_lock lock(mu);

        // Wakeup when:
        // 1. resource_groups not empty and
        // 2. highest priority of resource group is greater than zero(a.k.a. RU > 0)
        cv.wait(lock, [this, &task_queue] {
            if unlikely (is_finished)
                return true;

            if (resource_group_infos.empty())
                return false;

            ResourceGroupInfo group_info = resource_group_infos.top();
            const std::string & name = std::get<InfoIndexResourceGroupName>(group_info);
            const KeyspaceID & keyspace_id = std::get<InfoIndexResourceKeyspaceId>(group_info);
            task_queue = std::get<InfoIndexPipelineTaskQueue>(group_info);
            return LocalAdmissionController::global_instance->getPriority(name, keyspace_id) > 0.0 && !task_queue->empty();
        });

        if unlikely (is_finished)
            return false;
    }

    return task_queue->take(task);
}

template <typename NestedQueueType>
void ResourceControlQueue<NestedQueueType>::updateStatistics(const TaskPtr & task, ExecTaskStatus, size_t inc_value)
{
    assert(task);
    const std::string & name = task->getResourceGroupName();
    const KeyspaceID & keyspace_id = task->getKeyspaceID();

    std::lock_guard lock(mu);
    auto iter = resource_group_statistic.find(name);
    if (iter == resource_group_statistic.end())
    {
        UInt64 accumulated_cpu_time = inc_value;
        if (pipelineTaskTimeExceedYieldThreshold(accumulated_cpu_time))
        {
            updateResourceGroupStatistic(name, keyspace_id, accumulated_cpu_time);
            accumulated_cpu_time = 0;
        }
        resource_group_statistic.insert({name, accumulated_cpu_time});
    }
    else
    {
        iter->second += inc_value;
        if (pipelineTaskTimeExceedYieldThreshold(iter->second))
        {
            updateResourceGroupStatistic(name, keyspace_id, iter->second);
            iter->second = 0;
        }
    }
}

template <typename NestedQueueType>
void ResourceControlQueue<NestedQueueType>::updateResourceGroupStatistic(const std::string & name, const KeyspaceID & keyspace_id, UInt64 consumed_cpu_time)
{
    LocalAdmissionController::global_instance->consumeResource(name, keyspace_id, toRU(consumed_cpu_time), consumed_cpu_time);
    {
        std::lock_guard lock(mu);
        updateResourceGroupInfosWithoutLock();
    }
}

template <typename NestedQueueType>
void ResourceControlQueue<NestedQueueType>::updateResourceGroupInfosWithoutLock()
{
    ResourceGroupInfoQueue new_resource_group_infos{compator};
    while (!resource_group_infos.empty())
    {
        const auto & group_info = resource_group_infos.top();
        const auto & name = std::get<InfoIndexResourceGroupName>(group_info);
        const auto & keyspace_id = std::get<InfoIndexResourceKeyspaceId>(group_info);
        auto new_priority = LocalAdmissionController::global_instance->getPriority(name, keyspace_id);
        new_resource_group_infos.push(std::make_tuple(new_priority, std::get<InfoIndexPipelineTaskQueue>(group_info), name, keyspace_id));
    }
    resource_group_infos = new_resource_group_infos;
}

template <typename NestedQueueType>
bool ResourceControlQueue<NestedQueueType>::empty() const
{
    std::lock_guard lock(mu);

    if (pipeline_tasks.empty())
        return true;

    bool empty = true;
    for (const auto & task_queue_iter : pipeline_tasks)
    {
        if (!task_queue_iter.second->empty())
            empty = false;
    }
    return empty;
}

template <typename NestedQueueType>
void ResourceControlQueue<NestedQueueType>::finish()
{
    {
        std::lock_guard lock(mu);
        is_finished = true;
        for (auto & ele : pipeline_tasks)
            ele.second->finish();
    }
    cv.notify_all();
}

template <typename NestedQueueType>
void ResourceControlQueue<NestedQueueType>::cancel(const String & query_id)
{

}

template class ResourceControlQueue<CPUMultiLevelFeedbackQueue>;
template class ResourceControlQueue<IOPriorityQueue>;
} // namespace DB
