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
#include <Flash/Pipeline/Schedule/TaskQueues/ResourceControlQueue.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskHelper.h>

namespace DB
{
void ResourceControlQueue::doFinish()
{
    for (auto & ele : pipeline_tasks)
    {
        ele.second->finish();
    }
}

void ResourceControlQueue::submit(TaskPtr && task)
{
    if unlikely (is_finished)
    {
        doFinish();
    }
    else
    {
        const std::string & name = task->getResourceGroupName();
        std::lock_guard lock(mu);

        auto iter = pipeline_tasks.find(name);
        if (iter == pipeline_tasks.end())
        {
            ResourceGroupPtr group = LocalAdmissionController::global_instance->getOrCreateResourceGroup(name);
            auto task_queue = std::make_shared<CPUMultiLevelFeedbackQueue>();
            task_queue->submit(std::move(task));
            resource_group_infos.push({group->getPriority(), task_queue, name});
            pipeline_tasks.insert({name, task_queue});
        }
        else
        {
            iter->second->submit(std::move(task));
        }
    }
}

void ResourceControlQueue::submit(std::vector<TaskPtr> & tasks)
{
    for (auto & task : tasks)
    {
        if unlikely (is_finished)
        {
            doFinish();
            break;
        }
        // Each task may belong to different resource group,
        // so there is no better way to submit batch tasks at once.
        submit(std::move(task));
    }
}

bool ResourceControlQueue::take(TaskPtr & task)
{
    // gjt todo: io?
    std::string name;
    std::shared_ptr<CPUMultiLevelFeedbackQueue> task_queue;
    {
        std::unique_lock lock(mu);

        // Wakeup when:
        // 1. resource_groups not empty and
        // 2. top priority of resource group is greater than zero(a.k.a. RU > 0)
        cv.wait(lock, [this, &task_queue] {
            if unlikely (is_finished)
                return true;

            if (resource_group_infos.empty())
                return false;

            ResourceGroupInfo group_info = resource_group_infos.top();
            ResourceGroupPtr resource_group = LocalAdmissionController::global_instance->getOrCreateResourceGroup(std::get<2>(group_info));
            task_queue = std::get<1>(group_info);
            return resource_group->getPriority() >= 0.0 && !task_queue->empty();
        });
    }

    if unlikely (is_finished)
    {
        doFinish();
        return false;
    }

    return task_queue->take(task);
}

void ResourceControlQueue::updateStatistics(const TaskPtr & task, size_t inc_value)
{
    assert(task);
    std::string name = task->getResourceGroupName();

    auto iter = resource_group_statics.find(name);
    if (iter == resource_group_statics.end())
    {
        UInt64 accumulated_cpu_time = inc_value;
        if unlikely (pipelineTaskTimeExceedThreshold(accumulated_cpu_time))
        {
            updateResourceGroupResource(name, accumulated_cpu_time);
            accumulated_cpu_time = 0;
        }
        resource_group_statics.insert({name, accumulated_cpu_time});
    }
    else
    {
        iter->second += inc_value;
        if (pipelineTaskTimeExceedThreshold(iter->second))
        {
            updateResourceGroupResource(name, iter->second);
            iter->second = 0;
        }
    }
}

void ResourceControlQueue::updateResourceGroupResource(const std::string & name, UInt64 consumed_cpu_time)
{
    LocalAdmissionController::global_instance->getOrCreateResourceGroup(name)->consumeResource(toRU(consumed_cpu_time), consumed_cpu_time);
    {
        std::lock_guard lock(mu);
        updateResourceGroupInfos();
    }
}

void ResourceControlQueue::updateResourceGroupInfos()
{
    ResourceGroupInfoQueue new_resource_group_infos;
    while (!resource_group_infos.empty())
    {
        const auto & group_info = resource_group_infos.top();
        auto new_priority = LocalAdmissionController::global_instance->getOrCreateResourceGroup(std::get<2>(group_info))->getPriority();
        new_resource_group_infos.push(std::make_tuple(new_priority, std::get<1>(group_info), std::get<2>(group_info)));
    }
    resource_group_infos = new_resource_group_infos;
}

bool ResourceControlQueue::empty() const
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

void ResourceControlQueue::finish()
{
    {
        std::lock_guard lock(mu);
        is_finished = true;
    }
    cv.notify_all();
}

} // namespace DB
