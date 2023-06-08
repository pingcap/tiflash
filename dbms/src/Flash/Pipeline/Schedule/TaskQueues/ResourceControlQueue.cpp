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

#include <Flash/Pipeline/Schedule/TaskQueues/ResourceControlQueue.h>

namespace DB
{
void ResourceControlQueue::submit(TaskPtr && task)
{
    ResourceGroupPipelineTasks::iterator iter;
    const std::string & name = task->getResourceGroupName();
    {
        std::lock_guard lock(mu);
        iter = pipeline_tasks.find(name);
    }

    ResourceGroupPtr resource_group;
    if (iter == pipeline_tasks.end())
    {
        resource_group = LocalAdmissionController::global_instance->getOrCreateResourceGroup(name);
        {
            std::lock_guard lock(mu);
            std::vector<TaskPtr> task_vec;
            task_vec.emplace_back(std::move(task));
            pipeline_tasks.insert({resource_group->getName(), std::move(task_vec)});
        }
    }
    else
    {
        iter->second.emplace_back(std::move(task));
    }
}

void ResourceControlQueue::submit(std::vector<TaskPtr> & tasks)
{
    for (auto & task : tasks)
    {
        // Each task may belong to different resource group,
        // so there is no better way to submit batch tasks at once.
        submit(std::move(task));
    }
}

void ResourceControlQueue::updateResourceGroupQueue()
{

}

bool ResourceControlQueue::take(TaskPtr & task)
{
    // gjt todo: io?
    std::string name;
    std::shared_ptr<CPUMultiLevelFeedbackQueue> task_queue;
    {
        std::unique_lock lock(mu);

        // Wakeup when: resource_groups not empty and top priority of resource group is greater than zero(a.k.a. RU > 0)
        cv.wait(lock, [this] {
            if (resource_groups.empty())
              return false;
            ResourceGroupPtr resource_group = LocalAdmissionController::global_instance->getOrCreateResourceGroup(std::get<2>(resource_groups.top()));
            return resource_group->getPriority() >= 0.0;
        });

        task_queue = std::get<1>(resource_groups.top());
        name = std::get<2>(resource_groups.top());
    }

    task_queue->take(task);

    // Remove resource group when pipeline task is empty.
    if (task_queue->empty())
    {
        std::lock_guard lock(mu);
        ResourceGroupQueue new_resource_groups;
        while (!resource_groups.empty())
        {
            const auto & ele = resource_groups.top();
            resource_groups.pop();
            if (std::get<2>(ele) != name)
                new_resource_groups.push(ele);
        }
        resource_groups = new_resource_groups;
    }
}

void ResourceControlQueue::updateStatistics(const TaskPtr & task, size_t inc_value)
{
    assert(task);
    auto & resource_group = getOrCreateResourceGroup(task.getResourceGroupName());
    // gjt todo
    resource_group.consumeRU(cpu_ru);
}

bool ResourceControlQueue::empty() const
{
    std::lock_guard lock(mu);

    if (resource_groups.empty())
        return true;

    bool empty = true;
    for (const auto & resource_group : resource_groups)
    {
        if (!resource_group.pipelineTaskEmpty())
        {
            empty = false;
            break;
        }
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
