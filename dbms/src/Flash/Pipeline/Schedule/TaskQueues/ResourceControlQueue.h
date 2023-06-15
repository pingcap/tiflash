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

#pragma once

#include <Flash/Pipeline/Schedule/TaskQueues/TaskQueue.h>
#include <Flash/ResourceControl/LocalAdmissionController.h>

#include <mutex>
#include <unordered_map>

namespace DB
{
template <typename NestedQueueType>
class ResourceControlQueue : public TaskQueue
{
public:
    ResourceControlQueue() = default;
    ~ResourceControlQueue() override = default;

    void submit(TaskPtr && task) override;

    void submit(std::vector<TaskPtr> & tasks) override;

    bool take(TaskPtr & task) override;

    void updateStatistics(const TaskPtr & task, size_t inc_value) override;

    bool empty() const override;

    void finish() override;

private:
    // <resource_group_name, pipeline_tasks>
    using PipelineTasks = std::unordered_map<std::string, std::shared_ptr<NestedQueueType>>;

    // <priority, iterator_of_MLFQ_of_pipeline_tasks, resource_group_name, keyspace_id>
    using ResourceGroupInfo = std::tuple<double, std::shared_ptr<NestedQueueType>, std::string, KeyspaceID>;
    using CompatorType = bool (*)(const ResourceGroupInfo &, const ResourceGroupInfo &);
    using ResourceGroupInfoQueue = std::priority_queue<ResourceGroupInfo, std::vector<ResourceGroupInfo>, CompatorType>;

    // Index of ResourceGroupInfo.
    static constexpr auto InfoIndexPriority = 0;
    static constexpr auto InfoIndexPipelineTaskQueue = 1;
    static constexpr auto InfoIndexResourceName = 2;
    static constexpr auto InfoIndexResourceKeyspaceId = 3;

    // ResourceGroupInfoQueue compator.
    static bool compareResourceInfo(const ResourceGroupInfo & info1, const ResourceGroupInfo & info2)
    {
        return std::get<InfoIndexPriority>(info1) > std::get<InfoIndexPriority>(info2);
    }

    // 1. Update cpu time of resource group.
    // 2. Reorder resource_group_infos.
    void updateResourceGroupStatics(const std::string & name, const KeyspaceID & keyspace_id, UInt64 consumed_cpu_time);

    // Update resource_group_infos, will reorder resource group by priority.
    void updateResourceGroupInfosWithoutLock();

    // Submit task into task queue of specific resource group.
    void submitWithoutLock(TaskPtr && task);

    mutable std::mutex mu;
    std::condition_variable cv;

    bool is_finished = false;

    CompatorType compator = compareResourceInfo;
    ResourceGroupInfoQueue resource_group_infos;
    PipelineTasks pipeline_tasks;

    // <resource_group_name, acculumated_cpu_time>
    // when acculumated_cpu_time >= YIELD_MAX_TIME_SPENT_NS, will update resource group.
    // This is to prevent the resource group from being updated too frequently.
    std::unordered_map<std::string, UInt64> resource_group_statics;
};
} // namespace DB
