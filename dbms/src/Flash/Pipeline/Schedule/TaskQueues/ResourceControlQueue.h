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
    ~ResourceControlQueue() override {}

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
    using ResourceGroupInfoQueue = std::priority_queue<ResourceGroupInfo>;
    using ResourceGroupNameSet = std::unordered_set<std::string>;

    // Update used cpu time of resource group.
    void updateResourceGroupResource(const std::string & name, const KeyspaceID & keyspace_id, UInt64 consumed_cpu_time);

    // Update resource_group_infos, will reorder resource group by priority.
    // NOTE: not thread safe!
    void updateResourceGroupInfos();
    // NOTE: not thread safe!
    void doFinish();

    mutable std::mutex mu;

    std::atomic<bool> is_finished = false;

    std::condition_variable cv;

    ResourceGroupInfoQueue resource_group_infos;
    PipelineTasks pipeline_tasks;

    // <resource_group_name, acculumated_cpu_time>
    // when acculumated_cpu_time >= YIELD_MAX_TIME_SPENT_NS, will update resource group then set it to 0.
    std::unordered_map<std::string, UInt64> resource_group_statics;
};
} // namespace DB
