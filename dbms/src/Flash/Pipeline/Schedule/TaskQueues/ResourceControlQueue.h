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

#include <Flash/Pipeline/Schedule/TaskQueues/FIFOQueryIdCache.h>
#include <Flash/Pipeline/Schedule/TaskQueues/TaskQueue.h>
#include <Flash/ResourceControl/LocalAdmissionController.h>

#include <mutex>
#include <unordered_map>

namespace DB
{
template <typename NestedQueueType>
class ResourceControlQueue : public TaskQueue
    , private boost::noncopyable
{
public:
    ResourceControlQueue() = default;
    ~ResourceControlQueue() override = default;

    void submit(TaskPtr && task) override;

    void submit(std::vector<TaskPtr> & tasks) override;

    bool take(TaskPtr & task) override;

    void updateStatistics(const TaskPtr & task, ExecTaskStatus, size_t inc_value) override;

    bool empty() const override;

    void finish() override;

    void cancel(const String & query_id, const String & resource_group_name) override;

private:
    bool tryTakeCancelTaskWithoutLock(TaskPtr & task);

    // <resource_group_name, pipeline_tasks>
    using PipelineTasks = std::unordered_map<std::string, std::shared_ptr<NestedQueueType>>;

    // <priority, corresponding_task_queue, resource_group_name>
    using ResourceGroupInfo = std::tuple<UInt64, std::shared_ptr<NestedQueueType>, std::string>;
    using CompatorType = bool (*)(const ResourceGroupInfo &, const ResourceGroupInfo &);
    using ResourceGroupInfoQueue = std::priority_queue<ResourceGroupInfo, std::vector<ResourceGroupInfo>, CompatorType>;

    // Index of ResourceGroupInfo.
    static constexpr auto InfoIndexPriority = 0;
    static constexpr auto InfoIndexPipelineTaskQueue = 1;
    static constexpr auto InfoIndexResourceGroupName = 2;
    static constexpr auto DEFAULT_WAIT_INTERVAL_WHEN_RUN_OUT_OF_RU = std::chrono::seconds(1);

    // ResourceGroupInfoQueue compator.
    static bool compareResourceInfo(const ResourceGroupInfo & info1, const ResourceGroupInfo & info2)
    {
        auto priority1 = std::get<InfoIndexPriority>(info1);
        auto priority2 = std::get<InfoIndexPriority>(info2);

        // Return true means lower priority.
        // Here we want make negative priority to be lower priority than positive priority.
        // Because negative priority means corresponding resource group has no more token.
        if (priority1 <= 0)
            return true;
        if (priority2 <= 0)
            return false;
        return priority1 > priority2;
    }

    // 1. Update cpu time of resource group.
    // 2. Reorder resource_group_infos.
    void updateResourceGroupStatisticWithoutLock(const std::string & name, UInt64 consumed_cpu_time);

    // Update resource_group_infos, will reorder resource group by priority.
    void updateResourceGroupInfosWithoutLock();

    // Submit task into task queue of specific resource group.
    void submitWithoutLock(TaskPtr && task);

    mutable std::mutex mu;
    std::condition_variable cv;

    bool is_finished = false;

    CompatorType compator = compareResourceInfo;
    ResourceGroupInfoQueue resource_group_infos{compator};
    PipelineTasks pipeline_tasks;

    // <resource_group_name, acculumated_cpu_time>
    // when acculumated_cpu_time >= YIELD_MAX_TIME_SPENT_NS, will update resource group.
    // This is to prevent the resource group from being updated too frequently.
    std::unordered_map<std::string, UInt64> resource_group_statistic;

    std::unordered_map<std::string, std::shared_ptr<NestedQueueType>> cancel_query_ids;
};
} // namespace DB
