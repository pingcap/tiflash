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
class ResourceControlQueue
    : public TaskQueue
    , private boost::noncopyable
{
public:
    ResourceControlQueue()
    {
        LocalAdmissionController::global_instance->registerRefillTokenCallback([&]() {
            std::lock_guard lock(mu);
            cv.notify_all();
        });
    }

    ~ResourceControlQueue() override { LocalAdmissionController::global_instance->stop(); }

    void submit(TaskPtr && task) override;

    void submit(std::vector<TaskPtr> & tasks) override;

    bool take(TaskPtr & task) override;

    void updateStatistics(const TaskPtr & task, ExecTaskStatus, size_t inc_value) override;

    bool empty() const override;

    void finish() override;

    void cancel(const String & query_id, const String & resource_group_name) override;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    // <resource_group_name, resource_group_task_queues>
    using ResourceGroupTaskQueue = std::unordered_map<std::string, std::shared_ptr<NestedQueueType>>;

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
    // Larger value means lower priority.
    static bool compareResourceInfo(const ResourceGroupInfo & info1, const ResourceGroupInfo & info2)
    {
        // info1 outputs first if return false.
        return std::get<InfoIndexPriority>(info1) > std::get<InfoIndexPriority>(info2);
    }

    // 1. Update cpu time/RU of resource group.
    // 2. Update resource_group_infos and reorder resource_group_infos by priority.
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
    ResourceGroupTaskQueue resource_group_task_queues;

    // <resource_group_name, acculumated_cpu_time>
    // when acculumated_cpu_time >= YIELD_MAX_TIME_SPENT_NS, will update resource group.
    // This is to prevent the resource group from being updated too frequently.
    std::unordered_map<std::string, UInt64> resource_group_statistic;

    FIFOQueryIdCache cancel_query_id_cache;
    std::deque<TaskPtr> cancel_task_queue;
};
} // namespace DB
