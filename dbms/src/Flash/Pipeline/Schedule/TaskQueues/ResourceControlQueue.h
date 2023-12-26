// Copyright 2023 PingCAP, Inc.
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
template <typename NestedTaskQueueType>
class ResourceControlQueue
    : public TaskQueue
    , private boost::noncopyable
{
public:
    ResourceControlQueue()
    {
        RUNTIME_CHECK_MSG(
            LocalAdmissionController::global_instance != nullptr,
            "LocalAdmissionController::global_instance has not been initialized yet.");
        LocalAdmissionController::global_instance->registerRefillTokenCallback([&]() { cv.notify_all(); });
    }

    ~ResourceControlQueue() override { LocalAdmissionController::global_instance->unregisterRefillTokenCallback(); }

    void submit(TaskPtr && task) override;

    void submit(std::vector<TaskPtr> & tasks) override;

    bool take(TaskPtr & task) override;

    void updateStatistics(const TaskPtr & task, ExecTaskStatus exec_task_status, UInt64 inc_value) override;

    bool empty() const override;

    void finish() override;

    void cancel(const String & query_id, const String & resource_group_name) override;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    using NestedTaskQueuePtr = std::shared_ptr<NestedTaskQueueType>;
    using ResourceGroupTaskQueue = std::unordered_map<String, NestedTaskQueuePtr>;

    void submitWithoutLock(TaskPtr && task);

    struct ResourceGroupInfo
    {
        ResourceGroupInfo(const String & name_, UInt64 priority_, const NestedTaskQueuePtr & task_queue_)
            : name(name_)
            , priority(priority_)
            , task_queue(task_queue_)
        {}

        String name;
        UInt64 priority;
        NestedTaskQueuePtr task_queue;

        bool operator<(const ResourceGroupInfo & rhs) const
        {
            // Larger value means lower priority.
            return priority > rhs.priority;
        }
    };

    static constexpr const char * error_template = "resource group {} not found, maybe has been deleted";

    // Update resource_group_infos, will reorder resource group by priority.
    // Return true if got error resource group.
    bool updateResourceGroupInfosWithoutLock();

    // Erase resource group info and task_queue.
    void mustEraseResourceGroupInfoWithoutLock(const String & name);
    static void mustTakeTask(const NestedTaskQueuePtr & task_queue, TaskPtr & task);

    mutable std::mutex mu;
    std::condition_variable cv;

    bool is_finished = false;

    std::priority_queue<ResourceGroupInfo> resource_group_infos;
    ResourceGroupTaskQueue resource_group_task_queues;

    FIFOQueryIdCache cancel_query_id_cache;
    std::deque<TaskPtr> cancel_task_queue;

    // Store tasks whose resource group info is not found in LAC,
    // it will be cancelled in take().
    std::deque<TaskPtr> error_task_queue;
};
} // namespace DB
