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

namespace DB
{
class ResourceControlQueue : public TaskQueue
{
public:
    ~ResourceControlQueue() override;

    void submit(TaskPtr && task) override;

    void submit(std::vector<TaskPtr> & tasks) override;

    bool take(TaskPtr & task) override;

    void updateStatistics(const TaskPtr & task, size_t inc_value) override;

    bool empty() const override;

    void finish() override;

private:
    using PipelineTasks = std::vector<std::shared_ptr<CPUMultiLevelFeedbackQueue>>;
    // <priority, iterator_of_MLFQ_of_pipeline_tasks, resource_group_name>
    using ResourceGroupQueue = std::priority_queue<std::tuple<double, std::shared_ptr<CPUMultiLevelFeedbackQueue>, std::string>>;

    std::mutex mu;

    bool is_finished;

    std::condition_variable cv;

    ResourceGroupQueue resource_groups;
    PipelineTasks pipeline_tasks;
};
} // namespace DB
