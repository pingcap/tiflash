// Copyright 2022 PingCAP, Ltd.
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

#include <Flash/Mpp/MPPTaskId.h>
#include <Flash/Pipeline/task/TaskScheduler.h>

#include <memory>
#include <unordered_map>

namespace DB
{
class DAGScheduler;
using DAGSchedulerPtr = std::shared_ptr<DAGScheduler>;
using DAGSchedulerMap = std::unordered_map<MPPTaskId, DAGSchedulerPtr>;

struct PipelineManager
{
    PipelineManager();

    std::unique_ptr<TaskScheduler> task_scheduler;
    DAGSchedulerMap dag_scheduler_map;

    std::mutex mu;

    DAGSchedulerPtr getDAGScheduler(const MPPTaskId & mpp_task_id);

    void registerDAGScheduler(const DAGSchedulerPtr & dag_scheduler);

    void unregisterDAGScheduler(const MPPTaskId & mpp_task_id);
};

using PipelineManagerPtr = std::unique_ptr<PipelineManager>;
} // namespace DB
