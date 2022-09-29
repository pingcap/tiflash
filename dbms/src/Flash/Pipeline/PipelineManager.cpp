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

#include <Flash/Pipeline/PipelineManager.h>
#include <Flash/Pipeline/dag/DAGScheduler.h>

namespace DB
{
PipelineManager::PipelineManager(const ServerInfo & server_info)
    : task_scheduler(std::make_unique<TaskScheduler>(*this, server_info))
{}

DAGSchedulerPtr PipelineManager::getDAGScheduler(const MPPTaskId & mpp_task_id)
{
    std::shared_lock lock(rwlock);
    auto dag_it = dag_scheduler_map.find(mpp_task_id);
    return dag_it != dag_scheduler_map.end() ? dag_it->second : nullptr;
}

void PipelineManager::registerDAGScheduler(const DAGSchedulerPtr & dag_scheduler)
{
    std::unique_lock lock(rwlock);
    dag_scheduler_map[dag_scheduler->getMPPTaskId()] = dag_scheduler;
}

void PipelineManager::unregisterDAGScheduler(const MPPTaskId & mpp_task_id)
{
    std::unique_lock lock(rwlock);
    auto dag_it = dag_scheduler_map.find(mpp_task_id);
    if (dag_it != dag_scheduler_map.end())
    {
        dag_scheduler_map.erase(dag_it);
    }
}
} // namespace DB
