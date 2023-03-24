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

#include <Flash/Pipeline/Pipeline.h>
#include <Flash/Pipeline/Schedule/Events/PlainPipelineEvent.h>
#include <Flash/Pipeline/Schedule/Tasks/PipelineTask.h>

namespace DB
{
std::vector<TaskPtr> PlainPipelineEvent::scheduleImpl()
{
    assert(pipeline);
    auto pipeline_exec_group = pipeline->buildExecGroup(exec_status, context, concurrency);
    RUNTIME_CHECK(!pipeline_exec_group.empty());
    std::vector<TaskPtr> tasks;
    tasks.reserve(pipeline_exec_group.size());
    for (auto & pipeline_exec : pipeline_exec_group)
        tasks.push_back(std::make_unique<PipelineTask>(mem_tracker, log->identifier(), exec_status, shared_from_this(), std::move(pipeline_exec)));
    return tasks;
}

void PlainPipelineEvent::finishImpl()
{
    // Plan nodes in pipeline hold resources like hash table for join, when destruction they will operate memory tracker in MPP task. But MPP task may get destructed once `exec_status.onEventFinish()` is called.
    // So pipeline needs to be released before `exec_status.onEventFinish()` is called.
    pipeline.reset();
}
} // namespace DB
