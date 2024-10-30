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

#include <Flash/Pipeline/Pipeline.h>
#include <Flash/Pipeline/Schedule/Events/Impls/PlainPipelineEvent.h>
#include <Flash/Pipeline/Schedule/Tasks/Impls/PipelineTask.h>

namespace DB
{
void PlainPipelineEvent::scheduleImpl()
{
    RUNTIME_CHECK(pipeline);
    auto pipeline_exec_group = pipeline->buildExecGroup(exec_context, context, concurrency, minTSO_wait_time_in_ms);
    RUNTIME_CHECK(!pipeline_exec_group.empty());
    for (auto & pipeline_exec : pipeline_exec_group)
        addTask(std::make_unique<PipelineTask>(
            exec_context,
            log->identifier(),
            shared_from_this(),
            std::move(pipeline_exec)));
}

void PlainPipelineEvent::finishImpl()
{
    if (auto complete_event = pipeline->complete(exec_context); complete_event)
        insertEvent(complete_event);
    // Plan nodes in pipeline hold resources like hash table for join, when destruction they will operate memory tracker in MPP task. But MPP task may get destructed once `exec_context.decActiveRefCount()` is called.
    // So pipeline needs to be released before `exec_context.decActiveRefCount()` is called.
    pipeline.reset();
}

} // namespace DB
