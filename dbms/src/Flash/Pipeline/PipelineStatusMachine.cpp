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

#include <Flash/Pipeline/PipelineStatusMachine.h>

namespace DB
{
void PipelineStatusMachine::addPipeline(const PipelinePtr & pipeline)
{
    assert(id_to_pipeline.find(pipeline->getId()) == id_to_pipeline.end());
    id_to_pipeline[pipeline->getId()] = pipeline;
}

PipelinePtr PipelineStatusMachine::getPipeline(UInt32 id) const
{
    auto it = id_to_pipeline.find(id);
    assert(it != id_to_pipeline.end());
    return it->second;
}

bool PipelineStatusMachine::isWaiting(UInt32 id) const
{
    assert(getPipeline(id));
    return waiting_ids.find(id) != waiting_ids.end();
}
bool PipelineStatusMachine::isRunning(UInt32 id) const
{
    assert(getPipeline(id));
    return running_ids.find(id) != running_ids.end();
}
bool PipelineStatusMachine::isCompleted(UInt32 id) const
{
    assert(getPipeline(id));
    return complete_ids.find(id) != complete_ids.end();
}

void PipelineStatusMachine::stateToWaiting(UInt32 id)
{
    assert(getPipeline(id));
    assert(!isRunning(id) && !isCompleted(id));
    waiting_ids.insert(id);
}
void PipelineStatusMachine::stateToRunning(UInt32 id)
{
    assert(getPipeline(id));
    assert(!isCompleted(id));
    waiting_ids.erase(id);
    running_ids.insert(id);
}
void PipelineStatusMachine::stateToComplete(UInt32 id)
{
    assert(getPipeline(id));
    waiting_ids.erase(id);
    running_ids.erase(id);
    complete_ids.insert(id);
}

std::unordered_set<PipelinePtr> PipelineStatusMachine::nextPipelines(UInt32 id) const
{
    assert(getPipeline(id));
    std::unordered_set<PipelinePtr> next_pipelines;
    for (const auto & [_, pipe] : id_to_pipeline)
    {
        const auto & parent_ids = pipe->getParentIds();
        if (parent_ids.find(id) != parent_ids.end())
            next_pipelines.insert(pipe);
    }
    return next_pipelines;
}
}
