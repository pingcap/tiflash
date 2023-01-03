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

#include <Flash/Pipeline/Pipeline.h>
#include <Flash/Pipeline/PipelineEvent.h>
#include <Flash/Pipeline/PipelineTask.h>

namespace DB
{
bool PipelineEvent::scheduleImpl()
{
    exec_status.addActivePipeline();

    assert(pipeline);
    auto op_pipeline_groups = pipeline->transform(context, concurrency);
    pipeline.reset();

    // Until `non-joined-fetch` and `fine grained shuffle` are supported, the size of op_pipeline_groups can only be 1.
    assert(op_pipeline_groups.size() == 1);
    auto op_pipeline_group = std::move(op_pipeline_groups.back());
    if (op_pipeline_group.empty())
        return true;

    std::vector<TaskPtr> tasks;
    tasks.reserve(op_pipeline_group.size());
    for (auto & op_pipeline : op_pipeline_group)
        tasks.push_back(std::make_unique<PipelineTask>(mem_tracker, shared_from_this(), std::move(op_pipeline)));
    scheduleTask(tasks);
    return false;
}
} // namespace DB
