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
    auto op_groups = pipeline->transform(context, concurrency);
    assert(op_groups.size() == 1);
    auto group = std::move(op_groups.back());
    if (group.empty())
        return true;
    std::vector<TaskPtr> tasks;
    tasks.reserve(group.size());
    for (auto & op_executor : group)
        tasks.push_back(std::make_unique<PipelineTask>(mem_tracker, shared_from_this(), std::move(op_executor)));
    scheduleTask(tasks);
    return false;
}
} // namespace DB
