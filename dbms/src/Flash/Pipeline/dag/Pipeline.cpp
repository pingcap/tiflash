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

#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Pipeline/dag/Pipeline.h>
#include <Flash/Planner/PhysicalPlanVisitor.h>
#include <Interpreters/Context.h>
#include <Transforms/TransformsPipeline.h>

namespace DB
{
String Pipeline::toString() const
{
    return fmt::format("{{pipeline_id: {}, mpp_task_id: {}}}", id, mpp_task_id.toString());
}

Pipeline::Pipeline(
    const PhysicalPlanNodePtr & plan_node_,
    const MPPTaskId & mpp_task_id_,
    UInt32 id_,
    const std::unordered_set<UInt32> & parent_ids_,
    const String & req_id)
    : plan_node(plan_node_)
    , mpp_task_id(mpp_task_id_)
    , id(id_)
    , parent_ids(parent_ids_)
    , log(Logger::get("Pipeline", req_id, fmt::format("<pipeline_id:{}>", id)))
{
    assert(plan_node);
    LOG_FMT_DEBUG(log, "pipeline plan node:\n{}", PhysicalPlanVisitor::visitToString(plan_node));
}

std::vector<PipelineTask> Pipeline::transform(Context & context, size_t concurrency)
{
    assert(plan_node);
    TransformsPipeline pipeline;
    plan_node->transform(pipeline, context, concurrency);

    std::vector<PipelineTask> tasks;
    for (const auto & transforms : pipeline.transforms_vec)
    {
        tasks.emplace_back(active_task_num++, id, mpp_task_id, transforms);
        task_transforms_vec.push_back(transforms);
    }
    return tasks;
}

void Pipeline::cancel(bool is_kill)
{
    for (const auto & transforms : task_transforms_vec)
    {
        transforms->cancel(is_kill);
    }
    task_transforms_vec.clear();
}

void Pipeline::finish(size_t task_id)
{
    assert(active_task_num > 0);
    assert(!task_transforms_vec.empty());
    --active_task_num;
    task_transforms_vec[task_id] = nullptr;
}

void Pipeline::finish()
{
    task_transforms_vec = {};
    plan_node = nullptr;
}
} // namespace DB
