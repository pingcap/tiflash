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

#include <Flash/Executor/PipelineExecutor.h>
#include <Flash/Planner/PhysicalPlanVisitor.h>
#include <Interpreters/Context.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Storages/Transaction/TMTContext.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <Flash/Pipeline/PipelineManager.h>
#include <Flash/Pipeline/dag/DAGScheduler.h>

namespace DB
{
PipelineExecutor::PipelineExecutor(
    Context & context_,
    const PhysicalPlanNodePtr & plan_node_,
    const String & req_id,
    std::shared_ptr<ProcessListEntry> process_list_entry_)
    : QueryExecutor()
    , process_list_entry(process_list_entry_)
    , dag_scheduler(std::make_shared<DAGScheduler>(context_, context_.getDAGContext()->getMPPTaskId(), req_id))
    , plan_node(plan_node_)
    , context(context_)
{
    auto & pipeline_manager = context.getTMTContext().getMPPTaskManager()->getPipelineManager();
    pipeline_manager.registerDAGScheduler(dag_scheduler);
}

PipelineExecutor::~PipelineExecutor()
{
    auto & pipeline_manager = context.getTMTContext().getMPPTaskManager()->getPipelineManager();
    pipeline_manager.unregisterDAGScheduler(dag_scheduler->getMPPTaskId());
}

std::pair<bool, String> PipelineExecutor::execute(ResultHandler result_handler)
{
    auto res = dag_scheduler->run(plan_node, result_handler);
    plan_node = nullptr;
    return res;
}

String PipelineExecutor::dump() const
{
    assert(plan_node);
    return PhysicalPlanVisitor::visitToString(plan_node);
}

void PipelineExecutor::cancel(bool is_kill)
{
    plan_node = nullptr;
    dag_scheduler->cancel(is_kill);
}
} // namespace DB
