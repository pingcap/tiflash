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

namespace DB
{
std::pair<bool, String> PipelineExecutor::execute(ResultHandler result_handler)
{
    auto res = dag_scheduler.run(plan_node, result_handler);
    plan_node = nullptr;
    return res;
}

String PipelineExecutor::dump() const
{
    assert(plan_node);
    return PhysicalPlanVisitor::visitToString(plan_node);
    ;
}
} // namespace DB
