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

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Planner/PhysicalPlanBuilder.h>
#include <Flash/Planner/plans/PhysicalLimit.h>
#include <Flash/Planner/plans/PhysicalTopN.h>
#include <Flash/Planner/plans/PhysicalSource.h>
#include <Interpreters/Context.h>

namespace DB
{
void PhysicalPlanBuilder::build(const String & executor_id, const tipb::Executor * executor)
{
    assert(executor);
    switch (executor->tp())
    {
    case tipb::ExecType::TypeLimit:
        cur_plans.push_back(PhysicalLimit::build(executor_id, log->identifier(), executor->limit(), popBack()));
        break;
    case tipb::ExecType::TypeTopN:
        cur_plans.push_back(PhysicalTopN::build(context, executor_id, log->identifier(), executor->topn(), popBack()));
        break;
    case tipb::ExecType::TypeSelection:
        cur_plans.push_back(PhysicalFilter::build(context, executor_id, log->identifier(), executor->selection(), popBack()));
        break;
    case tipb::ExecType::TypeAggregation:
    case tipb::ExecType::TypeStreamAgg:
        cur_plans.push_back(PhysicalAggregation::build(context, executor_id, log->identifier(), executor->aggregation(), popBack()));
        break;
    default:
        throw TiFlashException(fmt::format("{} executor is not supported", executor->tp()), Errors::Planner::Unimplemented);
    }
}

DAGContext & PhysicalPlanBuilder::dagContext() const
{
    return *context.getDAGContext();
}

PhysicalPlanPtr PhysicalPlanBuilder::popBack()
{
    RUNTIME_ASSERT(!cur_plans.empty(), log, "cur_plans is empty, cannot popBack");
    PhysicalPlanPtr back = cur_plans.back();
    cur_plans.pop_back();
    return back;
}

void PhysicalPlanBuilder::buildSource(const Block & sample_block)
{
    cur_plans.push_back(PhysicalSource::build(sample_block, log->identifier()));
}
} // namespace DB
