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
#include <Flash/Planner/ExecutorIdGenerator.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <Flash/Planner/PhysicalPlanVisitor.h>
#include <Flash/Planner/optimize.h>
#include <Flash/Planner/plans/PhysicalAggregation.h>
#include <Flash/Planner/plans/PhysicalExchangeReceiver.h>
#include <Flash/Planner/plans/PhysicalExchangeSender.h>
#include <Flash/Planner/plans/PhysicalFilter.h>
#include <Flash/Planner/plans/PhysicalJoin.h>
#include <Flash/Planner/plans/PhysicalLimit.h>
#include <Flash/Planner/plans/PhysicalMockExchangeReceiver.h>
#include <Flash/Planner/plans/PhysicalMockExchangeSender.h>
#include <Flash/Planner/plans/PhysicalProjection.h>
#include <Flash/Planner/plans/PhysicalSource.h>
#include <Flash/Planner/plans/PhysicalTopN.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Interpreters/Context.h>

namespace DB
{
void PhysicalPlan::build(const tipb::DAGRequest * dag_request)
{
    assert(dag_request);
    ExecutorIdGenerator id_generator;
    traverseExecutorsReverse(
        dag_request,
        [&](const tipb::Executor & executor) {
            build(id_generator.generate(executor), &executor);
            return true;
        });
}

void PhysicalPlan::build(const String & executor_id, const tipb::Executor * executor)
{
    assert(executor);
    switch (executor->tp())
    {
    case tipb::ExecType::TypeLimit:
        pushBack(PhysicalLimit::build(executor_id, log, executor->limit(), popBack()));
        break;
    case tipb::ExecType::TypeTopN:
        pushBack(PhysicalTopN::build(context, executor_id, log, executor->topn(), popBack()));
        break;
    case tipb::ExecType::TypeSelection:
        pushBack(PhysicalFilter::build(context, executor_id, log, executor->selection(), popBack()));
        break;
    case tipb::ExecType::TypeAggregation:
    case tipb::ExecType::TypeStreamAgg:
        pushBack(PhysicalAggregation::build(context, executor_id, log, executor->aggregation(), popBack()));
        break;
    case tipb::ExecType::TypeExchangeSender:
    {
        if (unlikely(dagContext().isTest()))
            pushBack(PhysicalMockExchangeSender::build(executor_id, log, popBack()));
        else
            pushBack(PhysicalExchangeSender::build(executor_id, log, executor->exchange_sender(), popBack()));
        break;
    }
    case tipb::ExecType::TypeExchangeReceiver:
    {
        if (unlikely(dagContext().isTest()))
            pushBack(PhysicalMockExchangeReceiver::build(context, executor_id, log, executor->exchange_receiver()));
        else
            pushBack(PhysicalExchangeReceiver::build(context, executor_id, log));
        break;
    }
    case tipb::ExecType::TypeProjection:
        pushBack(PhysicalProjection::build(context, executor_id, log, executor->projection(), popBack()));
        break;
    case tipb::ExecType::TypeJoin:
    {
        if (dagContext().isTest())
            buildFinalProjection(fmt::format("{}_l_", executor_id), false);
        auto left = popBack();

        if (dagContext().isTest())
            buildFinalProjection(fmt::format("{}_r_", executor_id), false);
        auto right = popBack();

        pushBack(PhysicalJoin::build(context, executor_id, log, executor->join(), left, right));
        break;
    }
    default:
        throw TiFlashException(fmt::format("{} executor is not supported", executor->tp()), Errors::Planner::Unimplemented);
    }
}

void PhysicalPlan::buildFinalProjection(const String & column_prefix, bool is_root)
{
    const auto & final_projection = is_root
        ? PhysicalProjection::buildRootFinal(
            context,
            log,
            dagContext().output_field_types,
            dagContext().output_offsets,
            column_prefix,
            dagContext().keep_session_timezone_info,
            popBack())
        : PhysicalProjection::buildNonRootFinal(
            context,
            log,
            column_prefix,
            popBack());
    pushBack(final_projection);
}

DAGContext & PhysicalPlan::dagContext() const
{
    return *context.getDAGContext();
}

void PhysicalPlan::pushBack(const PhysicalPlanNodePtr & plan_node)
{
    assert(plan_node);
    cur_plan_nodes.push_back(plan_node);
}

PhysicalPlanNodePtr PhysicalPlan::popBack()
{
    if (unlikely(cur_plan_nodes.empty()))
        throw TiFlashException("cur_plan_nodes is empty, cannot popBack", Errors::Planner::Internal);
    PhysicalPlanNodePtr back = cur_plan_nodes.back();
    assert(back);
    cur_plan_nodes.pop_back();
    return back;
}

void PhysicalPlan::buildSource(const BlockInputStreams & source_streams)
{
    pushBack(PhysicalSource::build(source_streams, log));
}

void PhysicalPlan::outputAndOptimize()
{
    RUNTIME_ASSERT(!root_node, log, "root_node shoud be nullptr before `outputAndOptimize`");
    RUNTIME_ASSERT(cur_plan_nodes.size() == 1, log, "There can only be one plan node output, but here are {}", cur_plan_nodes.size());

    root_node = popBack();
    LOG_FMT_DEBUG(
        log,
        "build unoptimized physical plan: \n{}",
        toString());

    root_node = optimize(context, root_node);
    LOG_FMT_DEBUG(
        log,
        "build optimized physical plan: \n{}",
        toString());

    RUNTIME_ASSERT(root_node, log, "root_node shoudn't be nullptr after `outputAndOptimize`");
}

String PhysicalPlan::toString() const
{
    assert(root_node);
    return PhysicalPlanVisitor::visitToString(root_node);
}

void PhysicalPlan::transform(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    assert(root_node);
    root_node->transform(pipeline, context, max_streams);
}
} // namespace DB
