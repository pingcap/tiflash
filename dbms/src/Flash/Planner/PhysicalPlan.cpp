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

#include <Common/TiFlashMetrics.h>
#include <Debug/MockStorage.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/FineGrainedShuffle.h>
#include <Flash/Pipeline/Pipeline.h>
#include <Flash/Pipeline/PipelineBuilder.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <Flash/Planner/PhysicalPlanVisitor.h>
#include <Flash/Planner/Plans/PhysicalAggregation.h>
#include <Flash/Planner/Plans/PhysicalExchangeReceiver.h>
#include <Flash/Planner/Plans/PhysicalExchangeSender.h>
#include <Flash/Planner/Plans/PhysicalExpand.h>
#include <Flash/Planner/Plans/PhysicalFilter.h>
#include <Flash/Planner/Plans/PhysicalJoin.h>
#include <Flash/Planner/Plans/PhysicalLimit.h>
#include <Flash/Planner/Plans/PhysicalMockExchangeReceiver.h>
#include <Flash/Planner/Plans/PhysicalMockExchangeSender.h>
#include <Flash/Planner/Plans/PhysicalMockTableScan.h>
#include <Flash/Planner/Plans/PhysicalProjection.h>
#include <Flash/Planner/Plans/PhysicalTableScan.h>
#include <Flash/Planner/Plans/PhysicalTopN.h>
#include <Flash/Planner/Plans/PhysicalWindow.h>
#include <Flash/Planner/Plans/PhysicalWindowSort.h>
#include <Flash/Planner/optimize.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace
{
bool pushDownSelection(Context & context, const PhysicalPlanNodePtr & plan, const String & executor_id, const tipb::Selection & selection)
{
    if (plan->tp() == PlanType::TableScan)
    {
        auto physical_table_scan = std::static_pointer_cast<PhysicalTableScan>(plan);
        return physical_table_scan->setFilterConditions(executor_id, selection);
    }
    if (unlikely(plan->tp() == PlanType::MockTableScan && context.isExecutorTest() && !context.getSettingsRef().enable_pipeline))
    {
        auto physical_mock_table_scan = std::static_pointer_cast<PhysicalMockTableScan>(plan);
        if (context.mockStorage()->useDeltaMerge() && context.mockStorage()->tableExistsForDeltaMerge(physical_mock_table_scan->getLogicalTableID()))
        {
            return physical_mock_table_scan->setFilterConditions(context, executor_id, selection);
        }
    }
    return false;
}
} // namespace

void PhysicalPlan::build(const tipb::DAGRequest * dag_request)
{
    assert(dag_request);
    traverseExecutorsReverse(
        dag_request,
        [&](const tipb::Executor & executor) {
            build(&executor);
            return true;
        });
}

void PhysicalPlan::buildTableScan(const String & executor_id, const tipb::Executor * executor)
{
    TiDBTableScan table_scan(executor, executor_id, dagContext());
    if (unlikely(context.isTest()))
        pushBack(PhysicalMockTableScan::build(context, executor_id, log, table_scan));
    else
        pushBack(PhysicalTableScan::build(executor_id, log, table_scan));
    dagContext().table_scan_executor_id = executor_id;
}

void PhysicalPlan::build(const tipb::Executor * executor)
{
    assert(executor);
    assert(executor->has_executor_id());
    const auto & executor_id = executor->executor_id();
    switch (executor->tp())
    {
    case tipb::ExecType::TypeLimit:
        GET_METRIC(tiflash_coprocessor_executor_count, type_limit).Increment();
        pushBack(PhysicalLimit::build(executor_id, log, executor->limit(), popBack()));
        break;
    case tipb::ExecType::TypeTopN:
        GET_METRIC(tiflash_coprocessor_executor_count, type_topn).Increment();
        pushBack(PhysicalTopN::build(context, executor_id, log, executor->topn(), popBack()));
        break;
    case tipb::ExecType::TypeSelection:
    {
        GET_METRIC(tiflash_coprocessor_executor_count, type_sel).Increment();
        auto child = popBack();
        if (pushDownSelection(context, child, executor_id, executor->selection()))
            pushBack(child);
        else
            pushBack(PhysicalFilter::build(context, executor_id, log, executor->selection(), child));
        break;
    }
    case tipb::ExecType::TypeStreamAgg:
        RUNTIME_CHECK_MSG(executor->aggregation().group_by_size() == 0, "Group by key is not supported in StreamAgg");
    case tipb::ExecType::TypeAggregation:
        GET_METRIC(tiflash_coprocessor_executor_count, type_agg).Increment();
        pushBack(PhysicalAggregation::build(context, executor_id, log, executor->aggregation(), FineGrainedShuffle(executor), popBack()));
        break;
    case tipb::ExecType::TypeExchangeSender:
    {
        GET_METRIC(tiflash_coprocessor_executor_count, type_exchange_sender).Increment();
        buildFinalProjection(fmt::format("{}_", executor_id), true);
        if (unlikely(context.isExecutorTest() || context.isInterpreterTest()))
            pushBack(PhysicalMockExchangeSender::build(executor_id, log, popBack()));
        else
        {
            // for MPP test, we can use real exchangeSender to run an query across different compute nodes
            // or use one compute node to simulate MPP process.
            pushBack(PhysicalExchangeSender::build(executor_id, log, executor->exchange_sender(), FineGrainedShuffle(executor), popBack()));
        }
        break;
    }
    case tipb::ExecType::TypeExchangeReceiver:
    {
        GET_METRIC(tiflash_coprocessor_executor_count, type_exchange_receiver).Increment();
        if (unlikely(context.isExecutorTest() || context.isInterpreterTest()))
        {
            pushBack(PhysicalMockExchangeReceiver::build(context, executor_id, log, executor->exchange_receiver(), FineGrainedShuffle(executor)));
        }
        else
        {
            // for MPP test, we can use real exchangeReceiver to run an query across different compute nodes
            // or use one compute node to simulate MPP process.
            pushBack(PhysicalExchangeReceiver::build(context, executor_id, log, FineGrainedShuffle(executor)));
        }
        break;
    }
    case tipb::ExecType::TypeProjection:
        GET_METRIC(tiflash_coprocessor_executor_count, type_projection).Increment();
        pushBack(PhysicalProjection::build(context, executor_id, log, executor->projection(), popBack()));
        break;
    case tipb::ExecType::TypeWindow:
        GET_METRIC(tiflash_coprocessor_executor_count, type_window).Increment();
        pushBack(PhysicalWindow::build(context, executor_id, log, executor->window(), FineGrainedShuffle(executor), popBack()));
        break;
    case tipb::ExecType::TypeSort:
        GET_METRIC(tiflash_coprocessor_executor_count, type_window_sort).Increment();
        pushBack(PhysicalWindowSort::build(context, executor_id, log, executor->sort(), FineGrainedShuffle(executor), popBack()));
        break;
    case tipb::ExecType::TypeTableScan:
        GET_METRIC(tiflash_coprocessor_executor_count, type_ts).Increment();
        buildTableScan(executor_id, executor);
        break;
    case tipb::ExecType::TypePartitionTableScan:
        GET_METRIC(tiflash_coprocessor_executor_count, type_partition_ts).Increment();
        buildTableScan(executor_id, executor);
        break;
    case tipb::ExecType::TypeJoin:
    {
        GET_METRIC(tiflash_coprocessor_executor_count, type_join).Increment();
        /// Both sides of the join need to have non-root-final-projection to ensure that
        /// there are no duplicate columns in the blocks on the build and probe sides.
        buildFinalProjection(fmt::format("{}_r_", executor_id), false);
        auto right = popBack();

        buildFinalProjection(fmt::format("{}_l_", executor_id), false);
        auto left = popBack();

        pushBack(PhysicalJoin::build(context, executor_id, log, executor->join(), FineGrainedShuffle(executor), left, right));
        break;
    }
    case tipb::ExecType::TypeExpand:
    {
        GET_METRIC(tiflash_coprocessor_executor_count, type_expand).Increment();
        pushBack(PhysicalExpand::build(context, executor_id, log, executor->expand(), popBack()));
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
    RUNTIME_CHECK(!cur_plan_nodes.empty());
    PhysicalPlanNodePtr back = cur_plan_nodes.back();
    assert(back);
    cur_plan_nodes.pop_back();
    return back;
}

/// For MPP, root final projection has been added under PhysicalExchangeSender or PhysicalMockExchangeSender.
/// For batchcop/cop that without PhysicalExchangeSender or PhysicalMockExchangeSender, We need to add root final projection.
void PhysicalPlan::addRootFinalProjectionIfNeed()
{
    assert(root_node);
    if (root_node->tp() != PlanType::ExchangeSender && root_node->tp() != PlanType::MockExchangeSender)
    {
        pushBack(root_node);
        buildFinalProjection(fmt::format("{}_", root_node->execId()), true);
        root_node = popBack();
    }
}

PhysicalPlanNodePtr PhysicalPlan::outputAndOptimize()
{
    RUNTIME_ASSERT(!root_node, log, "root_node should be nullptr before `outputAndOptimize`");
    RUNTIME_ASSERT(cur_plan_nodes.size() == 1, log, "There can only be one plan node output, but here are {}", cur_plan_nodes.size());
    root_node = popBack();
    addRootFinalProjectionIfNeed();

    LOG_DEBUG(
        log,
        "build unoptimized physical plan: \n{}",
        toString());

    root_node = optimize(context, root_node, log);
    LOG_DEBUG(
        log,
        "build optimized physical plan: \n{}",
        toString());

    RUNTIME_ASSERT(root_node, log, "root_node shouldn't be nullptr after `outputAndOptimize`");

    return root_node;
}

String PhysicalPlan::toString() const
{
    assert(root_node);
    return PhysicalPlanVisitor::visitToString(root_node);
}

void PhysicalPlan::buildBlockInputStream(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    assert(root_node);
    root_node->buildBlockInputStream(pipeline, context, max_streams);
}

PipelinePtr PhysicalPlan::toPipeline()
{
    assert(root_node);
    PipelineBuilder builder{log->identifier()};
    root_node->buildPipeline(builder);
    root_node.reset();
    auto pipeline = builder.build();
    auto to_string = [&]() -> String {
        FmtBuffer buffer;
        pipeline->toTreeString(buffer);
        return buffer.toString();
    };
    LOG_DEBUG(
        log,
        "build pipeline dag: \n{}",
        to_string());
    return pipeline;
}
} // namespace DB
