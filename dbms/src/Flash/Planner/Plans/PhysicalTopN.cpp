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

#include <Common/Logger.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/Plans/PhysicalTopN.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalTopN::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::TopN & top_n,
    const PhysicalPlanNodePtr & child)
{
    RUNTIME_CHECK(child);

    if (unlikely(top_n.order_by_size() == 0))
    {
        //should not reach here
        throw TiFlashException("TopN executor without order by exprs", Errors::Planner::BadRequest);
    }

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr before_sort_actions = PhysicalPlanHelper::newActions(child->getSampleBlock());

    auto order_columns = analyzer.buildOrderColumns(before_sort_actions, top_n.order_by());
    SortDescription order_descr = getSortDescription(order_columns, top_n.order_by());

    auto physical_top_n = std::make_shared<PhysicalTopN>(
        executor_id,
        child->getSchema(),
        child->getFineGrainedShuffle(),
        log->identifier(),
        child,
        order_descr,
        before_sort_actions,
        top_n.limit());
    return physical_top_n;
}

void PhysicalTopN::buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->buildBlockInputStream(pipeline, context, max_streams);

    executeExpression(pipeline, before_sort_actions, log, "before TopN");

    orderStreams(pipeline, max_streams, order_descr, limit, false, context, log);
}

void PhysicalTopN::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    size_t concurrency)
{
    executeExpression(exec_context, group_builder, before_sort_actions, log);

    // If the `limit` is very large, using a `final sort` can avoid outputting excessively large amounts of data.
    // TODO find a suitable threshold is necessary; 10000 is just a value picked without much consideration.
    if (group_builder.concurrency() * limit <= 10000)
    {
        executeLocalSort(exec_context, group_builder, order_descr, limit, false, context, log);
    }
    else
    {
        executeFinalSort(exec_context, group_builder, order_descr, limit, context, log);
        if (is_restore_concurrency)
            restoreConcurrency(
                exec_context,
                group_builder,
                concurrency,
                context.getSettingsRef().max_buffered_bytes_in_executor,
                log);
    }
}

void PhysicalTopN::finalizeImpl(const Names & parent_require)
{
    Names required_output = parent_require;
    required_output.reserve(required_output.size() + order_descr.size());
    for (const auto & desc : order_descr)
        required_output.emplace_back(desc.column_name);
    before_sort_actions->finalize(required_output);

    child->finalize(before_sort_actions->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(before_sort_actions, child->getSampleBlock().columns());

    FinalizeHelper::checkSampleBlockContainsParentRequire(getSampleBlock(), parent_require);
}

const Block & PhysicalTopN::getSampleBlock() const
{
    return before_sort_actions->getSampleBlock();
}
} // namespace DB
