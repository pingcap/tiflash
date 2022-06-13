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

#include <Common/Logger.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/PartialSortingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/plans/PhysicalTopN.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalPlanPtr PhysicalTopN::build(
    const Context & context,
    const String & executor_id,
    const String & req_id,
    const tipb::TopN & top_n,
    PhysicalPlanPtr child)
{
    assert(child);

    if (unlikely(top_n.order_by_size() == 0))
    {
        //should not reach here
        throw TiFlashException("TopN executor without order by exprs", Errors::Coprocessor::BadRequest);
    }

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr before_sort_actions = PhysicalPlanHelper::newActions(child->getSampleBlock(), context);

    auto order_columns = analyzer.buildOrderColumns(before_sort_actions, top_n.order_by());
    SortDescription order_descr = getSortDescription(order_columns, top_n.order_by());

    auto physical_top_n = std::make_shared<PhysicalTopN>(executor_id, child->getSchema(), req_id, order_descr, before_sort_actions, top_n.limit());
    physical_top_n->appendChild(child);
    return physical_top_n;
}

void PhysicalTopN::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->transform(pipeline, context, max_streams);

    const Settings & settings = context.getSettingsRef();

    executeExpression(pipeline, before_sort_actions, log);

    pipeline.transform([&](auto & stream) {
        auto sorting_stream = std::make_shared<PartialSortingBlockInputStream>(stream, order_descr, log->identifier(), limit);

        /// Limits on sorting
        IProfilingBlockInputStream::LocalLimits limits;
        limits.mode = IProfilingBlockInputStream::LIMITS_TOTAL;
        limits.size_limits = SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode);
        sorting_stream->setLimits(limits);

        stream = sorting_stream;
    });

    /// If there are several streams, we merge them into one
    executeUnion(pipeline, max_streams, log);

    /// Merge the sorted blocks.
    pipeline.firstStream() = std::make_shared<MergeSortingBlockInputStream>(
        pipeline.firstStream(),
        order_descr,
        settings.max_block_size,
        limit,
        settings.max_bytes_before_external_sort,
        context.getTemporaryPath(),
        log->identifier());
}

void PhysicalTopN::finalize(const Names & parent_require)
{
    Names required_output = parent_require;
    required_output.reserve(required_output.size() + order_descr.size());
    for (const auto & desc : order_descr)
        required_output.push_back(desc.column_name);
    before_sort_actions->finalize(required_output);

    child->finalize(before_sort_actions->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(before_sort_actions, child->getSampleBlock().columns());

    FinalizeHelper::checkSampleBlockContainsSchema(getSampleBlock(), schema);
}

const Block & PhysicalTopN::getSampleBlock() const
{
    return before_sort_actions->getSampleBlock();
}
} // namespace DB
