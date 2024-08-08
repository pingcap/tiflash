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
#include <Flash/Planner/Plans/PhysicalWindowSort.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalWindowSort::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::Sort & window_sort,
    const FineGrainedShuffle & fine_grained_shuffle,
    const PhysicalPlanNodePtr & child)
{
    RUNTIME_CHECK(child);

    RUNTIME_ASSERT(window_sort.ispartialsort(), log, "for window sort, ispartialsort must be true");

    DAGExpressionAnalyzer analyzer(child->getSchema(), context);
    const auto & order_columns = analyzer.buildWindowOrderColumns(window_sort);
    const SortDescription & order_descr = getSortDescription(order_columns, window_sort.byitems());

    auto physical_window_sort = std::make_shared<PhysicalWindowSort>(
        executor_id,
        child->getSchema(),
        fine_grained_shuffle,
        log->identifier(),
        child,
        order_descr);
    return physical_window_sort;
}

void PhysicalWindowSort::buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->buildBlockInputStream(pipeline, context, max_streams);

    orderStreams(pipeline, max_streams, order_descr, 0, fine_grained_shuffle.enabled(), context, log);
}

void PhysicalWindowSort::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    size_t /*concurrency*/)
{
    if (fine_grained_shuffle.enabled())
        executeLocalSort(exec_context, group_builder, order_descr, {}, true, context, log);
    else
        executeFinalSort(exec_context, group_builder, order_descr, {}, context, log);
}

void PhysicalWindowSort::finalizeImpl(const Names & parent_require)
{
    Names required_output = parent_require;
    required_output.reserve(required_output.size() + order_descr.size());
    for (const auto & desc : order_descr)
        required_output.emplace_back(desc.column_name);

    child->finalize(required_output);
}

const Block & PhysicalWindowSort::getSampleBlock() const
{
    return child->getSampleBlock();
}
} // namespace DB
