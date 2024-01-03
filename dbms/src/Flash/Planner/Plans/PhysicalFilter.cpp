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
#include <DataStreams/FilterBlockInputStream.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/Plans/PhysicalFilter.h>
#include <Interpreters/Context.h>
#include <Operators/FilterTransformOp.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalFilter::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::Selection & selection,
    const PhysicalPlanNodePtr & child)
{
    RUNTIME_CHECK(child);

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr before_filter_actions = PhysicalPlanHelper::newActions(child->getSampleBlock());

    String filter_column_name = analyzer.buildFilterColumn(before_filter_actions, selection.conditions());

    auto physical_filter = std::make_shared<PhysicalFilter>(
        executor_id,
        child->getSchema(),
        child->getFineGrainedShuffle(),
        log->identifier(),
        child,
        filter_column_name,
        before_filter_actions);

    return physical_filter;
}

void PhysicalFilter::buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->buildBlockInputStream(pipeline, context, max_streams);

    pipeline.transform([&](auto & stream) {
        stream
            = std::make_shared<FilterBlockInputStream>(stream, before_filter_actions, filter_column, log->identifier());
    });
}

void PhysicalFilter::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & /*context*/,
    size_t /*concurrency*/)
{
    auto input_header = group_builder.getCurrentHeader();
    group_builder.transform([&](auto & builder) {
        builder.appendTransformOp(std::make_unique<FilterTransformOp>(
            exec_context,
            log->identifier(),
            input_header,
            before_filter_actions,
            filter_column));
    });
}

void PhysicalFilter::finalizeImpl(const Names & parent_require)
{
    Names required_output = parent_require;
    required_output.emplace_back(filter_column);
    before_filter_actions->finalize(required_output);

    child->finalize(before_filter_actions->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(before_filter_actions, child->getSampleBlock().columns());

    FinalizeHelper::checkSampleBlockContainsParentRequire(getSampleBlock(), parent_require);
}

const Block & PhysicalFilter::getSampleBlock() const
{
    return before_filter_actions->getSampleBlock();
}
} // namespace DB
