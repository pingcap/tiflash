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

#include <Common/FailPoint.h>
#include <Common/Logger.h>
#include <Common/TiFlashException.h>
#include <DataTypes/DataTypeNullable.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/Plans/PhysicalExpand.h>
#include <Interpreters/Context.h>
#include <fmt/format.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalExpand::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::Expand & expand,
    const PhysicalPlanNodePtr & child)
{
    RUNTIME_CHECK(child);

    if (unlikely(expand.grouping_sets().empty()))
    {
        //should not reach here
        throw TiFlashException("Expand executor without grouping sets", Errors::Planner::BadRequest);
    }

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr before_expand_actions = PhysicalPlanHelper::newActions(child->getSampleBlock());

    auto grouping_sets = analyzer.buildExpandGroupingColumns(expand, before_expand_actions);
    auto expand_action = ExpressionAction::expandSource(grouping_sets);
    // include expand action itself.
    before_expand_actions->add(expand_action);

    NamesAndTypes expand_output_columns;
    auto child_header = child->getSchema();
    for (const auto & one : child_header)
    {
        expand_output_columns.emplace_back(
            one.name,
            expand_action.expand->isInGroupSetColumn(one.name) ? makeNullable(one.type) : one.type);
    }
    expand_output_columns.emplace_back(
        Expand::grouping_identifier_column_name,
        Expand::grouping_identifier_column_type);

    auto physical_expand = std::make_shared<PhysicalExpand>(
        executor_id,
        expand_output_columns,
        child->getFineGrainedShuffle(),
        log->identifier(),
        child,
        expand_action.expand,
        before_expand_actions);
    return physical_expand;
}

void PhysicalExpand::expandTransform(DAGPipeline & child_pipeline)
{
    String expand_extra_info = fmt::format(
        "expand, expand_executor_id = {}: grouping set {}",
        execId(),
        shared_expand->getGroupingSetsDes());
    executeExpression(child_pipeline, expand_actions, log, expand_extra_info);
}

void PhysicalExpand::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & /*context*/,
    size_t /*concurrency*/)
{
    executeExpression(exec_context, group_builder, expand_actions, log);
}

void PhysicalExpand::buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->buildBlockInputStream(pipeline, context, max_streams);
    expandTransform(pipeline);
}

void PhysicalExpand::finalizeImpl(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
    Names required_output = parent_require;
    required_output.emplace_back(Expand::grouping_identifier_column_name);
    expand_actions->finalize(required_output);

    child->finalize(expand_actions->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(expand_actions, child->getSampleBlock().columns());
    FinalizeHelper::checkSampleBlockContainsParentRequire(getSampleBlock(), parent_require);
}

const Block & PhysicalExpand::getSampleBlock() const
{
    return expand_actions->getSampleBlock();
}
} // namespace DB
