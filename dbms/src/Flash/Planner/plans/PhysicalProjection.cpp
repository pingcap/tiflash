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
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/plans/PhysicalProjection.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalProjection::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::Projection & projection,
    const PhysicalPlanNodePtr & child)
{
    assert(child);

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr project_actions = PhysicalPlanHelper::newActions(child->getSampleBlock(), context);

    NamesAndTypes schema;
    NamesWithAliases project_aliases;
    UniqueNameGenerator unique_name_generator;
    for (const auto & expr : projection.exprs())
    {
        auto expr_name = analyzer.getActions(expr, project_actions);
        const auto & col = project_actions->getSampleBlock().getByName(expr_name);

        String alias = unique_name_generator.toUniqueName(col.name);
        project_aliases.emplace_back(col.name, alias);
        schema.emplace_back(alias, col.type);
    }
    /// TODO When there is no alias, there is no need to add the project action.
    /// https://github.com/pingcap/tiflash/issues/3921
    project_actions->add(ExpressionAction::project(project_aliases));

    auto physical_projection = std::make_shared<PhysicalProjection>(
        executor_id,
        schema,
        log->identifier(),
        child,
        "projection",
        project_actions);
    return physical_projection;
}

PhysicalPlanNodePtr PhysicalProjection::buildNonRootFinal(
    const Context & context,
    const LoggerPtr & log,
    const String & column_prefix,
    const PhysicalPlanNodePtr & child)
{
    assert(child);

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr project_actions = PhysicalPlanHelper::newActions(child->getSampleBlock(), context);
    auto final_project_aliases = analyzer.genNonRootFinalProjectAliases(column_prefix);
    project_actions->add(ExpressionAction::project(final_project_aliases));

    NamesAndTypes schema = child->getSchema();
    assert(final_project_aliases.size() == schema.size());
    // replace column name of schema by alias.
    for (size_t i = 0; i < final_project_aliases.size(); ++i)
    {
        assert(schema[i].name == final_project_aliases[i].first);
        schema[i].name = final_project_aliases[i].second;
    }

    auto physical_projection = std::make_shared<PhysicalProjection>(
        child->execId(),
        schema,
        log->identifier(),
        child,
        "final projection",
        project_actions);
    // For final projection, no need to record profile streams.
    physical_projection->disableRecordProfileStreams();
    return physical_projection;
}

PhysicalPlanNodePtr PhysicalProjection::buildRootFinal(
    const Context & context,
    const LoggerPtr & log,
    const std::vector<tipb::FieldType> & require_schema,
    const std::vector<Int32> & output_offsets,
    const String & column_prefix,
    bool keep_session_timezone_info,
    const PhysicalPlanNodePtr & child)
{
    assert(child);

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr project_actions = PhysicalPlanHelper::newActions(child->getSampleBlock(), context);

    NamesWithAliases final_project_aliases = analyzer.buildFinalProjection(
        project_actions,
        require_schema,
        output_offsets,
        column_prefix,
        keep_session_timezone_info);

    project_actions->add(ExpressionAction::project(final_project_aliases));

    assert(final_project_aliases.size() == output_offsets.size());
    NamesAndTypes schema;
    for (size_t i = 0; i < final_project_aliases.size(); ++i)
    {
        const auto & alias = final_project_aliases[i].second;
        assert(!alias.empty());
        const auto & type = analyzer.getCurrentInputColumns()[output_offsets[i]].type;
        schema.emplace_back(alias, type);
    }

    auto physical_projection = std::make_shared<PhysicalProjection>(
        child->execId(),
        schema,
        log->identifier(),
        child,
        "final projection",
        project_actions);
    // For final projection, no need to record profile streams.
    physical_projection->disableRecordProfileStreams();
    return physical_projection;
}

void PhysicalProjection::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->transform(pipeline, context, max_streams);

    executeExpression(pipeline, project_actions, log, extra_info);
}

void PhysicalProjection::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
    project_actions->finalize(parent_require);

    child->finalize(project_actions->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(project_actions, child->getSampleBlock().columns());

    FinalizeHelper::checkSampleBlockContainsSchema(getSampleBlock(), schema);
}

const Block & PhysicalProjection::getSampleBlock() const
{
    return project_actions->getSampleBlock();
}
} // namespace DB
