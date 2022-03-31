#include <Common/Logger.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/plans/PhysicalProjection.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalPlanPtr PhysicalProjection::build(
    const Context & context,
    const String & executor_id,
    const tipb::Projection & projection,
    PhysicalPlanPtr child)
{
    assert(child);

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr project_actions = PhysicalPlanHelper::newActions(child->getSampleBlock(), context);

    NamesAndTypes schema;
    NamesWithAliases project_aliases;
    UniqueNameGenerator unique_name_generator;
    bool should_add_project_alias = false;
    for (const auto & expr : projection.exprs())
    {
        auto expr_name = analyzer.getActions(expr, project_actions);
        const auto & col = project_actions->getSampleBlock().getByName(expr_name);

        String alias = unique_name_generator.toUniqueName(col.name);
        project_aliases.emplace_back(col.name, alias);
        should_add_project_alias |= (alias != col.name);

        schema.emplace_back(alias, col.type);
    }
    if (should_add_project_alias)
        project_actions->add(ExpressionAction::project(project_aliases));

    auto physical_projection = std::make_shared<PhysicalProjection>(executor_id, schema, project_actions);
    physical_projection->appendChild(child);
    return physical_projection;
}

PhysicalPlanPtr PhysicalProjection::buildNonRootFinal(
    const Context & context,
    const String & column_prefix,
    PhysicalPlanPtr child)
{
    assert(child);

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr project_actions = PhysicalPlanHelper::newActions(child->getSampleBlock(), context);
    auto final_project_aliases = analyzer.genNonRootFinalProjectAliases(column_prefix);
    project_actions->add(ExpressionAction::project(final_project_aliases));

    NamesAndTypes schema = child->getSchema();
    assert(final_project_aliases.size() == schema.size());
    // replace name by alias.
    for (size_t i = 0; i < final_project_aliases.size(); ++i)
    {
        assert(schema[i].name == final_project_aliases[i].first);
        schema[i].name = final_project_aliases[i].second;
    }

    auto physical_projection = std::make_shared<PhysicalProjection>("NonRootFinalProjection", schema, project_actions);
    // For final projection, no need to record profile streams.
    physical_projection->disableRecordProfileStreams();
    physical_projection->appendChild(child);
    return physical_projection;
}

PhysicalPlanPtr PhysicalProjection::buildRootFinal(
    const Context & context,
    const std::vector<tipb::FieldType> & require_schema,
    const std::vector<Int32> & output_offsets,
    const String & column_prefix,
    bool keep_session_timezone_info,
    const PhysicalPlanPtr & child)
{
    assert(child);

    if (unlikely(output_offsets.empty()))
        throw Exception("Root Query block without output_offsets", ErrorCodes::LOGICAL_ERROR);

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

    auto physical_projection = std::make_shared<PhysicalProjection>("RootFinalProjection", schema, project_actions);
    // For final projection, no need to record profile streams.
    physical_projection->disableRecordProfileStreams();
    physical_projection->appendChild(child);
    return physical_projection;
}

void PhysicalProjection::transformImpl(DAGPipeline & pipeline, const Context & context, size_t max_streams)
{
    children(0)->transform(pipeline, context, max_streams);

    const auto & logger = context.getDAGContext()->log;
    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, project_actions, logger->identifier()); });
}

void PhysicalProjection::finalize(const Names & parent_require)
{
    // Maybe parent_require.size() > schema.size() for root final projection.
    if (parent_require.size() > schema.size())
        FinalizeHelper::checkParentRequireContainsSchema(parent_require, schema);
    else
        FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
    project_actions->finalize(parent_require);

    child->finalize(project_actions->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(project_actions, child->getSampleBlock().columns());

    if (parent_require.size() >= schema.size())
        FinalizeHelper::checkSampleBlockContainsSchema(getSampleBlock(), schema);
    else
        FinalizeHelper::checkSchemaContainsSampleBlock(schema, getSampleBlock());
}

const Block & PhysicalProjection::getSampleBlock() const
{
    return project_actions->getSampleBlock();
}
} // namespace DB