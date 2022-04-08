#include <Common/Logger.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/plans/PhysicalFilter.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalPlanPtr PhysicalFilter::build(
    const Context & context,
    const String & executor_id,
    const tipb::Selection & selection,
    PhysicalPlanPtr child)
{
    assert(child);

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr before_filter_actions = PhysicalPlanHelper::newActions(child->getSampleBlock(), context);

    std::vector<const tipb::Expr *> conditions;
    for (const auto & c : selection.conditions())
        conditions.push_back(&c);
    String filter_column_name = analyzer.buildFilterColumn(before_filter_actions, conditions);

    auto physical_filter = std::make_shared<PhysicalFilter>(executor_id, child->getSchema(), filter_column_name, before_filter_actions);
    physical_filter->appendChild(child);
    return physical_filter;
}

void PhysicalFilter::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    children(0)->transform(pipeline, context, max_streams);

    const auto & logger = context.getDAGContext()->log;
    pipeline.transform([&](auto & stream) { stream = std::make_shared<FilterBlockInputStream>(stream, before_filter_actions, filter_column, logger->identifier()); });
}

void PhysicalFilter::finalize(const Names & parent_require)
{
    Names required_output = parent_require;
    required_output.push_back(filter_column);
    before_filter_actions->finalize(required_output);

    child->finalize(before_filter_actions->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(before_filter_actions, child->getSampleBlock().columns());

    FinalizeHelper::checkSampleBlockContainsSchema(getSampleBlock(), schema);
}

const Block & PhysicalFilter::getSampleBlock() const
{
    return before_filter_actions->getSampleBlock();
}
} // namespace DB