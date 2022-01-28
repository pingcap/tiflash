#include <Common/LogWithPrefix.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Planner/PhysicalFilter.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{
void PhysicalFilter::transform(DAGPipeline & pipeline, Context & context, size_t)
{
    const LogWithPrefixPtr & logger = context.getDAGContext()->log;
    pipeline.transform([&](auto & stream) { stream = std::make_shared<FilterBlockInputStream>(stream, before_filter_actions, filter_column, logger); });
}

bool PhysicalFilter::finalize(const Names & parent_require)
{
    Names required_output = parent_require;
    required_output.push_back(filter_column);
    before_filter_actions->finalize(required_output);

    if (child->finalize(before_filter_actions->getRequiredColumns()))
    {
        size_t columns_from_previous = child->getSampleBlock().columns();
        if (!before_filter_actions->getRequiredColumnsWithTypes().empty()
            && columns_from_previous > before_filter_actions->getRequiredColumnsWithTypes().size())
            before_filter_actions->prependProjectInput();
    }

    return true;
}

const Block & PhysicalFilter::getSampleBlock() const
{
    return before_filter_actions->getSampleBlock();
}
} // namespace DB