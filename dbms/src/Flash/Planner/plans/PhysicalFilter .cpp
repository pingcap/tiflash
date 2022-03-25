#include <Common/Logger.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <Flash/Planner/plans/PhysicalFilter.h>
#include <Interpreters/Context.h>

namespace DB
{
void PhysicalFilter::transformImpl(DAGPipeline & pipeline, const Context & context, size_t max_streams)
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
        prependProjectInputIfNeed(before_filter_actions, child->getSampleBlock().columns());
}

const Block & PhysicalFilter::getSampleBlock() const
{
    return before_filter_actions->getSampleBlock();
}
} // namespace DB