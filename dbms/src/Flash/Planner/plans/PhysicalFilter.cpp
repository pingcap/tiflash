#include <Common/LogWithPrefix.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Planner/plans/PhysicalFilter.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <Interpreters/Context.h>
#include <Flash/Planner/FinalizeHelper.h>

namespace DB
{
void PhysicalFilter::transform(DAGPipeline & pipeline, const Context & context, size_t)
{
    const LogWithPrefixPtr & logger = context.getDAGContext()->log;
    pipeline.transform([&](auto & stream) { stream = std::make_shared<FilterBlockInputStream>(stream, before_filter_actions, filter_column, logger); });
    recordProfileStreams(pipeline, *context.getDAGContext());
}

bool PhysicalFilter::finalize(const Names & parent_require)
{
    Names required_output = parent_require;
    required_output.push_back(filter_column);
    before_filter_actions->finalize(required_output);

    if (child->finalize(before_filter_actions->getRequiredColumns()))
        prependProjectInputIfNeed(before_filter_actions, child->getSampleBlock().columns());

    return true;
}

const Block & PhysicalFilter::getSampleBlock() const
{
    return before_filter_actions->getSampleBlock();
}
} // namespace DB