#include <Common/Logger.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/plans/PhysicalProjection.h>
#include <Interpreters/Context.h>

namespace DB
{
void PhysicalProjection::transformImpl(DAGPipeline & pipeline, const Context & context, size_t max_streams)
{
    children(0)->transform(pipeline, context, max_streams);

    const auto & logger = context.getDAGContext()->log;
    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, project_actions, logger->identifier()); });
}

void PhysicalProjection::finalize(const Names & parent_require)
{
    Names required_output;
    required_output.reserve(schema.size() + parent_require.size());
    for (const auto & column : schema)
        required_output.push_back(column.name);
    for (const auto & name : parent_require)
        required_output.push_back(name);
    project_actions->finalize(required_output);

    child->finalize(project_actions->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(project_actions, child->getSampleBlock().columns());
}

const Block & PhysicalProjection::getSampleBlock() const
{
    return project_actions->getSampleBlock();
}
} // namespace DB