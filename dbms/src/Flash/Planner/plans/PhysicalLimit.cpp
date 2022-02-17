#include <Common/LogWithPrefix.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/plans/PhysicalLimit.h>
#include <Interpreters/Context.h>

namespace DB
{
void PhysicalLimit::transform(DAGPipeline & pipeline, const Context & context, size_t max_streams)
{
    children(0)->transform(pipeline, context, max_streams);

    const LogWithPrefixPtr & logger = context.getDAGContext()->log;
    pipeline.transform([&](auto & stream) { stream = std::make_shared<LimitBlockInputStream>(stream, limit, 0, logger, false); });
    if (pipeline.hasMoreThanOneStream())
    {
        executeUnion(pipeline, max_streams, logger);
        pipeline.transform([&](auto & stream) { stream = std::make_shared<LimitBlockInputStream>(stream, limit, 0, logger, false); });
    }
    recordProfileStreams(pipeline, *context.getDAGContext());
}

bool PhysicalLimit::finalize(const Names & parent_require)
{
    return child->finalize(parent_require);
}

const Block & PhysicalLimit::getSampleBlock() const
{
    return child->getSampleBlock();
}
} // namespace DB