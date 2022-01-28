#include <Common/LogWithPrefix.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Planner/PhysicalProjection.h>
#include <Interpreters/Context.h>

namespace DB
{
void PhysicalProjection::transform(DAGPipeline & pipeline, Context & context, size_t)
{
    const LogWithPrefixPtr & logger = context.getDAGContext()->log;
    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, project_actions, logger); });
}

bool PhysicalProjection::finalize(const Names & parent_require)
{
    Names required_output = schema;
    required_output.reserve(schema.size() + parent_require.size());
    for (const auto & name : parent_require)
        required_output.push_back(name);
    project_actions->finalize(required_output);

    if (child->finalize(project_actions->getRequiredColumns()))
    {
        size_t columns_from_previous = child->getSampleBlock().columns();
        if (!project_actions->getRequiredColumnsWithTypes().empty()
            && columns_from_previous > project_actions->getRequiredColumnsWithTypes().size())
            project_actions->prependProjectInput();
    }

    return true;
}

const Block & PhysicalProjection::getSampleBlock() const
{
    return project_actions->getSampleBlock();
}

bool PhysicalProjection::isOnlyProject() const
{
    const auto & actions = project_actions->getActions();
    if (actions.size() == 1)
    {
        const auto & action = actions.back();
        if (action.type == ExpressionAction::PROJECT)
        {
            const auto & projections = action.projections;
            if (projections.size() == schema.size())
            {
                for (size_t i = 0; i < schema.size(); ++i)
                {
                    const auto projection = projections[i];
                    if (projection.first != schema[i]
                        || (!projection.second.empty()
                            && projection.second != projection.first))
                        return false;
                }
                return true;
            }
        }
    }
    return false;
}
} // namespace DB