#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Planner/PhysicalPlan.h>

namespace DB
{
void PhysicalPlan::recordProfileStreams(DAGPipeline & pipeline, DAGContext & dag_context)
{
    if (is_record_profile_streams)
    {
        auto & profile_streams = dag_context.getProfileStreamsMap()[executor_id];
        pipeline.transform([&profile_streams](auto & stream) { profile_streams.push_back(stream); });
    }
}
} // namespace DB