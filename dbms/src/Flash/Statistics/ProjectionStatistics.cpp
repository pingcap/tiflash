#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <Flash/Statistics/ProjectionStatistics.h>
#include <Interpreters/Context.h>

namespace DB
{
ProjectionStatistics::ProjectionStatistics(const tipb::Executor * executor, Context & context_)
    : ExecutorStatistics(executor, context_)
{}

bool ProjectionStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "Projection_");
}

void ProjectionStatistics::collectRuntimeDetail()
{
    const auto & profile_streams_info = context.getDAGContext()->getProfileStreams(executor_id);
    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                castBlockInputStream<IProfilingBlockInputStream>(stream_ptr, [&](const IProfilingBlockInputStream & stream) {
                    collectBaseInfo(this, stream.getProfileInfo());
                }),
                stream_ptr->getName(),
                "IProfilingBlockInputStream");
        });
}
} // namespace DB