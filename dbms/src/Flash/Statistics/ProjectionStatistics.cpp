#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <Flash/Statistics/ProjectionStatistics.h>
#include <common/types.h>
#include <fmt/format.h>

namespace DB
{
bool ProjectionStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "Projection_");
}

ExecutorStatisticsPtr ProjectionStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, Context & context)
{
    using ProjectionStatisticsPtr = std::shared_ptr<ProjectionStatistics>;
    ProjectionStatisticsPtr statistics = std::make_shared<ProjectionStatistics>(executor_id, context);
    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                castBlockInputStream<IProfilingBlockInputStream>(stream_ptr, [&](const IProfilingBlockInputStream & stream) {
                    collectBaseInfo(statistics, stream.getProfileInfo());
                }),
                stream_ptr->getName(),
                "IProfilingBlockInputStream");
        });
    return statistics;
}
} // namespace DB