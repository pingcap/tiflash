#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <Flash/Statistics/ProjectionStatistics.h>
#include <common/types.h>
#include <fmt/format.h>

namespace DB
{
String ProjectionStatistics::toJson() const
{
    return fmt::format(
        R"({{"id":"{}","type":"{}"}})",
        id,
        type);
}

bool ProjectionStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "Projection_");
}

ExecutorStatisticsPtr ProjectionStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info [[maybe_unused]], DAGContext & dag_context [[maybe_unused]])
{
    using ProjectionStatisticsPtr = std::shared_ptr<ProjectionStatistics>;
    ProjectionStatisticsPtr statistics = std::make_shared<ProjectionStatistics>(executor_id);
    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                castBlockInputStream<IProfilingBlockInputStream>(stream_ptr, [&](const IProfilingBlockInputStream &) {}),
                stream_ptr->getName(),
                "IProfilingBlockInputStream");
        });
    return statistics;
}
} // namespace DB