#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <Flash/Statistics/FilterStatistics.h>
#include <common/types.h>
#include <fmt/format.h>

namespace DB
{
String FilterStatistics::extraToJson() const
{
    return fmt::format(
        R"(,"inbound_rows":{},"inbound_blocks":{},"inbound_bytes":{})",
        inbound_rows,
        inbound_blocks,
        inbound_bytes);
}

bool FilterStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "Selection_");
}

ExecutorStatisticsPtr FilterStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, Context & context)
{
    using FilterStatisticsPtr = std::shared_ptr<FilterStatistics>;
    FilterStatisticsPtr statistics = std::make_shared<FilterStatistics>(executor_id, context);
    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            return castBlockInputStream<FilterBlockInputStream>(stream_ptr, [&](const FilterBlockInputStream & stream) {
                collectBaseInfo(statistics, stream.getProfileInfo());
            });
        },
        [&](const BlockInputStreamPtr & child_stream_ptr) {
            throwFailCastException(
                castBlockInputStream<IProfilingBlockInputStream>(child_stream_ptr, [&](const IProfilingBlockInputStream & stream) {
                    collectInboundInfo(statistics, stream.getProfileInfo());
                }),
                child_stream_ptr->getName(),
                "IProfilingBlockInputStream");
        });
    return statistics;
}
} // namespace DB