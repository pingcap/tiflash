#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <Flash/Statistics/AggStatistics.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <fmt/format.h>

namespace DB
{
String AggStatistics::extraToJson() const
{
    return fmt::format(
        R"(,"inbound_rows":{},"inbound_blocks":{},"inbound_bytes":{},"hash_table_rows":{})",
        inbound_rows,
        inbound_blocks,
        inbound_bytes,
        hash_table_rows);
}

bool AggStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "HashAgg_") || startsWith(executor_id, "StreamAgg_");
}

ExecutorStatisticsPtr AggStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, Context & context)
{
    using AggStatisticsPtr = std::shared_ptr<AggStatistics>;
    AggStatisticsPtr statistics = std::make_shared<AggStatistics>(executor_id, context);
    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                elseThen(
                    [&]() {
                        return castBlockInputStream<AggregatingBlockInputStream>(stream_ptr, [&](const AggregatingBlockInputStream & stream) {
                            statistics->hash_table_rows += stream.getAggregatedDataVariantsSizeWithoutOverflowRow();
                            collectBaseInfo(statistics, stream.getProfileInfo());
                        });
                    },
                    [&]() {
                        return castBlockInputStream<ParallelAggregatingBlockInputStream>(stream_ptr, [&](const ParallelAggregatingBlockInputStream & stream) {
                            statistics->hash_table_rows += stream.getAggregatedDataVariantsSizeWithoutOverflowRow();
                            collectBaseInfo(statistics, stream.getProfileInfo());
                        });
                    }),
                stream_ptr->getName(),
                "AggregatingBlockInputStream/ParallelAggregatingBlockInputStream");
            return true;
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