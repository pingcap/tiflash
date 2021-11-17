#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/AggStatistics.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <fmt/format.h>

namespace DB
{
String AggStatistics::toJson() const
{
    return fmt::format(
        R"({{"id":"{}","type":"{}","rows_selectivity":{},"blocks_selectivity":{},"bytes_selectivity":{},"hash_table_rows":{}}})",
        id,
        type,
        divide(outbound_rows, inbound_rows),
        divide(outbound_blocks, inbound_blocks),
        divide(outbound_bytes, inbound_bytes),
        hash_table_rows);
}

bool AggStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "HashAgg_") || startsWith(executor_id, "StreamAgg_");
}

ExecutorStatisticsPtr AggStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, DAGContext & dag_context [[maybe_unused]])
{
    using AggStatisticsPtr = std::shared_ptr<AggStatistics>;
    AggStatisticsPtr statistics = std::make_shared<AggStatistics>(executor_id);
    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                elseThen(
                    [&]() {
                        return castBlockInputStream<AggregatingBlockInputStream>(stream_ptr, [&](const AggregatingBlockInputStream & stream) {
                            statistics->hash_table_rows += stream.getAggregatedDataVariantsSizeWithoutOverflowRow();
                            const auto & profile_info = stream.getProfileInfo();
                            statistics->outbound_rows += profile_info.rows;
                            statistics->outbound_blocks += profile_info.blocks;
                            statistics->outbound_bytes += profile_info.bytes;
                        });
                    },
                    [&]() {
                        return castBlockInputStream<ParallelAggregatingBlockInputStream>(stream_ptr, [&](const ParallelAggregatingBlockInputStream & stream) {
                            statistics->hash_table_rows += stream.getAggregatedDataVariantsSizeWithoutOverflowRow();
                            const auto & profile_info = stream.getProfileInfo();
                            statistics->outbound_rows += profile_info.rows;
                            statistics->outbound_blocks += profile_info.blocks;
                            statistics->outbound_bytes += profile_info.bytes;
                        });
                    }),
                stream_ptr->getName(),
                "AggregatingBlockInputStream/ParallelAggregatingBlockInputStream");
        },
        [&](const BlockInputStreamPtr & child_stream_ptr) {
            throwFailCastException(
                castBlockInputStream<IProfilingBlockInputStream>(child_stream_ptr, [&](const IProfilingBlockInputStream & stream) {
                    const auto & profile_info = stream.getProfileInfo();
                    statistics->inbound_rows += profile_info.rows;
                    statistics->inbound_blocks += profile_info.blocks;
                    statistics->inbound_bytes += profile_info.bytes;
                }),
                child_stream_ptr->getName(),
                "IProfilingBlockInputStream");
        });
    return statistics;
}

} // namespace DB