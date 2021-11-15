#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <Flash/Statistics/FilterStatistics.h>
#include <common/types.h>
#include <fmt/format.h>

namespace DB
{
String FilterStatistics::toString() const
{
    return fmt::format(
        R"({{"executor_id":"{}","rows_selectivity":{},"blocks_selectivity":{},"bytes_selectivity":{},"avg_rows_per_block":{},"avg_bytes_per_block":{}}})",
        executor_id,
        divide(outbound_rows, inbound_rows),
        divide(outbound_blocks, inbound_blocks),
        divide(outbound_bytes, inbound_bytes),
        divide(outbound_rows, outbound_blocks),
        divide(outbound_bytes, outbound_blocks));
}

FilterStatisticsPtr FilterStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, DAGContext & dag_context [[maybe_unused]])
{
    FilterStatisticsPtr statistics = std::make_shared<FilterStatistics>(executor_id);
    visitProfileStreamsInfo(
        profile_streams_info,
        [&](const BlockStreamProfileInfo & profile_info) {
            statistics->outbound_rows += profile_info.rows;
            statistics->outbound_blocks += profile_info.blocks;
            statistics->outbound_bytes += profile_info.bytes;
        },
        [&](const BlockStreamProfileInfo & child_profile_info) {
            statistics->inbound_rows += child_profile_info.rows;
            statistics->inbound_blocks += child_profile_info.blocks;
            statistics->inbound_bytes += child_profile_info.bytes;
        });
    return statistics;
}
} // namespace DB