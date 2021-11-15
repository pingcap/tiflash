#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Statistics/JoinStatistics.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <common/types.h>
#include <fmt/format.h>

namespace DB
{
String JoinStatistics::toString() const
{
    return fmt::format(
        R"({{"executor_id":"{}","probe_rows_selectivity":{},"probe_blocks_selectivity":{},"probe_bytes_selectivity":{}}})",
        executor_id,
        divide(probe_outbound_rows, probe_inbound_rows),
        divide(probe_outbound_blocks, probe_inbound_blocks),
        divide(probe_outbound_bytes, probe_inbound_bytes));
}

JoinStatisticsPtr JoinStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, DAGContext & dag_context [[maybe_unused]])
{
    JoinStatisticsPtr statistics = std::make_shared<JoinStatistics>(executor_id);
    visitProfileStreamsInfo(
        profile_streams_info,
        [&](const BlockStreamProfileInfo & profile_info) {
            statistics->probe_outbound_rows += profile_info.rows;
            statistics->probe_outbound_blocks += profile_info.blocks;
            statistics->probe_outbound_bytes += profile_info.bytes;
        },
        [&](const BlockStreamProfileInfo & child_profile_info) {
            statistics->probe_inbound_rows += child_profile_info.rows;
            statistics->probe_inbound_blocks += child_profile_info.blocks;
            statistics->probe_inbound_bytes += child_profile_info.bytes;
        });
    return statistics;
}

} // namespace DB