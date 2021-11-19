#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <Flash/Statistics/JoinStatistics.h>
#include <common/types.h>
#include <fmt/format.h>

namespace DB
{
String JoinStatistics::toJson() const
{
    return fmt::format(
        R"({{"id":"{}","type":"{}","probe_rows_selectivity":{},"probe_blocks_selectivity":{},"probe_bytes_selectivity":{},"hash_table_bytes":{},"process_time_for_build":{}}})",
        id,
        type,
        divide(probe_outbound_rows, probe_inbound_rows),
        divide(probe_outbound_blocks, probe_inbound_blocks),
        divide(probe_outbound_bytes, probe_inbound_bytes),
        hash_table_bytes,
        process_time_for_build);
}

bool JoinStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "HashJoin_");
}

ExecutorStatisticsPtr JoinStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, DAGContext & dag_context)
{
    using JoinStatisticsPtr = std::shared_ptr<JoinStatistics>;
    JoinStatisticsPtr statistics = std::make_shared<JoinStatistics>(executor_id);
    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                castBlockInputStream<ExpressionBlockInputStream>(stream_ptr, [&](const ExpressionBlockInputStream & stream) {
                    const auto & profile_info = stream.getProfileInfo();
                    statistics->probe_outbound_rows += profile_info.rows;
                    statistics->probe_outbound_blocks += profile_info.blocks;
                    statistics->probe_outbound_bytes += profile_info.bytes;
                }),
                stream_ptr->getName(),
                "ExpressionBlockInputStream");
        },
        [&](const BlockInputStreamPtr & child_stream_ptr) {
            throwFailCastException(
                castBlockInputStream<IProfilingBlockInputStream>(child_stream_ptr, [&](const IProfilingBlockInputStream & stream) {
                    const auto & profile_info = stream.getProfileInfo();
                    statistics->probe_inbound_rows += profile_info.rows;
                    statistics->probe_inbound_blocks += profile_info.blocks;
                    statistics->probe_inbound_bytes += profile_info.bytes;
                }),
                child_stream_ptr->getName(),
                "IProfilingBlockInputStream");
        });

    for (auto & join_alias : dag_context.getQBIdToJoinAliasMap()[profile_streams_info.qb_id])
    {
        const auto & profile_streams_map_for_join_build_side = dag_context.getProfileStreamsMapForJoinBuildSide();
        auto join_build_side_it = profile_streams_map_for_join_build_side.find(join_alias);
        if (join_build_side_it != profile_streams_map_for_join_build_side.end())
        {
            visitBlockInputStreamsRecursive(
                dag_context.getProfileStreamsMapForJoinBuildSide()[join_alias],
                [&](const BlockInputStreamPtr & stream_ptr) {
                    return castBlockInputStream<HashJoinBuildBlockInputStream>(stream_ptr, [&](const HashJoinBuildBlockInputStream & stream) {
                        statistics->hash_table_bytes += stream.getJoinPtr()->getTotalByteCount();
                        const auto & profile_info = stream.getProfileInfo();
                        statistics->process_time_for_build = std::max(statistics->process_time_for_build, profile_info.execution_time);
                    });
                });
        }
    }

    return statistics;
}
} // namespace DB