#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <Flash/Statistics/JoinStatistics.h>
#include <Interpreters/Context.h>
#include <common/types.h>
#include <fmt/format.h>

namespace DB
{
String JoinStatistics::extraToJson() const
{
    return fmt::format(
        R"(,"inbound_rows":{},"inbound_blocks":{},"inbound_bytes":{},"hash_table_bytes":{},"process_time_ns_for_build":{})",
        inbound_rows,
        inbound_blocks,
        inbound_bytes,
        hash_table_bytes,
        process_time_ns_for_build);
}

bool JoinStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "HashJoin_");
}

ExecutorStatisticsPtr JoinStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, Context & context)
{
    using JoinStatisticsPtr = std::shared_ptr<JoinStatistics>;
    JoinStatisticsPtr statistics = std::make_shared<JoinStatistics>(executor_id, context);
    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                castBlockInputStream<ExpressionBlockInputStream>(stream_ptr, [&](const ExpressionBlockInputStream & stream) {
                    collectBaseInfo(statistics, stream.getProfileInfo());
                }),
                stream_ptr->getName(),
                "ExpressionBlockInputStream");
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

    auto & dag_context = *context.getDAGContext();
    for (auto & join_alias : dag_context.getQBIdToJoinAliasMap()[profile_streams_info.qb_id])
    {
        const auto & profile_streams_map_for_join_build_side = dag_context.getProfileStreamsMapForJoinBuildSide();
        auto join_build_side_it = profile_streams_map_for_join_build_side.find(join_alias);
        if (join_build_side_it != profile_streams_map_for_join_build_side.end())
        {
            visitBlockInputStreamsRecursive(
                context.getDAGContext()->getProfileStreamsMapForJoinBuildSide()[join_alias],
                [&](const BlockInputStreamPtr & stream_ptr) {
                    return castBlockInputStream<HashJoinBuildBlockInputStream>(stream_ptr, [&](const HashJoinBuildBlockInputStream & stream) {
                        statistics->hash_table_bytes += stream.getJoinPtr()->getTotalByteCount();
                        const auto & profile_info = stream.getProfileInfo();
                        statistics->process_time_ns_for_build = std::max(statistics->process_time_ns_for_build, profile_info.execution_time);
                    });
                });
        }
    }

    return statistics;
}
} // namespace DB