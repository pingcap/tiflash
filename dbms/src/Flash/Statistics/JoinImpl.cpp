#include <Common/TiFlashException.h>
#include <Flash/Statistics/JoinImpl.h>

namespace DB
{
void JoinStatistics::appendExtraJson(FmtBuffer & fmt_buffer) const
{
    fmt_buffer.fmtAppend(
        R"("hash_table_bytes":{},"inbound_rows_for_build":{},"inbound_blocks_for_build":{},"inbound_bytes_for_build":{},"process_time_ns_for_build":{})",
        hash_table_bytes,
        inbound_rows_for_build,
        inbound_blocks_for_build,
        inbound_bytes_for_build,
        process_time_ns_for_build);
}

void JoinStatistics::collectExtraRuntimeDetail()
{
    const auto & profile_streams_info = dag_context.getProfileStreams(executor_id);
    for (auto & join_alias : dag_context.getQBIdToJoinAliasMap()[profile_streams_info.qb_id])
    {
        const auto & profile_streams_map_for_join_build_side = dag_context.getProfileStreamsMapForJoinBuildSide();
        auto join_build_side_it = profile_streams_map_for_join_build_side.find(join_alias);
        if (join_build_side_it != profile_streams_map_for_join_build_side.end())
        {
            for (const auto & join_build_side_stream : join_build_side_it->second)
            {
                auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(join_build_side_stream.get());
                assert(p_stream);
                const auto & profile_info = p_stream->getProfileInfo();
                inbound_rows_for_build += profile_info.rows;
                inbound_blocks_for_build += profile_info.blocks;
                inbound_bytes_for_build += profile_info.bytes;
                process_time_ns_for_build = std::max(execution_time_ns, profile_info.execution_time);
            }
        }
    }
}

JoinStatistics::JoinStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : JoinStatisticsBase(executor, dag_context_)
{
    if (!dag_context.isMPPTask())
    {
        throw TiFlashException("Join is only supported in mpp", Errors::Coprocessor::Internal);
    }
}
} // namespace DB