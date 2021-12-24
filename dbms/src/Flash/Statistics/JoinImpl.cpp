#include <Flash/Statistics/JoinImpl.h>
#include <Interpreters/Join.h>

namespace DB
{
void JoinStatistics::appendExtraJson(FmtBuffer & fmt_buffer) const
{
    fmt_buffer.fmtAppend(
        R"("hash_table_bytes":{},"build_side_child":"{}")",
        hash_table_bytes,
        build_side_child);
}

void JoinStatistics::collectExtraRuntimeDetail()
{
    const auto & join_build_side_info_map = dag_context.getJoinBuildSideInfoMap();
    auto it = join_build_side_info_map.find(executor_id);
    if (it != join_build_side_info_map.end())
    {
        const auto & join_build_side_info = it->second;
        hash_table_bytes = join_build_side_info.join_ptr->getTotalByteCount();
        build_side_child = join_build_side_info.build_side_root_executor_id;
    }
}

JoinStatistics::JoinStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : JoinStatisticsBase(executor, dag_context_)
{}
} // namespace DB