// Copyright 2023 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Flash/Statistics/JoinImpl.h>
#include <Interpreters/Join.h>

namespace DB
{
void JoinStatistics::appendExtraJson(FmtBuffer & fmt_buffer) const
{
    fmt_buffer.fmtAppend(
        R"("hash_table_bytes":{},"build_side_child":"{}",)"
        R"("join_build_inbound_rows":{},"join_build_inbound_blocks":{},"join_build_inbound_bytes":{},"join_build_execution_time_ns":{})",
        hash_table_bytes,
        build_side_child,
        join_build_base.rows,
        join_build_base.blocks,
        join_build_base.bytes,
        join_build_base.execution_time_ns);
}


void JoinStatistics::collectExtraRuntimeDetail()
{
    const auto & join_execute_info_map = dag_context.getJoinExecuteInfoMap();
    auto it = join_execute_info_map.find(executor_id);
    if (it != join_execute_info_map.end())
    {
        const auto & join_execute_info = it->second;
        hash_table_bytes = join_execute_info.join_ptr->getTotalByteCount();
        build_side_child = join_execute_info.build_side_root_executor_id;
        for (const auto & join_build_stream : join_execute_info.join_build_streams)
        {
            if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(join_build_stream.get()); p_stream)
            {
                const auto & profile_info = p_stream->getProfileInfo();
                join_build_base.append(profile_info);
            }
        }
    }

    /// In TiFlash, a hash join's build side is finished before probe side starts,
    /// so the join probe side's running time does not include hash table's build time,
    /// when construct ExecutionSummaries, we need add the build cost to probe executor
    base.execution_time_ns += join_build_base.execution_time_ns;
}

JoinStatistics::JoinStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : JoinStatisticsBase(executor, dag_context_)
{}
} // namespace DB
