// Copyright 2022 PingCAP, Ltd.
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
        R"("peak_build_bytes_usage":{},"build_side_child":"{}","is_spill_enabled":{},"is_spilled":{})"
        R"("non_joined_outbound_rows":{},"non_joined_outbound_blocks":{},"non_joined_outbound_bytes":{},"non_joined_execution_time_ns":{},)"
        R"("join_build_inbound_rows":{},"join_build_inbound_blocks":{},"join_build_inbound_bytes":{},"join_build_execution_time_ns":{})",
        peak_build_bytes_usage,
        build_side_child,
        is_spill_enabled,
        is_spilled,
        non_joined_base.rows,
        non_joined_base.blocks,
        non_joined_base.bytes,
        non_joined_base.execution_time_ns,
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
        peak_build_bytes_usage = join_execute_info.join_ptr->getPeakBuildBytesUsage();
        build_side_child = join_execute_info.build_side_root_executor_id;
        is_spill_enabled = join_execute_info.join_ptr->isEnableSpill();
        is_spilled = join_execute_info.join_ptr->isSpilled();
        for (const auto & non_joined_stream : join_execute_info.non_joined_streams)
        {
            if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(non_joined_stream.get()); p_stream)
            {
                const auto & profile_info = p_stream->getProfileInfo();
                non_joined_base.append(profile_info);
            }
        }
        for (const auto & join_build_stream : join_execute_info.join_build_streams)
        {
            if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(join_build_stream.get()); p_stream)
            {
                const auto & profile_info = p_stream->getProfileInfo();
                join_build_base.append(profile_info);
            }
        }
    }
}

JoinStatistics::JoinStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : JoinStatisticsBase(executor, dag_context_)
{}
} // namespace DB