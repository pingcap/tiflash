// Copyright 2023 PingCAP, Inc.
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
        R"("peak_build_bytes_usage":{},"build_side_child":"{}","is_spill_enabled":{},"is_spilled":{},)"
        R"("join_build_inbound_rows":{},"join_build_inbound_blocks":{},"join_build_inbound_bytes":{},)"
        R"("join_build_inbound_allocated_bytes":{},"join_build_concurrency":{},"join_build_execution_time_ns":{})",
        peak_build_bytes_usage,
        build_side_child,
        is_spill_enabled,
        is_spilled,
        join_build_base.rows,
        join_build_base.blocks,
        join_build_base.bytes,
        join_build_base.allocated_bytes,
        join_build_base.concurrency,
        join_build_base.execution_time_ns);
}

void JoinStatistics::collectExtraRuntimeDetail()
{
    const auto & join_execute_info_map = dag_context.getJoinExecuteInfoMap();
    auto it = join_execute_info_map.find(executor_id);
    if (it != join_execute_info_map.end())
    {
        const auto & join_execute_info = it->second;
        peak_build_bytes_usage = join_execute_info.join_profile_info->peak_build_bytes_usage;
        build_side_child = join_execute_info.build_side_root_executor_id;
        is_spill_enabled = join_execute_info.join_profile_info->is_spill_enabled;
        is_spilled = join_execute_info.join_profile_info->is_spilled;
        switch (dag_context.getExecutionMode())
        {
        case ExecutionMode::None:
            break;
        case ExecutionMode::Stream:
            for (const auto & join_build_stream : join_execute_info.join_build_streams)
            {
                if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(join_build_stream.get()); p_stream)
                    join_build_base.append(p_stream->getProfileInfo());
            }
            break;
        case ExecutionMode::Pipeline:
            for (const auto & join_build_profile_info : join_execute_info.join_build_profile_infos)
                join_build_base.append(*join_build_profile_info);
            break;
        }
    }
}

JoinStatistics::JoinStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : JoinStatisticsBase(executor, dag_context_)
{}
} // namespace DB
