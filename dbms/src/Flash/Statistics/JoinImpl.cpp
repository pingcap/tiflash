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
        R"("hash_table_bytes":{},"build_side_child":"{}",)"
        R"("non_joined_outbound_rows":{},"non_joined_outbound_blocks":{},"non_joined_outbound_bytes":{},"non_joined_execution_time_ns":{})",
        hash_table_bytes,
        build_side_child,
        non_joined_base.rows,
        non_joined_base.blocks,
        non_joined_base.bytes,
        non_joined_base.execution_time_ns);
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
        for (const auto & non_joined_stream : join_execute_info.non_joined_streams)
        {
            auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(non_joined_stream.get());
            assert(p_stream);
            const auto & profile_info = p_stream->getProfileInfo();
            non_joined_base.append(profile_info);
        }
    }
}

JoinStatistics::JoinStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : JoinStatisticsBase(executor, dag_context_)
{}
} // namespace DB