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

#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Statistics/TableScanImpl.h>
#include <Interpreters/Join.h>

namespace DB
{
String TableScanDetail::toJson() const
{
    return fmt::format(R"({{"is_local":{},"packets":{},"bytes":{}}})", is_local, packets, bytes);
}

void TableScanStatistics::appendExtraJson(FmtBuffer & fmt_buffer) const
{
    fmt_buffer.fmtAppend(
        R"("connection_details":[{},{}])",
        local_table_scan_detail.toJson(),
        remote_table_scan_detail.toJson());
}

void TableScanStatistics::updateTableScanDetail(const std::vector<ConnectionProfileInfo> & connection_profile_infos)
{
    for (const auto & connection_profile_info : connection_profile_infos)
    {
        remote_table_scan_detail.packets += connection_profile_info.packets;
        remote_table_scan_detail.bytes += connection_profile_info.bytes;
    }
}

void TableScanStatistics::collectExtraRuntimeDetail()
{
    switch (dag_context.getExecutionMode())
    {
    case ExecutionMode::None:
        break;
    case ExecutionMode::Stream:
        transformInBoundIOProfileForStream(dag_context, executor_id, [&](const IBlockInputStream & stream) {
            if (const auto * cop_stream = dynamic_cast<const CoprocessorBlockInputStream *>(&stream); cop_stream)
            {
                /// remote read
                updateTableScanDetail(cop_stream->getConnectionProfileInfos());
            }
            else if (const auto * local_stream = dynamic_cast<const IProfilingBlockInputStream *>(&stream);
                     local_stream)
            {
                /// local read input stream also is IProfilingBlockInputStream
                local_table_scan_detail.bytes += local_stream->getProfileInfo().bytes;
            }
            else
            {
                /// Streams like: NullBlockInputStream.
            }
        });
        break;
    case ExecutionMode::Pipeline:
        transformInBoundIOProfileForPipeline(dag_context, executor_id, [&](const IOProfileInfo & profile_info) {
            if (profile_info.is_local)
                local_table_scan_detail.bytes += profile_info.operator_info->bytes;
            else
                updateTableScanDetail(profile_info.connection_profile_infos);
        });
        break;
    }
}

TableScanStatistics::TableScanStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : TableScanStatisticsBase(executor, dag_context_)
{}
} // namespace DB
