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
#include <Storages/DeltaMerge/ScanContext.h>

namespace DB
{
String TableScanDetail::toJson() const
{
    return fmt::format(
        R"({{"is_local":{},"packets":{},"bytes":{}}})",
        is_local,
        packets,
        bytes);
}

void TableScanStatistics::appendExtraJson(FmtBuffer & fmt_buffer) const
{
    auto scan_ctx_it = dag_context.scan_context_map.find(executor_id);
    fmt_buffer.fmtAppend(
        R"("connection_details":[{},{}],"scan_details":{})",
        local_table_scan_detail.toJson(),
        remote_table_scan_detail.toJson(),
        scan_ctx_it != dag_context.scan_context_map.end() ? scan_ctx_it->second->toJson()
                                                          : "{}" // empty json object for nullptr
    );
}

void TableScanStatistics::collectExtraRuntimeDetail()
{
    const auto & io_stream_map = dag_context.getInBoundIOInputStreamsMap();
    auto it = io_stream_map.find(executor_id);
    if (it != io_stream_map.end())
    {
        for (const auto & io_stream : it->second)
        {
            auto * cop_stream = dynamic_cast<CoprocessorBlockInputStream *>(io_stream.get());
            /// In tiflash_compute node, TableScan will be converted to ExchangeReceiver.
            auto * exchange_stream = dynamic_cast<ExchangeReceiverInputStream *>(io_stream.get());
            if (cop_stream || exchange_stream)
            {
                const std::vector<ConnectionProfileInfo> * connection_profile_infos = nullptr;
                if (cop_stream)
                    connection_profile_infos = &cop_stream->getConnectionProfileInfos();
                else if (exchange_stream)
                    connection_profile_infos = &exchange_stream->getConnectionProfileInfos();

                for (const auto & connection_profile_info : *connection_profile_infos)
                {
                    remote_table_scan_detail.packets += connection_profile_info.packets;
                    remote_table_scan_detail.bytes += connection_profile_info.bytes;
                }
            }
            else if (auto * local_stream = dynamic_cast<IProfilingBlockInputStream *>(io_stream.get()); local_stream)
            {
                /// local read input stream also is IProfilingBlockInputStream
                local_table_scan_detail.bytes += local_stream->getProfileInfo().bytes;
            }
            else
            {
                /// Streams like: NullBlockInputStream.
            }
        }
    }
}

TableScanStatistics::TableScanStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : TableScanStatisticsBase(executor, dag_context_)
{}
} // namespace DB
