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

#include <Flash/Statistics/TableScanImpl.h>
#include <Interpreters/Join.h>
#include <Operators/UnorderedSourceOp.h>
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
    fmt_buffer.fmtAppend(
        R"("connection_details":[{},{}])",
        local_table_scan_detail.toJson(),
        remote_table_scan_detail.toJson());
}

void TableScanStatistics::collectExtraRuntimeDetail()
{
    const auto & connection_profiles_for_executor = dag_context.getConnectionProfilesMap();
    auto it = connection_profiles_for_executor.find(executor_id);
    if (it != connection_profiles_for_executor.end())
    {
        for (const auto & connection_profiles : it->second)
        {
            for (const auto & profile : connection_profiles)
            {
                remote_table_scan_detail.packets += profile.packets;
                remote_table_scan_detail.bytes += profile.bytes;
            }
        }
    }
    local_table_scan_detail.bytes = base.bytes;
}

TableScanStatistics::TableScanStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : TableScanStatisticsBase(executor, dag_context_)
{}
} // namespace DB
