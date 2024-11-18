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

#pragma once

#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <Flash/Statistics/ExecutorStatistics.h>
#include <tipb/executor.pb.h>

namespace DB
{
struct TableScanDetail : public ConnectionProfileInfo
{
    const bool is_local;
    double min_stream_cost_ns = -1.0;
    double max_stream_cost_ns = -1.0;

    explicit TableScanDetail(bool is_local_)
        : is_local(is_local_)
    {}

    String toJson() const;
};

struct TableScanImpl
{
    static constexpr bool has_extra_info = true;

    static constexpr auto type = "TableScan";

    static bool isMatch(const tipb::Executor * executor)
    {
        return executor->has_tbl_scan() || executor->has_partition_table_scan();
    }

    static bool isSourceExecutor() { return true; }
};
using TableScanStatisticsBase = ExecutorStatistics<TableScanImpl>;

class TableScanStatistics : public TableScanStatisticsBase
{
public:
    TableScanStatistics(const tipb::Executor * executor, DAGContext & dag_context_);

private:
    TableScanDetail local_table_scan_detail{true};
    TableScanDetail remote_table_scan_detail{false};

protected:
    void appendExtraJson(FmtBuffer &) const override;
    void collectExtraRuntimeDetail() override;

private:
    void updateTableScanDetail(const std::vector<ConnectionProfileInfo> & connection_profile_infos);
};
} // namespace DB
