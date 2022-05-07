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

#include <Common/FmtUtils.h>
#include <Flash/Coprocessor/RemoteRequest.h>
#include <Storages/MutableSupport.h>

namespace DB
{
RemoteRequest RemoteRequest::build(
    const RegionRetryList & retry_regions,
    DAGContext & dag_context,
    const TiDBTableScan & table_scan,
    const TiDB::TableInfo & table_info,
    const PushDownFilter & push_down_filter,
    const LoggerPtr & log)
{
    auto print_retry_regions = [&retry_regions, &table_info] {
        FmtBuffer buffer;
        buffer.fmtAppend("Start to build remote request for {} regions (", retry_regions.size());
        buffer.joinStr(
            retry_regions.cbegin(),
            retry_regions.cend(),
            [](const auto & r, FmtBuffer & fb) { fb.fmtAppend("{}", r.get().region_id); },
            ",");
        buffer.fmtAppend(") for table {}", table_info.id);
        return buffer.toString();
    };
    LOG_FMT_INFO(log, "{}", print_retry_regions());

    DAGSchema schema;
    tipb::DAGRequest dag_req;
    auto * executor = push_down_filter.constructSelectionForRemoteRead(dag_req.mutable_root_executor());

    {
        tipb::Executor * ts_exec = executor;
        ts_exec->set_tp(tipb::ExecType::TypeTableScan);
        ts_exec->set_executor_id(table_scan.getTableScanExecutorID());
        auto * mutable_table_scan = ts_exec->mutable_tbl_scan();
        table_scan.constructTableScanForRemoteRead(mutable_table_scan, table_info.id);

        String handle_column_name = MutableSupport::tidb_pk_column_name;
        if (auto pk_handle_col = table_info.getPKHandleColumn())
            handle_column_name = pk_handle_col->get().name;

        for (int i = 0; i < table_scan.getColumnSize(); ++i)
        {
            const auto & col = table_scan.getColumns()[i];
            auto col_id = col.column_id();

            if (col_id == DB::TiDBPkColumnID)
            {
                ColumnInfo ci;
                ci.tp = TiDB::TypeLongLong;
                ci.setPriKeyFlag();
                ci.setNotNullFlag();
                schema.emplace_back(std::make_pair(handle_column_name, std::move(ci)));
            }
            else if (col_id == ExtraTableIDColumnID)
            {
                ColumnInfo ci;
                ci.tp = TiDB::TypeLongLong;
                schema.emplace_back(std::make_pair(MutableSupport::extra_table_id_column_name, std::move(ci)));
            }
            else
            {
                const auto & col_info = table_info.getColumnInfo(col_id);
                schema.emplace_back(std::make_pair(col_info.name, col_info));
            }
            dag_req.add_output_offsets(i);
        }
        dag_req.set_encode_type(tipb::EncodeType::TypeCHBlock);
        dag_req.set_force_encode_type(true);
    }
    /// do not collect execution summaries because in this case because the execution summaries
    /// will be collected by CoprocessorBlockInputStream
    dag_req.set_collect_execution_summaries(false);
    const auto & original_dag_req = *dag_context.dag_request;
    if (original_dag_req.has_time_zone_name() && !original_dag_req.time_zone_name().empty())
        dag_req.set_time_zone_name(original_dag_req.time_zone_name());
    if (original_dag_req.has_time_zone_offset())
        dag_req.set_time_zone_offset(original_dag_req.time_zone_offset());
    std::vector<pingcap::coprocessor::KeyRange> key_ranges;
    for (const auto & region : retry_regions)
    {
        for (const auto & range : region.get().key_ranges)
            key_ranges.emplace_back(*range.first, *range.second);
    }
    sort(key_ranges.begin(), key_ranges.end());
    return {std::move(dag_req), std::move(schema), std::move(key_ranges)};
}
} // namespace DB
