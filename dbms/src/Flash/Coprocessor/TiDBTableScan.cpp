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

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/TiDBTableScan.h>

namespace DB
{
TiDBTableScan::TiDBTableScan(
    const tipb::Executor * table_scan_,
    const String & executor_id_,
    const DAGContext & dag_context)
    : table_scan(table_scan_)
    , executor_id(executor_id_)
    , is_partition_table_scan(table_scan->tp() == tipb::TypePartitionTableScan)
    , columns(
          is_partition_table_scan ? std::move(TiDB::toTiDBColumnInfos(table_scan->partition_table_scan().columns()))
                                  : std::move(TiDB::toTiDBColumnInfos(table_scan->tbl_scan().columns())))
    , pushed_down_filters(
          is_partition_table_scan ? table_scan->partition_table_scan().pushed_down_filter_conditions()
                                  : table_scan->tbl_scan().pushed_down_filter_conditions())
    , ann_query_info(
          is_partition_table_scan ? table_scan->partition_table_scan().ann_query() : table_scan->tbl_scan().ann_query())
    // Only No-partition table need keep order when tablescan executor required keep order.
    // If keep_order is not set, keep order for safety.
    , keep_order(
          !is_partition_table_scan && (table_scan->tbl_scan().keep_order() || !table_scan->tbl_scan().has_keep_order()))
    , is_fast_scan(
          is_partition_table_scan ? table_scan->partition_table_scan().is_fast_scan()
                                  : table_scan->tbl_scan().is_fast_scan())
{
    RUNTIME_CHECK_MSG(
        !keep_order || pushed_down_filters.empty(),
        "Bad TiDB table scan executor: push down filter is not empty when keep order is true");

    if (is_partition_table_scan)
    {
        for (const auto & rf_pb : table_scan->partition_table_scan().runtime_filter_list())
        {
            runtime_filter_ids.push_back(rf_pb.id());
        }
        max_wait_time_ms = table_scan->partition_table_scan().max_wait_time_ms();
        if (table_scan->partition_table_scan().has_table_id())
            logical_table_id = table_scan->partition_table_scan().table_id();
        else
            throw TiFlashException("Partition table scan without table id.", Errors::Coprocessor::BadRequest);
        std::set<Int64> all_physical_table_ids;
        for (const auto & partition_table_id : table_scan->partition_table_scan().partition_ids())
        {
            if (all_physical_table_ids.count(partition_table_id) > 0)
                throw TiFlashException(
                    "Partition table scan contains duplicated physical table ids.",
                    Errors::Coprocessor::BadRequest);
            all_physical_table_ids.insert(partition_table_id);
            if (dag_context.containsRegionsInfoForTable(partition_table_id))
                physical_table_ids.push_back(partition_table_id);
        }
        std::sort(physical_table_ids.begin(), physical_table_ids.end());
        if (physical_table_ids.size() != dag_context.tables_regions_info.tableCount())
            throw TiFlashException(
                "Partition table scan contains table_region_info that is not belongs to the partition table.",
                Errors::Coprocessor::BadRequest);
    }
    else
    {
        for (const auto & rf_pb : table_scan->tbl_scan().runtime_filter_list())
        {
            runtime_filter_ids.push_back(rf_pb.id());
        }
        max_wait_time_ms = table_scan->tbl_scan().max_wait_time_ms();
        if (table_scan->tbl_scan().next_read_engine() != tipb::EngineType::Local)
            throw TiFlashException("Unsupported remote query.", Errors::Coprocessor::BadRequest);

        if (table_scan->tbl_scan().has_table_id())
            logical_table_id = table_scan->tbl_scan().table_id();
        else
            throw TiFlashException("table scan without table id.", Errors::Coprocessor::BadRequest);
        physical_table_ids.push_back(logical_table_id);
    }
}
void TiDBTableScan::constructTableScanForRemoteRead(tipb::TableScan * tipb_table_scan, TableID table_id) const
{
    if (is_partition_table_scan)
    {
        const auto & partition_table_scan = table_scan->partition_table_scan();
        tipb_table_scan->set_table_id(table_id);
        for (const auto & column : partition_table_scan.columns())
            *tipb_table_scan->add_columns() = column;
        for (const auto & filter : partition_table_scan.pushed_down_filter_conditions())
            *tipb_table_scan->add_pushed_down_filter_conditions() = filter;
        tipb_table_scan->set_desc(partition_table_scan.desc());
        for (auto id : partition_table_scan.primary_column_ids())
            tipb_table_scan->add_primary_column_ids(id);
        tipb_table_scan->set_next_read_engine(tipb::EngineType::Local);
        for (auto id : partition_table_scan.primary_prefix_column_ids())
            tipb_table_scan->add_primary_prefix_column_ids(id);
        tipb_table_scan->set_is_fast_scan(partition_table_scan.is_fast_scan());
        tipb_table_scan->set_keep_order(false);

        if (partition_table_scan.has_ann_query())
            tipb_table_scan->mutable_ann_query()->CopyFrom(partition_table_scan.ann_query());
    }
    else
    {
        *tipb_table_scan = table_scan->tbl_scan();
        tipb_table_scan->set_table_id(table_id);
    }
}
} // namespace DB
