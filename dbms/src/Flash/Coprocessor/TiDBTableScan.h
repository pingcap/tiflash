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

#include <Storages/KVStore/Types.h>
#include <TiDB/Schema/TiDB.h>
#include <common/types.h>
#include <tipb/executor.pb.h>

namespace DB
{
class DAGContext;

/// TiDBTableScan is a wrap to hide the difference of `TableScan` and `PartitionTableScan`
class TiDBTableScan
{
public:
    TiDBTableScan(const tipb::Executor * table_scan_, const String & executor_id_, const DAGContext & dag_context);
    bool isPartitionTableScan() const { return is_partition_table_scan; }
    Int64 getColumnSize() const { return columns.size(); }
    const TiDB::ColumnInfos & getColumns() const { return columns; }
    void constructTableScanForRemoteRead(tipb::TableScan * tipb_table_scan, TableID table_id) const;
    Int64 getLogicalTableID() const { return logical_table_id; }
    const std::vector<Int64> & getPhysicalTableIDs() const { return physical_table_ids; }
    const String & getTableScanExecutorID() const { return executor_id; }
    bool keepOrder() const { return keep_order; }

    bool isFastScan() const { return is_fast_scan; }

    const tipb::Executor * getTableScanPB() const { return table_scan; }
    const std::vector<Int32> & getRuntimeFilterIDs() const { return runtime_filter_ids; }
    int getMaxWaitTimeMs() const { return max_wait_time_ms; }

    const google::protobuf::RepeatedPtrField<tipb::Expr> & getPushedDownFilters() const { return pushed_down_filters; }

    const tipb::ANNQueryInfo & getANNQueryInfo() const { return ann_query_info; }

private:
    const tipb::Executor * table_scan;
    String executor_id;
    bool is_partition_table_scan;
    const TiDB::ColumnInfos columns;
    /// logical_table_id is the table id for a TiDB' table, while if the
    /// TiDB table is partition, each partition is a physical table, and
    /// the partition's table id is the physical table id.
    /// So, for non-partition table, physical_table_ids.size() == 1, and
    /// physical_table_ids[0] == logical_table_id,
    /// for partition table, logical_table_id is the partition table id,
    /// physical_table_ids contains the table ids of its partitions
    std::vector<Int64> physical_table_ids;
    Int64 logical_table_id;

    /// pushed_down_filter_conditions is the filter conditions that are
    /// pushed down to table scan by late materialization.
    /// They will be executed on Storage layer.
    const google::protobuf::RepeatedPtrField<tipb::Expr> pushed_down_filters;

    const tipb::ANNQueryInfo ann_query_info;

    bool keep_order;
    bool is_fast_scan;
    std::vector<Int32> runtime_filter_ids;
    int max_wait_time_ms;
};

} // namespace DB
