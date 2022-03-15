#pragma once

#include <Flash/Coprocessor/DAGContext.h>

#include <vector>

namespace DB
{
/// TiDBTableScan is a wrap to hide the difference of `TableScan` and `PartitionTableScan`
class TiDBTableScan
{
public:
    TiDBTableScan(const tipb::Executor * table_scan_, const DAGContext & dag_context);
    bool isPartitionTableScan() const
    {
        return is_partition_table_scan;
    }
    Int64 getColumnSize() const
    {
        return columns.size();
    }
    const google::protobuf::RepeatedPtrField<tipb::ColumnInfo> & getColumns() const
    {
        return columns;
    }
    void constructTableScanForRemoteRead(tipb::TableScan * tipb_table_scan, TableID table_id) const;
    Int64 getLogicalTableID() const
    {
        return logical_table_id;
    }
    const std::vector<Int64> & getPhysicalTableIDs() const
    {
        return physical_table_ids;
    }
    String getTableScanExecutorID() const
    {
        return table_scan->executor_id();
    }

private:
    const tipb::Executor * table_scan;
    bool is_partition_table_scan;
    const google::protobuf::RepeatedPtrField<tipb::ColumnInfo> & columns;
    /// logical_table_id is the table id for a TiDB' table, while if the
    /// TiDB table is partition, each partition is a physical table, and
    /// the partition's table id is the physical table id.
    /// So, for non-partition table, physical_table_ids.size() == 1, and
    /// physical_table_ids[0] == logical_table_id,
    /// for partition table, logical_table_id is the partition table id,
    /// physical_table_ids contains the table ids of its partitions
    std::vector<Int64> physical_table_ids;
    Int64 logical_table_id;
};

} // namespace DB
