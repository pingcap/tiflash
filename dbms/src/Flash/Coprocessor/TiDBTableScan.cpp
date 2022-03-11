#include <Flash/Coprocessor/TiDBTableScan.h>

namespace DB
{
TiDBTableScan::TiDBTableScan(const tipb::Executor * table_scan_, const DAGContext & dag_context)
    : table_scan(table_scan_)
    , is_partition_table_scan(table_scan->tp() == tipb::TypePartitionTableScan)
    , columns(is_partition_table_scan ? table_scan->partition_table_scan().columns() : table_scan->tbl_scan().columns())
{
    if (is_partition_table_scan)
    {
        if (table_scan->partition_table_scan().has_table_id())
            logical_table_id = table_scan->partition_table_scan().table_id();
        else
            throw TiFlashException("Partition table scan without table id.", Errors::Coprocessor::BadRequest);
        for (const auto & partition_table_id : table_scan->partition_table_scan().partition_ids())
        {
            if (dag_context.containsRegionsInfoForTable(partition_table_id))
                physical_table_ids.push_back(partition_table_id);
        }
        std::sort(physical_table_ids.begin(), physical_table_ids.end());
        if (physical_table_ids.size() != dag_context.tables_regions_info.tableCount())
            throw TiFlashException("Partition table scan contains table_region_info that is not belongs to the partition table.", Errors::Coprocessor::BadRequest);
    }
    else
    {
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
        tipb_table_scan->set_desc(partition_table_scan.desc());
        for (auto id : partition_table_scan.primary_column_ids())
            tipb_table_scan->add_primary_column_ids(id);
        tipb_table_scan->set_next_read_engine(tipb::EngineType::Local);
        for (auto id : partition_table_scan.primary_prefix_column_ids())
            tipb_table_scan->add_primary_prefix_column_ids(id);
    }
    else
    {
        *tipb_table_scan = table_scan->tbl_scan();
        tipb_table_scan->set_table_id(table_id);
    }
}
} // namespace DB
