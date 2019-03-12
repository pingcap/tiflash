#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/Context.h>

#include <Debug/DBGInvoker.h>

namespace DB
{

// Region manipulation tools

// Inject a region and optionally map it to a table.
// Usage:
//   ./storages-client.sh "DBGInvoke put_region(region_id, start, end, database_name, table_name)"
void dbgFuncPutRegion(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Simulate a region snapshot raft command
// Usage:
//   ./storages-client.sh "DBGInvoke region_snapshot(region_id, start, end, database_name, table_name)"
void dbgFuncRegionSnapshot(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Dump region-partition relationship
// Usage:
//   ./storage-client.sh "DBGInvoke dump_region_partition()"
void dbgFuncRegion(Context& context, const ASTs& args, DBGInvoker::Printer output);

// Remove region's data from partition
// Usage:
//   ./storage-client.sh "DBGInvoke rm_region_data(region_id)"
void dbgFuncRegionRmData(Context & context, const ASTs & args, DBGInvoker::Printer output);

/*
// Check each partition: rows == sum(regions rows)
// Usage:
//   ./storage-client.sh "DBGInvoke check_partition_region(database_name, table_name)"
void dbgFuncCheckPartitionRegionRows(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Scan rows not in the correct partitions
// Usage:
//   ./storage-client.sh "DBGInvoke scan_partition_extra_rows(database_name, table_name)"
void dbgFuncScanPartitionExtraRows(Context & context, const ASTs & args, DBGInvoker::Printer output);

// check region correct
// Usage:
//   ./storage-client.sh "DBGInvoke check_region_correct(database_name, table_name)"
void dbgFuncCheckRegionCorrect(Context & context, const ASTs & args, DBGInvoker::Printer output);
*/

}
