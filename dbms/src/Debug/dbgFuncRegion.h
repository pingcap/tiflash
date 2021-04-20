#pragma once

#include <Debug/DBGInvoker.h>
#include <Parsers/IAST.h>

namespace DB
{

class Context;

// Region manipulation tools

// Inject a region and optionally map it to a table.
// Usage:
//   ./storages-client.sh "DBGInvoke put_region(region_id, start, end, database_name, table_name[, partition_id])"
void dbgFuncPutRegion(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Dump all region ranges for specific table
// Usage:
//   ./storage-client.sh "DBGInvoke dump_all_region(table_id)"
void dbgFuncDumpAllRegion(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Dump all region ranges for specific table
// Usage:
//   ./storage-client.sh "DBGInvoke dump_all_mock_region(table_id)"
void dbgFuncDumpAllMockRegion(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Try flush regions
// Usage:
//   ./storage-client.sh "DBGInvoke try_flush([force_flush])"
void dbgFuncTryFlush(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Try flush regions
// Usage:
//   ./storage-client.sh "DBGInvoke try_flush_region(database_name, table_name, region_id)"
void dbgFuncTryFlushRegion(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Remove region
// Usage:
//   ./storage-client.sh "DBGInvoke remove_region(region_id)"
void dbgFuncRemoveRegion(Context & context, const ASTs & args, DBGInvoker::Printer output);

} // namespace DB
