#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/Context.h>

#include <Debug/DBGInvoker.h>

namespace DB
{

// TiDB table test tool

// Change whether to mock schema syncer
// Usage:
//   ./storages-client.sh "DBGInvoke mock_schema_syncer(enabled)"
void dbgFuncMockSchemaSyncer(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Inject mocked TiDB table.
// Usage:
//   ./storages-client.sh "DBGInvoke mock_tidb_table(database_name, table_name, 'col1 type1, col2 type2, ...')"
void dbgFuncMockTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Inject mocked TiDB table.
// Usage:
//   ./storages-client.sh "DBGInvoke mock_tidb_partition(database_name, table_name, partition_name)"
void dbgFuncMockTiDBPartition(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Drop a mocked TiDB table.
// Usage:
//   ./storages-client.sh "DBGInvoke drop_tidb_table(database_name, table_name)"
void dbgFuncDropTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output);

}
