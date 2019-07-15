#pragma once

#include <Debug/DBGInvoker.h>
#include <Parsers/IAST.h>

namespace DB
{

class Context;

// Change whether to mock schema syncer.
// Usage:
//   ./storages-client.sh "DBGInvoke mock_schema_syncer(enabled)"
void dbgFuncMockSchemaSyncer(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Refresh schema of the given table.
// Usage:
//   ./storage-client.sh "DBGInvoke refresh_schema(database_name, table_name)"
void dbgFuncRefreshSchema(Context & context, const ASTs & args, DBGInvoker::Printer output);

} // namespace DB
