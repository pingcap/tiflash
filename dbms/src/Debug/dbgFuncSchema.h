#pragma once

#include <Debug/DBGInvoker.h>
#include <Parsers/IAST.h>

namespace DB
{

class Context;

// Refresh schema of the given table.
// Usage:
//   ./storage-client.sh "DBGInvoke refresh_schema(database_name, table_name)"
void dbgFuncRefreshSchema(Context & context, const ASTs & args, DBGInvoker::Printer output);

} // namespace DB
