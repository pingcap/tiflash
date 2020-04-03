#pragma once

#include <Debug/DBGInvoker.h>
#include <Parsers/IAST.h>

namespace DB
{

class Context;

// Schema name reverse map tools.

// Reverse map database name.
// Usage:
//   ./storage-client.sh "DBGInvoke reverse_database(database_name)"
void dbgFuncReverseDatabase(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Reverse map table name.
// Usage:
//   ./storage-client.sh "DBGInvoke reverse_table(database_name, table_name)"
void dbgFuncReverseTable(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Run query using mapped table name. Use place holder $d and $t to specify database name and table name in query.
// So far at most one database name and table name is supported.
// Usage:
//   ./storage-client.sh "DBGInvoke reverse_query('select * from $d.$t'), database_name, table_name)"
BlockInputStreamPtr dbgFuncReverseQuery(Context & context, const ASTs & args);

} // namespace DB
