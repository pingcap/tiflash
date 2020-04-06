#pragma once

#include <Debug/DBGInvoker.h>
#include <Parsers/IAST.h>

namespace DB
{

class Context;

// Schema name mapping tools.

// Get the mapped underlying database name of a TiDB database.
// Usage:
//   ./storage-client.sh "DBGInvoke mapped_database(database_name)"
void dbgFuncMappedDatabase(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Get the mapped underlying table name of a TiDB table.
// Usage:
//   ./storage-client.sh "DBGInvoke mapped_table(database_name, table_name[, qualify = 'true'])"
void dbgFuncMappedTable(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Run query using mapped table name. Use place holder $d and $t to specify database name and table name in query.
// So far at most one database name and table name is supported.
// Usage:
//   ./storage-client.sh "DBGInvoke query_mapped('select * from $d.$t', database_name[, table_name])"
BlockInputStreamPtr dbgFuncQueryMapped(Context & context, const ASTs & args);

} // namespace DB
