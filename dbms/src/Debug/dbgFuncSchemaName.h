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

// Check the mapped underlying table name of a TiDB table exists or not.
// Usage:
//   ./storage-client.sh "DBGInvoke mapped_table_exists(database_name, table_name)"
void dbgFuncTableExists(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Check the mapped underlying database name of a TiDB db exists or not.
// Usage:
//   ./storage-client.sh "DBGInvoke mapped_database_exists(database_name, table_name)"
void dbgFuncDatabaseExists(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Run query using mapped table name. Use place holder $d and $t to specify database name and table name in query.
// So far at most one database name and table name is supported.
// Usage:
//   ./storage-client.sh "DBGInvoke query_mapped('select * from $d.$t', database_name[, table_name])"
BlockInputStreamPtr dbgFuncQueryMapped(Context & context, const ASTs & args);

// Get table's tiflash replica counts with mapped table name
// Usage:
//   ./storage-client.sh "DBGInvoke get_tiflash_replica_count(db_name, table_name)"
void dbgFuncGetTiflashReplicaCount(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Get the logical table's partition tables' tiflash replica counts with mapped table name
// Usage:
//   ./storage-client.sh "DBGInvoke get_partition_tables_tiflash_replica_count(db_name, table_name)"
void dbgFuncGetPartitionTablesTiflashReplicaCount(Context & context, const ASTs & args, DBGInvoker::Printer output);

} // namespace DB
