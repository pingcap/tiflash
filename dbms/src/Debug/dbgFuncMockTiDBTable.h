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

// TiDB table test tool
struct MockTiDBTable
{
    // Inject mocked TiDB table.
    // Usage:

    //   ./storages-client.sh "DBGInvoke mock_tidb_table(database_name, table_name, 'col1 type1, col2 type2, ...' [, handle_pk_name])"
    //   engine: [tmt, dt], tmt by default
    static void dbgFuncMockTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Inject mocked TiDB table.
    // Usage:
    //   ./storages-client.sh "DBGInvoke mock_tidb_db(database_name)
    static void dbgFuncMockTiDBDB(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Inject a partition into mocked TiDB table.
    // Usage:
    //   ./storages-client.sh "DBGInvoke mock_tidb_partition(database_name, table_name, partition_id [, is_add_part])"
    static void dbgFuncMockTiDBPartition(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Inject a partition into mocked TiDB table.
    // Usage:
    //   ./storages-client.sh "DBGInvoke drop_tidb_partition(database_name, table_name, partition_id)"
    static void dbgFuncDropTiDBPartition(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Drop a mocked TiDB table.
    // Usage:
    //   ./storages-client.sh "DBGInvoke drop_tidb_table(database_name, table_name)"
    static void dbgFuncDropTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Drop a mocked TiDB table.
    // Usage:
    //   ./storages-client.sh "DBGInvoke drop_tidb_db(database_name)"
    static void dbgFuncDropTiDBDB(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Add a column to a mocked TiDB table.
    // Usage:
    //   ./storages-client.sh "DBGInvoke add_column_to_tidb_table(database_name, table_name, 'col type')"
    static void dbgFuncAddColumnToTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Drop a column from a mocked TiDB table.
    // Usage:
    //   ./storages-client.sh "DBGInvoke drop_column_from_tidb_table(database_name, table_name, column_name)"
    static void dbgFuncDropColumnFromTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Modify a column's type in a mocked TiDB table.
    // Usage:
    //   ./storages-client.sh "DBGInvoke modify_column_in_tidb_table(database_name, table_name, 'col type')"
    static void dbgFuncModifyColumnInTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Rename a column in a mocked TiDB table.
    // Usage:
    //   ./storages-client.sh "DBGInvoke rename_column_in_tidb_table(database_name, table_name, old_col_name, new_col_name)"
    static void dbgFuncRenameColumnInTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Rename a TiDB table.
    // Usage:
    //   ./storages-client.sh "DBGInvoke rename_tidb_table(database_name, table_name, new_table)"
    static void dbgFuncRenameTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Truncate a TiDB table.
    // Usage:
    //   ./storages-client.sh "DBGInvoke truncate_tidb_table(database_name, table_name)"
    static void dbgFuncTruncateTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Reset Schema Syncer.
    // Usage:
    //   ./storages-client.sh "DBGInvoke reset_syncer()"
    static void dbgFuncResetSyncer(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Remove All Region.
    // Usage:
    //   ./storages-client.sh "DBGInvoke clean_up_region()"
    static void dbgFuncCleanUpRegions(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Trigger a create tables ddl job.
    // Usage:
    //   ./storage-client.sh "DBGInvoke create_tidb_tables(db_name, table_name, [table_name], ..., [table_name])"
    static void dbgFuncCreateTiDBTables(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Trigger a rename tables ddl job.
    // Usage:
    //   ./storage-client.sh "DBGInvoke rename_tidb_tables(database-name, table-name, new-table-name, ..., [database-name, table-name, new-table-name])"
    static void dbgFuncRenameTiDBTables(Context & /*context*/, const ASTs & args, DBGInvoker::Printer output);

private:
    static void dbgFuncDropTiDBTableImpl(
        Context & context,
        String database_name,
        String table_name,
        bool drop_regions,
        bool is_drop_db,
        DBGInvoker::Printer output);
};

} // namespace DB
