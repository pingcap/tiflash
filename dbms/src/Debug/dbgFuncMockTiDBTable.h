#pragma once

#include <Debug/DBGInvoker.h>
#include <Parsers/IAST.h>

namespace DB
{

class Context;

// TiDB table test tool
struct MockTiDBTable
{
    // Change whether to mock schema syncer.
    // Usage:
    //   ./storages-client.sh "DBGInvoke mock_schema_syncer(enabled)"
    static void dbgFuncMockSchemaSyncer(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Inject mocked TiDB table.
    // Usage:
    //   ./storages-client.sh "DBGInvoke mock_tidb_table(database_name, table_name, 'col1 type1, col2 type2, ...')"
    static void dbgFuncMockTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Inject a partition into mocked TiDB table.
    // Usage:
    //   ./storages-client.sh "DBGInvoke mock_tidb_partition(database_name, table_name, partition_name)"
    static void dbgFuncMockTiDBPartition(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Rename the physical table of a partition of a TiDB partition table.
    // The physical table of a partition is named as table-name + '_' + partition-id, which is invisible by tests.
    // Rename to expose it to tests to making query to.
    // Usage:
    //   ./storages-client.sh "DBGInvoke rename_table_for_partition(database_name, table_name, partition_name, new_name)"
    static void dbgFuncRenameTableForPartition(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Drop a mocked TiDB table.
    // Usage:
    //   ./storages-client.sh "DBGInvoke drop_tidb_table(database_name, table_name)"
    static void dbgFuncDropTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output);

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
};

} // namespace DB
