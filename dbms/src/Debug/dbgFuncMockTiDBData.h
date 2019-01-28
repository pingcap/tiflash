#pragma once

#include <Interpreters/Context.h>
#include <Parsers/IAST.h>
#include <Debug/DBGInvoker.h>

namespace DB
{

// TiDB table data writing test tools

// Change flush threshold rows
// Usage:
//   ./storages-client.sh "DBGInvoke set_flush_rows(threshold_rows)"
void dbgFuncSetFlushRows(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Change flush deadline seconds
// Usage:
//   ./storages-client.sh "DBGInvoke set_deadline_seconds(seconds)"
void dbgFuncSetDeadlineSeconds(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Write one row of mocked TiDB data with raft command.
// Usage:
//   ./storages-client.sh "DBGInvoke raft_insert_row(database_name, table_name, region_id, handle_id, val1, val2, ...)"
void dbgFuncRaftInsertRow(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Write batch of rows into mocked TiDB with raft command.
// Usage:
//   ./storages-client.sh "DBGInvoke raft_insert_rows(database_name, table_name, thread_num, flush_num, batch_num, min_strlen, max_strlen)"
// Each thread will write thread_num * flush_num rows into new region.
void dbgFuncRaftInsertRows(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Delete one row of mocked TiDB data with raft command.
// Usage:
//   ./storages-client.sh "DBGInvoke raft_delete_row(database_name, table_name, region_id, handle_id)"
void dbgFuncRaftDeleteRow(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Update rows with handle between [start_handle, end_handle).
// Usage:
//   ./storages-client.sh "DBGInvoke raft_update_rows(database_name, table_name, start_handle, end_handle, magic_num)"
void dbgFuncRaftUpdateRows(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Delete rows with handle between [start_handle, end_handle).
// Usage:
//   ./storages-client.sh "DBGInvoke raft_delete_rows(database_name, table_name, start_handle, end_handle)"
void dbgFuncRaftDelRows(Context & context, const ASTs & args, DBGInvoker::Printer output);

}
