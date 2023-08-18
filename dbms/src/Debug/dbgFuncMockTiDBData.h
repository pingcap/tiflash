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

// TiDB table data writing test tools

// Write one row of mocked TiDB data with raft command.
// Usage:
//   ./storages-client.sh "DBGInvoke raft_insert_row(database_name, table_name, region_id, handle_id, val1, val2, ...)"
void dbgFuncRaftInsertRow(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Write one row of mocked TiDB data with raft command.
// Usage:
//   ./storages-client.sh "DBGInvoke raft_insert_row_full(database_name, table_name, region_id, handle_id, tso, del, val1, val2, ...)"
void dbgFuncRaftInsertRowFull(Context & context, const ASTs & args, DBGInvoker::Printer output);

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

} // namespace DB
