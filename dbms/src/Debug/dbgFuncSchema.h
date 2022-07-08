// Copyright 2022 PingCAP, Ltd.
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

// Enable/disable schema sync service.
// Usage:
//   ./storages-client.sh "DBGInvoke enable_schema_sync_service(enable)"
void dbgFuncEnableSchemaSyncService(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Refresh schemas for all tables.
// Usage:
//   ./storage-client.sh "DBGInvoke refresh_schemas()"
void dbgFuncRefreshSchemas(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Trigger gc on all databases / tables.
// Usage:
//   ./storage-client.sh "DBGInvoke gc_schemas([gc_safe_point])"
void dbgFuncGcSchemas(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Reset schemas.
// Usage:
//   ./storages-client.sh "DBGInvoke reset_schemas()"
void dbgFuncResetSchemas(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Check if table is tombstone.
// Usage:
//   ./storage-client.sh "DBGInvoke is_tombstone(db_name, table_name)"
void dbgFuncIsTombstone(Context & context, const ASTs & args, DBGInvoker::Printer output);
} // namespace DB
