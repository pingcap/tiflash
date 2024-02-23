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

// Region manipulation tools

// Inject a region and optionally map it to a table.
// Usage:
//   ./storages-client.sh "DBGInvoke put_region(region_id, start, end, database_name, table_name[, partition_id])"
void dbgFuncPutRegion(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Dump all region ranges for specific table
// Usage:
//   ./storage-client.sh "DBGInvoke dump_all_region(table_id)"
void dbgFuncDumpAllRegion(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Dump all region ranges for specific table
// Usage:
//   ./storage-client.sh "DBGInvoke dump_all_mock_region(table_id)"
void dbgFuncDumpAllMockRegion(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Try flush regions
// Usage:
//   ./storage-client.sh "DBGInvoke try_flush_region(database_name, table_name, region_id)"
void dbgFuncTryFlushRegion(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Remove region
// Usage:
//   ./storage-client.sh "DBGInvoke remove_region(region_id)"
void dbgFuncRemoveRegion(Context & context, const ASTs & args, DBGInvoker::Printer output);

void dbgFuncFindRegionByRange(Context & context, const ASTs & args, DBGInvoker::Printer);

} // namespace DB
