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
namespace DB
{
class Context;

// Coprocessor debug tools

// Run a DAG request using given query that will be compiled to DAG request, with the given (optional) region ID.
// Usage:
//   ./storages-client.sh "DBGInvoke dag(query[, region_id])"
BlockInputStreamPtr dbgFuncTiDBQuery(Context & context, const ASTs & args);

// Mock a DAG request using given query that will be compiled (with the metadata from MockTiDB) to DAG request, with the given region ID and (optional) start ts.
// Usage:
//   ./storages-client.sh "DBGInvoke mock_dag(query, region_id[, start_ts])"
BlockInputStreamPtr dbgFuncMockTiDBQuery(Context & context, const ASTs & args);

// Not used right now.
void dbgFuncTiDBQueryFromNaturalDag(Context & context, const ASTs & args, DBGInvoker::Printer output);

namespace Debug
{
void setServiceAddr(const std::string & addr);
}

} // namespace DB
