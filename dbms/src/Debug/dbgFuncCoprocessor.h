#pragma once

#include <Debug/DBGInvoker.h>
#include <Parsers/IAST.h>

namespace DB
{

class Context;

// Coprocessor debug tools

// Run a DAG request using given query (will be compiled to DAG executors), region ID and start ts.
// Usage:
//   ./storages-client.sh "DBGInvoke dag(query, region_id, start_ts)"
BlockInputStreamPtr dbgFuncDAG(Context & context, const ASTs & args);

} // namespace DB
