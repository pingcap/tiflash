#pragma once

#include <Debug/DBGInvoker.h>
#include <Parsers/IAST.h>

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

namespace Debug
{
void setServiceAddr(const std::string & addr);
}

} // namespace DB
