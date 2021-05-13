#pragma once

#include <Debug/DBGInvoker.h>

namespace DB
{

class Context;

// Find the last occurence of `key` in log file and extract the first number follow the key.
// Usage:
//   ./storage-client.sh "DBGInvoke search_log_for_key(key)"
void dbgFuncSearchLogForKey(Context & context, const ASTs & args, DBGInvoker::Printer output);

} // namespace DB
