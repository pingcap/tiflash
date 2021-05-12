#pragma once

#include <Debug/DBGInvoker.h>

namespace DB
{

class Context;

// Get the current log file path of the tiflash instance.
// Usage:
//   ./storage-client.sh "DBGInvoke get_log_path()"
void dbgGetLogPath(Context & context, const ASTs &, DBGInvoker::Printer);

} // namespace DB
