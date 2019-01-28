#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/Context.h>

#include <Debug/DBGInvoker.h>

namespace DB
{

// Change whether do data history verson GC on merging
// Usage:
//   ./storages-client.sh "DBGInvoke enable_history_gc(enable-history-gc, use-final-on-merging)"
void dbgFuncEnableHistoryGc(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Show whether do data history verson GC on merging
// Usage:
//   ./storages-client.sh "DBGInvoke show_enable_history_gc(enable-history-gc, use-final-on-merging)"
void dbgFuncShowEnableHistoryGc(Context & context, const ASTs & args, DBGInvoker::Printer output);
}
