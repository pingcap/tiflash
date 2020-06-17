#pragma once

#include <Debug/DBGInvoker.h>

namespace DB
{

struct DbgFailPointFunc
{

    static void dbgEnableFailPoint(Context & context, const ASTs & args, DBGInvoker::Printer output);

    static void dbgInitFailPoint(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Disable fail point.
    // Usage:
    //   ./stoage-client.sh "DBGInvoke disable_fail_point(name)"
    //     name == "*" means disable all fail point
    static void dbgDisableFailPoint(Context & context, const ASTs & args, DBGInvoker::Printer output);
};

} // namespace DB
