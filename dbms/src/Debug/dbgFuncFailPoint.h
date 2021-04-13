#pragma once

#include <Debug/DBGInvoker.h>

namespace DB
{

struct DbgFailPointFunc
{

    // Init fail point. must be called if you want to enable / disable failpoints
    //    DBGInvoke init_fail_point()
    static void dbgInitFailPoint(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Enable fail point.
    //    DBGInvoke enable_fail_point(name)
    static void dbgEnableFailPoint(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Disable fail point.
    // Usage:
    //   ./stoage-client.sh "DBGInvoke disable_fail_point(name)"
    //     name == "*" means disable all fail point
    static void dbgDisableFailPoint(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Wait till the fail point is disabled.
    //    DBGInvoke wait_fail_point(name)
    static void dbgWaitFailPoint(Context & context, const ASTs & args, DBGInvoker::Printer output);
};

} // namespace DB
