#pragma once

#include <Debug/DBGInvoker.h>

namespace DB
{

struct DbgFailPointFunc
{

    static void dbgEnableFailPoint(Context & context, const ASTs & args, DBGInvoker::Printer output);

    static void dbgInitFailPoint(Context & context, const ASTs & args, DBGInvoker::Printer output);
};

} // namespace DB
