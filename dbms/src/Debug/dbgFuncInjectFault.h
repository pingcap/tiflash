#pragma once

#include <Debug/DBGInvoker.h>

namespace DB
{

struct FaultInject
{

    static void dbgFaultInjectEnable(Context & context, const ASTs & args, DBGInvoker::Printer output);

    static void dbgFaultInjectInit(Context & context, const ASTs & args, DBGInvoker::Printer output);
};

} // namespace DB
