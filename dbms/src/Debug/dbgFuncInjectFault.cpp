#include <Debug/dbgFuncInjectFault.h>
#include <Parsers/ASTIdentifier.h>
#include <fiu-control.h>
#include <fiu.h>

namespace DB
{

void FaultInject::dbgFaultInjectInit(Context &, const ASTs &, DBGInvoker::Printer) { fiu_init(0); }

void FaultInject::dbgFaultInjectEnable(Context &, const ASTs & args, DBGInvoker::Printer)
{
    const String & fail_name = static_cast<const ASTIdentifier &>(*args[0]).name;
    fiu_enable(fail_name.data(), 1, nullptr, 0);
}

} // namespace DB