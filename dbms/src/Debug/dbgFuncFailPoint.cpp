#include <Common/FailPoint.h>
#include <Debug/dbgFuncFailPoint.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

void DbgFailPointFunc::dbgInitFailPoint(Context &, const ASTs &, DBGInvoker::Printer) { (void)fiu_init(0); }

void DbgFailPointFunc::dbgEnableFailPoint(Context &, const ASTs & args, DBGInvoker::Printer)
{
    const String & fail_name = static_cast<const ASTIdentifier &>(*args[0]).name;
    FailPointHelper::enableFailPoint(fail_name.data());
}

void DbgFailPointFunc::dbgDisableFailPoint(Context &, const ASTs & args, DBGInvoker::Printer)
{
    const String & fail_name = static_cast<const ASTIdentifier &>(*args[0]).name;
    FailPointHelper::disableFailPoint(fail_name.data());
}

} // namespace DB
