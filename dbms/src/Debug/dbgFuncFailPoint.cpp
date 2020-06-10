#include <Common/FailPoint.h>
#include <Common/typeid_cast.h>
#include <Debug/dbgFuncFailPoint.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

void DbgFailPointFunc::dbgInitFailPoint(Context &, const ASTs &, DBGInvoker::Printer) { (void)fiu_init(0); }

void DbgFailPointFunc::dbgEnableFailPoint(Context &, const ASTs & args, DBGInvoker::Printer)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: name", ErrorCodes::BAD_ARGUMENTS);

    const String fail_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    FailPointHelper::enableFailPoint(fail_name);
}

void DbgFailPointFunc::dbgDisableFailPoint(Context &, const ASTs & args, DBGInvoker::Printer)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: name", ErrorCodes::BAD_ARGUMENTS);

    const String fail_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    FailPointHelper::disableFailPoint(fail_name);
}

} // namespace DB
