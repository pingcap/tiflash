#include <Common/FailPoint.h>
#include <Common/typeid_cast.h>
#include <Debug/dbgFuncFailPoint.h>
#include <Parsers/ASTAsterisk.h>
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

    String fail_name;
    if (auto ast = typeid_cast<const ASTAsterisk *>(args[0].get()); ast != nullptr)
        fail_name = "*";
    else if (auto ast = typeid_cast<const ASTIdentifier *>(args[0].get()); ast != nullptr)
        fail_name = ast->name;
    else
        throw Exception("Can not parse arg[0], should be: name or *", ErrorCodes::BAD_ARGUMENTS);

    FailPointHelper::disableFailPoint(fail_name);
}

void DbgFailPointFunc::dbgWaitFailPoint(Context &, const ASTs & args, DBGInvoker::Printer)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: name", ErrorCodes::BAD_ARGUMENTS);

    const String fail_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    // Wait for the failpoint till it is disabled.
    // Return if it is already disabled.
    FAIL_POINT_PAUSE(fail_name.c_str());
}

} // namespace DB
