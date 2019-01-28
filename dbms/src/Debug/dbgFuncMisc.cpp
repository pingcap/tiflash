#include <Common/typeid_cast.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/Transaction/TMTContext.h>
#include <Debug/dbgFuncMisc.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

void dbgFuncEnableHistoryGc(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 2)
    {
        throw Exception("Args not matched, should be: enable-history-gc(true/false)",
            ErrorCodes::BAD_ARGUMENTS);
    }

    bool history_gc = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value) == "true";

    auto & tmt = context.getTMTContext();
    tmt.enableDataHistoryVersionGc(history_gc);

    std::stringstream ss;
    ss << "history version gc on merging: " << (history_gc ? "enabled" : "disabled");
    output(ss.str());
}

void dbgFuncShowEnableHistoryGc(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 0)
    {
        throw Exception("Args not matched, should be empty",
            ErrorCodes::BAD_ARGUMENTS);
    }

    auto & tmt = context.getTMTContext();
    bool history_gc = tmt.isEnabledDataHistoryVersionGc();

    std::stringstream ss;
    ss << "history version gc on merging: " << (history_gc ? "enabled" : "disabled");
    output(ss.str());
}

}
