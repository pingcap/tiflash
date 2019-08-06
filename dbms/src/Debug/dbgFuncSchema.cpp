#include <Debug/MockSchemaSyncer.h>
#include <Debug/dbgFuncSchema.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/SchemaSyncService.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_TABLE;
} // namespace ErrorCodes

void dbgFuncEnableSchemaSyncService(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: enable (true/false)", ErrorCodes::BAD_ARGUMENTS);

    bool enable = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value) == "true";

    if (enable)
    {
        if (!context.getSchemaSyncService())
            context.initializeSchemaSyncService();
    }
    else
    {
        if (context.getSchemaSyncService())
            context.getSchemaSyncService().reset();
    }

    std::stringstream ss;
    ss << "schema sync service " << (enable ? "enabled" : "disabled");
    output(ss.str());
}

void dbgFuncMockSchemaSyncer(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: enable (true/false)", ErrorCodes::BAD_ARGUMENTS);

    bool enabled = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value) == "true";

    TMTContext & tmt = context.getTMTContext();

    static auto old_schema_syncer = tmt.getSchemaSyncer();
    if (enabled)
    {
        tmt.setSchemaSyncer(std::make_shared<MockSchemaSyncer>());
    }
    else
    {
        tmt.setSchemaSyncer(old_schema_syncer);
    }

    std::stringstream ss;
    ss << "mock schema syncer " << (enabled ? "enabled" : "disabled");
    output(ss.str());
}

void dbgFuncRefreshSchemas(Context & context, const ASTs &, DBGInvoker::Printer output)
{
    TMTContext & tmt = context.getTMTContext();
    auto schema_syncer = tmt.getSchemaSyncer();
    schema_syncer->syncSchemas(context);

    std::stringstream ss;
    ss << "schemas refreshed";
    output(ss.str());
}

} // namespace DB
