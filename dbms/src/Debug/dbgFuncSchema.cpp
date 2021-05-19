#include <Databases/DatabaseTiFlash.h>
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

void dbgFuncRefreshSchemas(Context & context, const ASTs &, DBGInvoker::Printer output)
{
    TMTContext & tmt = context.getTMTContext();
    auto schema_syncer = tmt.getSchemaSyncer();
    schema_syncer->syncSchemas(context);

    std::stringstream ss;
    ss << "schemas refreshed";
    output(ss.str());
}

// Trigger gc on all databases / tables.
// Usage:
//   ./storage-client.sh "DBGInvoke gc_schemas([gc_safe_point])"
void dbgFuncGcSchemas(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    auto & service = context.getSchemaSyncService();
    Timestamp gc_safe_point = 0;
    if (args.size() == 0)
        gc_safe_point = PDClientHelper::getGCSafePointWithRetry(context.getTMTContext().getPDClient());
    else
        gc_safe_point = safeGet<Timestamp>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    service->gc(gc_safe_point);

    std::stringstream ss;
    ss << "schemas gc done";
    output(ss.str());
}

void dbgFuncResetSchemas(Context & context, const ASTs &, DBGInvoker::Printer output)
{
    TMTContext & tmt = context.getTMTContext();
    auto schema_syncer = tmt.getSchemaSyncer();
    schema_syncer->reset();

    std::stringstream ss;
    ss << "reset schemas";
    output(ss.str());
}

void dbgFuncIsTombstone(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() < 1 || args.size() > 2)
        throw Exception("Args not matched, should be: database-name[, table-name]", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    std::stringstream ss;
    if (args.size() == 1)
    {
        auto db = context.getDatabase(database_name);
        auto tiflash_db = std::dynamic_pointer_cast<DatabaseTiFlash>(db);
        if (!tiflash_db)
            throw Exception(database_name + " is not DatabaseTiFlash", ErrorCodes::BAD_ARGUMENTS);

        ss << (tiflash_db->isTombstone() ? "true" : "false");
    }
    else if (args.size() == 2)
    {

        const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
        auto storage = context.getTable(database_name, table_name);
        auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
        if (!managed_storage)
            throw Exception(database_name + "." + table_name + " is not ManageableStorage", ErrorCodes::BAD_ARGUMENTS);

        ss << (managed_storage->isTombstone() ? "true" : "false");
    }
    output(ss.str());
}

} // namespace DB
