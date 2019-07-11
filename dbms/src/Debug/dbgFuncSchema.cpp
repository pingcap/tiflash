#include <Debug/dbgFuncSchema.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_TABLE;
} // namespace ErrorCodes

void dbgFuncRefreshSchema(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 2)
        throw Exception("Args not matched, should be: database-name, table-name", ErrorCodes::BAD_ARGUMENTS);

    std::string database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    std::transform(database_name.begin(), database_name.end(), database_name.begin(), ::tolower);
    std::string table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    std::transform(table_name.begin(), table_name.end(), table_name.begin(), ::tolower);

    auto log = [&](TableID table_id) {
        std::stringstream ss;
        ss << "refreshed schema for table #" << table_id;
        output(ss.str());
    };

    TMTContext & tmt = context.getTMTContext();
    auto schema_syncer = tmt.getSchemaSyncer();
    TableID table_id = schema_syncer->getTableIdByName(database_name, table_name, context);
    auto storage = tmt.getStorages().getByName(database_name, table_name);

    if (storage == nullptr && table_id == InvalidTableID)
        // Table does not exist in CH nor TiDB, error out.
        throw Exception("Table " + database_name + "." + table_name + " doesn't exist in tidb", ErrorCodes::UNKNOWN_TABLE);

    if (storage == nullptr && table_id != InvalidTableID)
    {
        // Table does not exist in CH, but exists in TiDB.
        // Might be renamed or never synced.
        // Note there will be a dangling table in CH for the following scenario:
        // Table t was synced to CH already, then t was renamed (name changed) and truncated (ID changed).
        // Then this function was called with the new name given, the table will be synced to a new table.
        // User must manually call this function with the old name to remove the dangling table in CH.
        schema_syncer->syncSchema(table_id, context, true);

        log(table_id);

        return;
    }

    if (table_id == InvalidTableID)
    {
        // Table exists in CH, but does not exist in TiDB.
        // Just sync it using the storage's ID, syncer will then remove it.
        schema_syncer->syncSchema(storage->getTableInfo().id, context, true);

        log(table_id);

        return;
    }

    // Table exists in both CH and TiDB.
    if (table_id != storage->getTableInfo().id)
    {
        // Table in TiDB is not the old one, i.e. dropped/renamed then recreated.
        // Sync the old one in CH first, then sync the new one.
        schema_syncer->syncSchema(storage->getTableInfo().id, context, true);
        schema_syncer->syncSchema(table_id, context, true);

        log(table_id);

        return;
    }

    // Table in TiDB is the same one as in CH.
    // Just sync it.
    schema_syncer->syncSchema(table_id, context, true);

    log(table_id);
}

} // namespace DB
