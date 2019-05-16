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

    TMTContext & tmt = context.getTMTContext();
    auto storage = tmt.storages.getByName(database_name, table_name);
    if (storage == nullptr)
    {
        throw Exception("Table " + database_name + "." + table_name + " doesn't not exist", ErrorCodes::UNKNOWN_TABLE);
    }
    auto merge_tree = std::dynamic_pointer_cast<StorageMergeTree>(storage);
    auto schema_syncer = tmt.getSchemaSyncer();
    schema_syncer->syncSchema(merge_tree->getTableInfo().id, context, true);

    std::stringstream ss;
    ss << "refreshed schema for table #" << merge_tree->getTableInfo().id;
    output(ss.str());
}

} // namespace DB
