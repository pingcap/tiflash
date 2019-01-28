#include <Parsers/ASTTruncateQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterTruncateQuery.h>
#include <Common/typeid_cast.h>

#include <Storages/StorageMergeTree.h>


namespace DB
{

BlockIO InterpreterTruncateQuery::execute()
{
    auto & truncate = typeid_cast<ASTTruncateQuery &>(*query_ptr);

    const String & table_name = truncate.table;
    String database_name = truncate.database.empty() ? context.getCurrentDatabase() : truncate.database;
    StoragePtr table = context.getTable(database_name, table_name);
    table->truncate(query_ptr, context);
    return {};
}

}
