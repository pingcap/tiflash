#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/registerStorages.h>


namespace DB
{
using namespace TiDB;

String getTiDBTableInfoJsonByCurl(TableID table_id, Context & context);
String createDatabaseStmt(const TableInfo & table_info);
String createTableStmt(const TableInfo & table_info);

}

int main(int, char ** args)
{
    using namespace DB;
    using namespace TiDB;

    auto context = Context::createGlobal();

    std::istringstream is(args[1]);
    TableID table_id;
    is >> table_id;

    String json_str = getTiDBTableInfoJsonByCurl(table_id, context);

    std::cout << json_str << std::endl;

    TableInfo table_info(json_str, false);

    String create_db_stmt = createDatabaseStmt(table_info);

    std::cout << create_db_stmt << std::endl;

    String create_table_stmt = createTableStmt(table_info);

    std::cout << create_table_stmt << std::endl;

    return 0;
}
