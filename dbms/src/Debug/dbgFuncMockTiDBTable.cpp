#include <Debug/MockTiDB.h>
#include <Debug/dbgFuncMockTiDBTable.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_TABLE;
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

void MockTiDBTable::dbgFuncMockTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 3 && args.size() != 4 && args.size() != 5)
        throw Exception(
            "Args not matched, should be: database-name, table-name, schema-string [, handle_pk_name], [, engine-type(tmt|dt)]",
            ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    auto schema_str = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    String handle_pk_name = "";
    if (args.size() >= 4)
        handle_pk_name = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[3]).value);

    ASTPtr columns_ast;
    ParserColumnDeclarationList schema_parser;
    Tokens tokens(schema_str.data(), schema_str.data() + schema_str.length());
    TokenIterator pos(tokens);
    Expected expected;
    if (!schema_parser.parse(pos, columns_ast, expected))
        throw Exception("Invalid TiDB table schema", ErrorCodes::LOGICAL_ERROR);
    ColumnsDescription columns
        = InterpreterCreateQuery::getColumnsDescription(typeid_cast<const ASTExpressionList &>(*columns_ast), context);

    String engine_type("dt");
    if (context.getTMTContext().getEngineType() == ::TiDB::StorageEngine::TMT)
        engine_type = "tmt";
    if (args.size() == 5)
        engine_type = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[4]).value);

    auto tso = context.getTMTContext().getPDClient()->getTS();

    TableID table_id = MockTiDB::instance().newTable(database_name, table_name, columns, tso, handle_pk_name, engine_type);

    std::stringstream ss;
    ss << "mock table #" << table_id;
    output(ss.str());
}

void MockTiDBTable::dbgFuncMockTiDBDB(Context &, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: database-name", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;

    DatabaseID db_id = MockTiDB::instance().newDataBase(database_name);

    std::stringstream ss;
    ss << "mock db #" << db_id;
    output(ss.str());
}

void MockTiDBTable::dbgFuncMockTiDBPartition(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 3 && args.size() != 4)
        throw Exception("Args not matched, should be: database-name, table-name, partition-name", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    TableID partition_id = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    bool is_add_part = false;
    if (args.size() == 4)
    {
        is_add_part = typeid_cast<const ASTIdentifier &>(*args[3]).name == "true";
    }
    auto tso = context.getTMTContext().getPDClient()->getTS();

    MockTiDB::instance().newPartition(database_name, table_name, partition_id, tso, is_add_part);

    std::stringstream ss;
    ss << "mock partition #" << partition_id;
    output(ss.str());
}

void MockTiDBTable::dbgFuncDropTiDBPartition(Context &, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 3)
        throw Exception("Args not matched, should be: database-name, table-name, partition-name", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    TableID partition_id = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);

    MockTiDB::instance().dropPartition(database_name, table_name, partition_id);

    std::stringstream ss;
    ss << "drop partition #" << partition_id;
    output(ss.str());
}

void MockTiDBTable::dbgFuncDropTiDBDB(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1 && args.size() != 2)
        throw Exception("Args not matched, should be: database-name [, drop-regions]", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    bool drop_regions = true;
    if (args.size() == 2)
        drop_regions = typeid_cast<const ASTIdentifier &>(*args[1]).name == "true";

    MockTiDB::instance().dropDB(context, database_name, drop_regions);

    std::stringstream ss;
    ss << "dropped db #" << database_name;
    output(ss.str());
}

void MockTiDBTable::dbgFuncDropTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 2 && args.size() != 3)
        throw Exception("Args not matched, should be: database-name, table-name[, drop-regions]", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    bool drop_regions = true;
    if (args.size() == 3)
        drop_regions = typeid_cast<const ASTIdentifier &>(*args[1]).name == "true";

    MockTiDB::TablePtr table = nullptr;
    TableID table_id = InvalidTableID;
    try
    {
        table = MockTiDB::instance().getTableByName(database_name, table_name);
        table_id = table->id();
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::UNKNOWN_TABLE)
            throw;
        output("table not exists, skipped");
        return;
    }

    MockTiDB::instance().dropTable(context, database_name, table_name, drop_regions);

    std::stringstream ss;
    ss << "dropped table #" << table_id;
    output(ss.str());
}

void MockTiDBTable::dbgFuncAddColumnToTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 3)
        throw Exception("Args not matched, should be: database-name, table-name, 'col type'", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    auto col_str = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    ASTPtr col_ast;
    ParserColumnDeclarationList schema_parser;
    Tokens tokens(col_str.data(), col_str.data() + col_str.length());
    TokenIterator pos(tokens);
    Expected expected;
    if (!schema_parser.parse(pos, col_ast, expected))
        throw Exception("Invalid TiDB table column", ErrorCodes::LOGICAL_ERROR);
    ColumnsDescription cols = InterpreterCreateQuery::getColumnsDescription(typeid_cast<const ASTExpressionList &>(*col_ast), context);
    if (cols.getAllPhysical().size() > 1)
        throw Exception("Not support multiple columns", ErrorCodes::LOGICAL_ERROR);

    NameAndTypePair column = cols.getAllPhysical().front();
    auto it = cols.defaults.find(column.name);
    Field default_value;
    if (it != cols.defaults.end())
        default_value = getDefaultValue(it->second.expression);
    MockTiDB::instance().addColumnToTable(database_name, table_name, column, default_value);

    std::stringstream ss;
    ss << "added column " << column.name << " " << column.type->getName();
    output(ss.str());
}

void MockTiDBTable::dbgFuncDropColumnFromTiDBTable(Context & /*context*/, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 3)
        throw Exception("Args not matched, should be: database-name, table-name, column-name", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    const String & column_name = typeid_cast<const ASTIdentifier &>(*args[2]).name;

    MockTiDB::TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);

    MockTiDB::instance().dropColumnFromTable(database_name, table_name, column_name);

    std::stringstream ss;
    ss << "dropped column " << column_name;
    output(ss.str());
}

void MockTiDBTable::dbgFuncModifyColumnInTiDBTable(DB::Context & context, const DB::ASTs & args, DB::DBGInvoker::Printer output)
{
    if (args.size() != 3)
        throw Exception("Args not matched, should be: database-name, table-name, 'col type'", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    auto col_str = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    ASTPtr col_ast;
    ParserColumnDeclarationList schema_parser;
    Tokens tokens(col_str.data(), col_str.data() + col_str.length());
    TokenIterator pos(tokens);
    Expected expected;
    if (!schema_parser.parse(pos, col_ast, expected))
        throw Exception("Invalid TiDB table column", ErrorCodes::LOGICAL_ERROR);
    ColumnsDescription cols = InterpreterCreateQuery::getColumnsDescription(typeid_cast<const ASTExpressionList &>(*col_ast), context);
    if (cols.getAllPhysical().size() > 1)
        throw Exception("Not support multiple columns", ErrorCodes::LOGICAL_ERROR);

    NameAndTypePair column = cols.getAllPhysical().front();
    MockTiDB::instance().modifyColumnInTable(database_name, table_name, column);

    std::stringstream ss;
    ss << "modified column " << column.name << " " << column.type->getName();
    output(ss.str());
}

void MockTiDBTable::dbgFuncRenameColumnInTiDBTable(DB::Context &, const DB::ASTs & args, DB::DBGInvoker::Printer output)
{
    if (args.size() != 4)
        throw Exception("Args not matched, should be: database-name, table-name, old_col_name, new_col_name", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    const String & old_column_name = typeid_cast<const ASTIdentifier &>(*args[2]).name;
    const String & new_column_name = typeid_cast<const ASTIdentifier &>(*args[3]).name;

    MockTiDB::instance().renameColumnInTable(database_name, table_name, old_column_name, new_column_name);

    std::stringstream ss;
    ss << "rename column " << old_column_name << " " << new_column_name;
    output(ss.str());
}

void MockTiDBTable::dbgFuncRenameTiDBTable(Context & /*context*/, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 3)
        throw Exception("Args not matched, should be: database-name, table-name, new-table-name", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    const String & new_table_name = typeid_cast<const ASTIdentifier &>(*args[2]).name;

    MockTiDB::instance().renameTable(database_name, table_name, new_table_name);

    std::stringstream ss;
    ss << "renamed table " << database_name << "." << table_name << " to " << database_name << "." << new_table_name;
    output(ss.str());
}

void MockTiDBTable::dbgFuncTruncateTiDBTable(Context & /*context*/, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 2)
        throw Exception("Args not matched, should be: database-name, table-name", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    MockTiDB::instance().truncateTable(database_name, table_name);

    std::stringstream ss;
    ss << "truncated table " << database_name << "." << table_name;
    output(ss.str());
}

void MockTiDBTable::dbgFuncCleanUpRegions(DB::Context & context, const DB::ASTs &, DB::DBGInvoker::Printer output)
{
    std::vector<RegionID> regions;
    auto & kvstore = context.getTMTContext().getKVStore();
    auto & region_table = context.getTMTContext().getRegionTable();
    {
        for (const auto & e : kvstore->regions())
            regions.emplace_back(e.first);

        for (const auto & region_id : regions)
            kvstore->mockRemoveRegion(region_id, region_table);
    }
    output("all regions have been cleaned");
}

} // namespace DB
