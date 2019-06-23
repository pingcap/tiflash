#include <Debug/MockTiDB.h>
#include <Debug/dbgFuncMockTiDBTable.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/parseQuery.h>
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

void MockTiDBTable::dbgFuncMockSchemaSyncer(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: enable (true/false)", ErrorCodes::BAD_ARGUMENTS);

    bool enabled = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value) == "true";

    TMTContext & tmt = context.getTMTContext();
    if (enabled)
    {
        tmt.setSchemaSyncer(std::make_shared<MockTiDB::MockSchemaSyncer>());
    }
    else
    {
        tmt.setSchemaSyncer(std::make_shared<HttpJsonSchemaSyncer>());
    }

    std::stringstream ss;
    ss << "mock schema syncer " << (enabled ? "enabled" : "disabled");
    output(ss.str());
}

void MockTiDBTable::dbgFuncMockTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 3)
        throw Exception("Args not matched, should be: database-name, table-name, schema-string", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    auto schema_str = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    ASTPtr columns_ast;
    ParserColumnDeclarationList schema_parser;
    Tokens tokens(schema_str.data(), schema_str.data() + schema_str.length());
    TokenIterator pos(tokens);
    Expected expected;
    if (!schema_parser.parse(pos, columns_ast, expected))
        throw Exception("Invalid TiDB table schema", ErrorCodes::LOGICAL_ERROR);
    ColumnsDescription columns
        = InterpreterCreateQuery::getColumnsDescription(typeid_cast<const ASTExpressionList &>(*columns_ast), context);

    TableID table_id = MockTiDB::instance().newTable(database_name, table_name, columns);

    std::stringstream ss;
    ss << "mock table #" << table_id;
    output(ss.str());
}

void MockTiDBTable::dbgFuncMockTiDBPartition(Context &, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 3)
        throw Exception("Args not matched, should be: database-name, table-name, partition-name", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    const String & partition_name = typeid_cast<const ASTIdentifier &>(*args[2]).name;

    TableID partition_id = MockTiDB::instance().newPartition(database_name, table_name, partition_name);

    std::stringstream ss;
    ss << "mock partition #" << partition_id;
    output(ss.str());
}

void MockTiDBTable::dbgFuncRenameTableForPartition(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 4)
        throw Exception("Args not matched, should be: database-name, table-name, partition-name, view-name", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    const String & partition_name = typeid_cast<const ASTIdentifier &>(*args[2]).name;
    const String & new_name = typeid_cast<const ASTIdentifier &>(*args[3]).name;

    const auto & table = MockTiDB::instance().getTableByName(database_name, table_name);

    if (!table->isPartitionTable())
        throw Exception("Table " + database_name + "." + table_name + " is not partition table.", ErrorCodes::LOGICAL_ERROR);
    TableID partition_id = table->getPartitionIDByName(partition_name);
    String physical_name = table_name + "_" + std::to_string(partition_id);
    String rename_stmt = "RENAME TABLE " + database_name + "." + physical_name + " TO " + database_name + "." + new_name;

    ParserRenameQuery parser;
    ASTPtr ast = parseQuery(parser, rename_stmt.data(), rename_stmt.data() + rename_stmt.size(),
        "from rename table for partition " + database_name + "." + table_name + "." + partition_name, 0);

    InterpreterRenameQuery interpreter(ast, context);
    interpreter.execute();

    std::stringstream ss;
    ss << "table " << physical_name << " renamed to " << new_name;
    output(ss.str());
}

void MockTiDBTable::dbgFuncDropTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 2)
        throw Exception("Args not matched, should be: database-name, table-name", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    MockTiDB::TablePtr table = nullptr;
    TableID table_id = InvalidTableID;
    try
    {
        table = MockTiDB::instance().getTableByName(database_name, table_name);
        table_id = table->id();
    }
    catch (Exception e)
    {
        if (e.code() != ErrorCodes::UNKNOWN_TABLE)
            throw;
        output("table not exists, skipped");
        return;
    }

    TMTContext & tmt = context.getTMTContext();
    if (table->isPartitionTable())
    {
        auto partition_ids = table->getPartitionIDs();
        std::for_each(partition_ids.begin(), partition_ids.end(),
            [&](TableID partition_id) { tmt.getRegionTable().mockDropRegionsInTable(partition_id); });
    }
    tmt.getRegionTable().mockDropRegionsInTable(table_id);

    MockTiDB::instance().dropTable(database_name, table_name);

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

    // TODO: Support partition table.

    const NameAndTypePair & column = cols.getAllPhysical().front();
    MockTiDB::instance().addColumnToTable(database_name, table_name, column);

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

    // TODO: Support partition table.

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

    // TODO: Support partition table.

    const NameAndTypePair & column = cols.getAllPhysical().front();
    MockTiDB::instance().modifyColumnInTable(database_name, table_name, column);

    std::stringstream ss;
    ss << "modified column " << column.name << " " << column.type->getName();
    output(ss.str());
}

} // namespace DB
