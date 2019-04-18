#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/parseQuery.h>

#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>

#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>

#include <Raft/RaftContext.h>

#include <Debug/MockTiDB.h>
#include <Debug/dbgFuncMockTiDBTable.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

void dbgFuncMockSchemaSyncer(Context & context, const ASTs & args, DBGInvoker::Printer output)
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

void dbgFuncMockTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output)
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
    ColumnsDescription columns = InterpreterCreateQuery::getColumnsDescription(typeid_cast<const ASTExpressionList &>(*columns_ast), context);

    TableID table_id = MockTiDB::instance().newTable(database_name, table_name, columns);

    std::stringstream ss;
    ss << "mock table #" << table_id;
    output(ss.str());
}

void dbgFuncMockTiDBPartition(Context &, const ASTs & args, DBGInvoker::Printer output)
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

void dbgFuncRenameTableForPartition(Context & context, const ASTs & args, DBGInvoker::Printer output)
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
    ASTPtr ast = parseQuery(parser, rename_stmt.data(), rename_stmt.data() + rename_stmt.size(), "from rename table for partition " + database_name + "." + table_name + "." + partition_name, 0);

    InterpreterRenameQuery interpreter(ast, context);
    interpreter.execute();

    std::stringstream ss;
    ss << "table " << physical_name << " renamed to " << new_name;
    output(ss.str());
}

void dbgFuncDropTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output)
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
        std::for_each(partition_ids.begin(), partition_ids.end(), [&](TableID partition_id) {
            tmt.region_table.dropRegionsInTable(partition_id);
        });
    }
    tmt.region_table.dropRegionsInTable(table_id);


    MockTiDB::instance().dropTable(database_name, table_name);

    std::stringstream ss;
    ss << "dropped table #" << table_id;
    output(ss.str());
}

}
