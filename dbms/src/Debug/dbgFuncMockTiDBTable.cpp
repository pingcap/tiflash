#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>

#include <Interpreters/InterpreterCreateQuery.h>

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

void dbgFuncDropTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 2)
        throw Exception("Args not matched, should be: database-name, table-name", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    TableID table_id = InvalidTableID;
    try
    {
        auto table = MockTiDB::instance().getTableByName(database_name, table_name);
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
    tmt.region_partition.dropRegionsInTable(table_id);
    tmt.table_flushers.dropRegionsInTable(table_id);

    MockTiDB::instance().dropTable(database_name, table_name);

    std::stringstream ss;
    ss << "dropped table #" << table_id;
    output(ss.str());
}

}
