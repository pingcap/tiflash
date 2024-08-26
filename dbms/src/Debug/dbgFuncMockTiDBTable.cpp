// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Debug/MockTiDB.h>
#include <Debug/dbgFuncMockTiDBTable.h>
#include <Debug/dbgKVStore/dbgKVStore.h>
#include <Debug/dbgTools.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <TiDB/Schema/SchemaSyncer.h>
#include <fmt/core.h>

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
            "Args not matched, should be: database-name, table-name, schema-string [, handle_pk_name]",
            ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    auto schema_str = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    String handle_pk_name;
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

    auto tso = context.getTMTContext().getPDClient()->getTS();

    TableID table_id = MockTiDB::instance().newTable(database_name, table_name, columns, tso, handle_pk_name);

    output(fmt::format("mock table #{}", table_id));
}

void MockTiDBTable::dbgFuncMockTiDBDB(Context &, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: database-name", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;

    DatabaseID db_id = MockTiDB::instance().newDataBase(database_name);

    output(fmt::format("mock db #{}", db_id));
}

void MockTiDBTable::dbgFuncMockTiDBPartition(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 3 && args.size() != 4)
        throw Exception(
            "Args not matched, should be: database-name, table-name, partition-name",
            ErrorCodes::BAD_ARGUMENTS);

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

    output(fmt::format("mock partition #{}", partition_id));
}

void MockTiDBTable::dbgFuncDropTiDBPartition(Context &, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 3)
        throw Exception(
            "Args not matched, should be: database-name, table-name, partition-name",
            ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    TableID partition_id = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);

    MockTiDB::instance().dropPartition(database_name, table_name, partition_id);

    output(fmt::format("drop partition #{}", partition_id));
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

    output(fmt::format("dropped db #{}", database_name));
}

void MockTiDBTable::dbgFuncDropTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 2 && args.size() != 3)
        throw Exception(
            "Args not matched, should be: database-name, table-name[, drop-regions]",
            ErrorCodes::BAD_ARGUMENTS);

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

    output(fmt::format("dropped table #{}", table_id));
}

void MockTiDBTable::dbgFuncAddColumnToTiDBTable(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 3)
        throw Exception(
            "Args not matched, should be: database-name, table-name, 'col type'",
            ErrorCodes::BAD_ARGUMENTS);

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
    ColumnsDescription cols
        = InterpreterCreateQuery::getColumnsDescription(typeid_cast<const ASTExpressionList &>(*col_ast), context);
    if (cols.getAllPhysical().size() > 1)
        throw Exception("Not support multiple columns", ErrorCodes::LOGICAL_ERROR);

    NameAndTypePair column = cols.getAllPhysical().front();
    auto it = cols.defaults.find(column.name);
    Field default_value;
    if (it != cols.defaults.end())
        default_value = getDefaultValue(it->second.expression);
    MockTiDB::instance().addColumnToTable(database_name, table_name, column, default_value);

    output(fmt::format("add column {} {}", column.name, column.type->getName()));
}

void MockTiDBTable::dbgFuncDropColumnFromTiDBTable(Context & /*context*/, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 3)
        throw Exception(
            "Args not matched, should be: database-name, table-name, column-name",
            ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    const String & column_name = typeid_cast<const ASTIdentifier &>(*args[2]).name;

    MockTiDB::TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);

    MockTiDB::instance().dropColumnFromTable(database_name, table_name, column_name);

    output(fmt::format("dropped column {}", column_name));
}

void MockTiDBTable::dbgFuncModifyColumnInTiDBTable(
    DB::Context & context,
    const DB::ASTs & args,
    DB::DBGInvoker::Printer output)
{
    if (args.size() != 3)
        throw Exception(
            "Args not matched, should be: database-name, table-name, 'col type'",
            ErrorCodes::BAD_ARGUMENTS);

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
    ColumnsDescription cols
        = InterpreterCreateQuery::getColumnsDescription(typeid_cast<const ASTExpressionList &>(*col_ast), context);
    if (cols.getAllPhysical().size() > 1)
        throw Exception("Not support multiple columns", ErrorCodes::LOGICAL_ERROR);

    NameAndTypePair column = cols.getAllPhysical().front();
    MockTiDB::instance().modifyColumnInTable(database_name, table_name, column);

    output(fmt::format("modified column {} {}", column.name, column.type->getName()));
}

void MockTiDBTable::dbgFuncRenameColumnInTiDBTable(DB::Context &, const DB::ASTs & args, DB::DBGInvoker::Printer output)
{
    if (args.size() != 4)
        throw Exception(
            "Args not matched, should be: database-name, table-name, old_col_name, new_col_name",
            ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    const String & old_column_name = typeid_cast<const ASTIdentifier &>(*args[2]).name;
    const String & new_column_name = typeid_cast<const ASTIdentifier &>(*args[3]).name;

    MockTiDB::instance().renameColumnInTable(database_name, table_name, old_column_name, new_column_name);

    output(fmt::format("rename column {} {}", old_column_name, new_column_name));
}

void MockTiDBTable::dbgFuncRenameTiDBTable(Context & /*context*/, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 3)
        throw Exception(
            "Args not matched, should be: database-name, table-name, new-table-name",
            ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    const String & new_table_name = typeid_cast<const ASTIdentifier &>(*args[2]).name;

    MockTiDB::instance().renameTable(database_name, table_name, new_table_name);

    output(fmt::format("renamed table {}.{} to {}.{}", database_name, table_name, database_name, new_table_name));
}

void MockTiDBTable::dbgFuncTruncateTiDBTable(Context & /*context*/, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 2)
        throw Exception("Args not matched, should be: database-name, table-name", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    MockTiDB::instance().truncateTable(database_name, table_name);

    output(fmt::format("truncated table {}.{}", database_name, table_name));
}

void MockTiDBTable::dbgFuncCleanUpRegions(DB::Context & context, const DB::ASTs &, DB::DBGInvoker::Printer output)
{
    std::vector<RegionID> regions;
    auto & kvstore = context.getTMTContext().getKVStore();
    auto debug_kvstore = RegionBench::DebugKVStore(*kvstore);
    auto & region_table = context.getTMTContext().getRegionTable();
    {
        {
            auto manage_lock = kvstore->genRegionMgrReadLock();
            for (const auto & e : manage_lock.regions)
                regions.emplace_back(e.first);
        }

        for (const auto & region_id : regions)
            debug_kvstore.mockRemoveRegion(region_id, region_table);
    }
    output("all regions have been cleaned");
}

void MockTiDBTable::dbgFuncCreateTiDBTables(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() < 2)
        throw Exception(
            "Args not matched, should be: db_name, table_name, [table_name], ..., [table_name]",
            ErrorCodes::BAD_ARGUMENTS);
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    auto mapped_database_name = mappedDatabase(context, database_name);
    auto db = context.getDatabase(mapped_database_name);

    std::vector<std::tuple<String, ColumnsDescription, String>> tables;

    for (ASTs::size_type i = 1; i < args.size(); i++)
    {
        String schema_str = "i Int64";
        String table_name = fmt::format("t{}", i);
        ASTPtr columns_ast;
        ParserColumnDeclarationList schema_parser;
        Tokens tokens(schema_str.data(), schema_str.data() + schema_str.length());
        TokenIterator pos(tokens);
        Expected expected;
        if (!schema_parser.parse(pos, columns_ast, expected))
            throw Exception("Invalid TiDB table schema", ErrorCodes::LOGICAL_ERROR);
        ColumnsDescription columns = InterpreterCreateQuery::getColumnsDescription(
            typeid_cast<const ASTExpressionList &>(*columns_ast),
            context);
        tables.emplace_back(table_name, columns, "");
    }
    auto tso = context.getTMTContext().getPDClient()->getTS();
    MockTiDB::instance().newTables(database_name, tables, tso);
    output("");
}

void MockTiDBTable::dbgFuncRenameTiDBTables(Context & /*context*/, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() % 3 != 0)
        throw Exception(
            "Args not matched, should be: database-name, table-name, new-table-name, ..., [database-name, table-name, "
            "new-table-name]",
            ErrorCodes::BAD_ARGUMENTS);
    std::vector<std::tuple<std::string, std::string, std::string>> table_map;
    for (ASTs::size_type i = 0; i < args.size() / 3; i++)
    {
        const String & database_name = typeid_cast<const ASTIdentifier &>(*args[3 * i]).name;
        const String & table_name = typeid_cast<const ASTIdentifier &>(*args[3 * i + 1]).name;
        const String & new_table_name = typeid_cast<const ASTIdentifier &>(*args[3 * i + 2]).name;
        table_map.emplace_back(database_name, table_name, new_table_name);
    }
    MockTiDB::instance().renameTables(table_map);
    output(fmt::format("renamed tables"));
}

} // namespace DB
