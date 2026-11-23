// Copyright 2026 PingCAP, Inc.
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

#include <Common/typeid_cast.h>
#include <Debug/TiFlashTestEnv.h>
#include <Flash/FlashService.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
extern const int READONLY;
extern const int SYNTAX_ERROR;
extern const int INTO_OUTFILE_NOT_ALLOWED;
} // namespace DB::ErrorCodes

namespace DB::tests
{
namespace
{
void expectError(const String & sql, int code)
{
    auto ctx = TiFlashTestEnv::getContext();
    try
    {
        restrictTiFlashSystemTableQuery(*ctx, sql);
        FAIL() << sql;
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), code) << e.message();
    }
}
} // namespace

TEST(TiFlashSystemTableQueryTest, AllowsTiDBSystemTableSelects)
{
    auto ctx = TiFlashTestEnv::getContext();
    const std::vector<String> queries = {
        "SELECT * FROM system.dt_tables LIMIT 0, 1024",
        "SELECT * FROM system.dt_segments WHERE keyspace_id=1 LIMIT 0, 1024",
        "SELECT * FROM system.dt_local_indexes WHERE tidb_database IN ('db') AND tidb_table IN ('t') LIMIT 0, 1024",
        "SELECT 1 FROM system.one WHERE 1 IN system.one",
        "SELECT 1 FROM system.one WHERE 1 IN (1, 2)",
        "SELECT 1",
        "SELECT 1 UNION ALL SELECT 2",
    };
    for (const auto & sql : queries)
        ASSERT_NO_THROW(restrictTiFlashSystemTableQuery(*ctx, sql)) << sql;
    ASSERT_EQ(ctx->getSettingsRef().readonly, 1);
}

TEST(TiFlashSystemTableQueryTest, RejectsNonSelect)
{
    expectError("DBGInvoke foo()", ErrorCodes::READONLY);
    expectError("DROP TABLE t", ErrorCodes::READONLY);
    expectError("INSERT INTO t SELECT 1", ErrorCodes::READONLY);
    expectError("SET max_threads = 1", ErrorCodes::READONLY);
    expectError("SHOW TABLES", ErrorCodes::READONLY);
    expectError("DESCRIBE t", ErrorCodes::READONLY);
    expectError("USE default", ErrorCodes::READONLY);
}

TEST(TiFlashSystemTableQueryTest, RejectsMultiStatement)
{
    expectError("SELECT 1; DROP TABLE t", ErrorCodes::SYNTAX_ERROR);
}

TEST(TiFlashSystemTableQueryTest, RejectsTablesOutsideSystem)
{
    expectError("SELECT * FROM ks_1_db_2.t_3", ErrorCodes::READONLY);
    expectError("SELECT * FROM dt_tables", ErrorCodes::READONLY);
    expectError("SELECT * FROM system.dt_tables, ks_1_db_2.t_3", ErrorCodes::READONLY);
    expectError(
        "SELECT * FROM system.dt_tables WHERE tidb_table_id IN (SELECT 1 FROM ks_1_db_2.t_3)",
        ErrorCodes::READONLY);
    expectError("SELECT * FROM numbers(1)", ErrorCodes::READONLY);
    expectError("SELECT 1 FROM system.one WHERE 1 IN ks_1_db_2.t_3", ErrorCodes::READONLY);
    expectError("SELECT 1 FROM system.one WHERE 1 NOT IN ks_1_db_2.t_3", ErrorCodes::READONLY);
    expectError("SELECT 1 FROM system.one WHERE 1 GLOBAL IN ks_1_db_2.t_3", ErrorCodes::READONLY);
    expectError("SELECT 1 FROM system.one WHERE 1 IN t_3", ErrorCodes::READONLY);
    expectError(
        "SELECT hasColumnInTable('other_db', 't', 'secret') FROM system.dt_tables LIMIT 1",
        ErrorCodes::READONLY);
    expectError(
        "SELECT HASCOLUMNINTABLE('other_db', 't', 'secret') FROM system.dt_tables LIMIT 1",
        ErrorCodes::READONLY);
    expectError(
        "SELECT hasColumnInTable('system', 'dt_tables', 'name') FROM system.dt_tables LIMIT 1",
        ErrorCodes::READONLY);
}

TEST(TiFlashSystemTableQueryTest, RejectsIntoOutfile)
{
    expectError("SELECT 1 INTO OUTFILE '/tmp/tiflash-system-table'", ErrorCodes::INTO_OUTFILE_NOT_ALLOWED);
}

TEST(TiFlashSystemTableQueryTest, ReadonlyBlocksSettingsClause)
{
    auto ctx = TiFlashTestEnv::getContext();
    const String sql = "SELECT 1 SETTINGS readonly = 0";
    ASSERT_NO_THROW(restrictTiFlashSystemTableQuery(*ctx, sql));
    ASSERT_EQ(ctx->getSettingsRef().readonly, 1);

    ParserQuery parser(sql.data() + sql.size());
    const ASTPtr ast = parseQuery(parser, sql, "", 0);
    const auto * select_union = typeid_cast<const ASTSelectWithUnionQuery *>(ast.get());
    ASSERT_NE(select_union, nullptr);
    ASSERT_EQ(select_union->list_of_selects->children.size(), 1);
    const auto * select = typeid_cast<const ASTSelectQuery *>(select_union->list_of_selects->children[0].get());
    ASSERT_NE(select, nullptr);
    ASSERT_TRUE(select->settings);

    try
    {
        InterpreterSetQuery(select->settings, *ctx).executeForCurrentContext();
        FAIL() << sql;
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::READONLY) << e.message();
    }
    ASSERT_EQ(ctx->getSettingsRef().readonly, 1);
}
} // namespace DB::tests
