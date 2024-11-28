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
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/registerStorages.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TiDB/Schema/SchemaNameMapper.h>
#include <TiDB/Schema/TiDB.h>

namespace DB::tests
{
class InterperCreateQueryTiFlashTest : public ::testing::Test
{
public:
    static void SetUpTestCase() {}

    static void TearDownTestCase() {}

    InterperCreateQueryTiFlashTest()
        : log(Logger::get("InterperCreateQuery"))
        , context(TiFlashTestEnv::getGlobalContext())
    {}

    ASTPtr parseCreateStatement(const String & statement)
    {
        ParserCreateQuery parser;
        const char * pos = statement.data();
        std::string error_msg;
        auto ast = tryParseQuery(
            parser,
            pos,
            pos + statement.size(),
            error_msg,
            /*hilite=*/false,
            String("in ") + __PRETTY_FUNCTION__,
            /*allow_multi_statements=*/false,
            0);
        if (!ast)
        {
            LOG_ERROR(log, "Failed to parse create statement: {}", error_msg);
            return nullptr;
        }
        return ast;
    }


    void SetUp() override
    {
        recreateMetadataPath();
        registerStorages();
        try
        {
            // create db
            String statement
                = R"json(CREATE DATABASE IF NOT EXISTS `db_2` ENGINE = TiFlash('{"charset":"utf8mb4","collate":"utf8mb4_bin","db_name":{"L":"test","O":"test"},"id":2,"keyspace_id":4294967295,"state":5}', 1))json";

            ASTPtr ast = parseCreateStatement(statement);

            InterpreterCreateQuery interpreter(ast, context);
            interpreter.setInternal(true);
            interpreter.setForceRestoreData(false);
            interpreter.execute();
        }
        catch (...)
        {
            LOG_ERROR(log, "Failed to create database: {}", getCurrentExceptionMessage(true));
            throw;
        }
    }

    void TearDown() override
    {
        auto ctx = TiFlashTestEnv::getContext();
        for (const auto & [name, db] : ctx->getDatabases())
        {
            ctx->detachDatabase(name);
            db->shutdown();
        }
    }

    static DB::ASTPtr getASTCreateQuery()
    {
        String stmt
            = R"json(CREATE TABLE `db_2`.`t_88`(`a` Nullable(Int32), `b` Nullable(Int32), `_tidb_rowid` Int64) Engine = DeltaMerge((`_tidb_rowid`), '{"cols":[{"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"a","O":"a"},"offset":0,"origin_default":null,"state":5,"type":{"Charset":"binary","Collate":"binary","Decimal":0,"Elems":null,"Flag":0,"Flen":11,"Tp":3}},{"comment":"","default":null,"default_bit":null,"id":2,"name":{"L":"b","O":"b"},"offset":1,"origin_default":null,"state":5,"type":{"Charset":"binary","Collate":"binary","Decimal":0,"Elems":null,"Flag":0,"Flen":11,"Tp":3}}],"comment":"","id":88,"index_info":[],"is_common_handle":false,"keyspace_id":4294967295,"name":{"L":"t1","O":"t1"},"partition":null,"pk_is_handle":false,"schema_version":-1,"state":5,"tiflash_replica":{"Available":false,"Count":1},"update_timestamp":442125004587401229}'))json";

        String table_info_json
            = R"json({"id":88,"name":{"O":"t1","L":"t1"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[{"id":1,"name":{"O":"a","L":"a"},"offset":0,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":0,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":2,"name":{"O":"b","L":"b"},"offset":1,"origin_default":null,"origin_default_bit":null,"default":null, "default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":0,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2}],"index_info":null,"constraint_info":null,"fk_info":null,"state":5,"pk_is_handle":false,"is_common_handle":false,"common_handle_version":0,"comment":"","auto_inc_id":0,"auto_id_cache":0,"auto_rand_id":0,"max_col_id":2,"max_idx_id":0,"max_fk_id":0,"max_cst_id":0,"update_timestamp":442125004587401229,"ShardRowIDBits":0,"max_shard_row_id_bits":0,"auto_random_bits":0,"auto_random_range_bits":0,"pre_split_regions":0, "partition":null,"compression":"","view":null,"sequence":null,"Lock":null,"version":5,"tiflash_replica":{"Count":1,"LocationLabels":[],"Available":false,"AvailablePartitionIDs":null},"is_columnar":false,"temp_table_type":0,"cache_table_status":0,"policy_ref_info":null,"stats_options":null,"exchange_partition_info":null,"ttl_info":null})json";

        String db_info_json
            = R"json({"id":2,"db_name":{"O":"test","L":"test"},"charset":"utf8mb4","collate":"utf8mb4_bin","state":5})json";

        TiDB::DBInfo db_info(db_info_json, NullspaceID);
        TiDB::TableInfo table_info(table_info_json, NullspaceID);

        ParserCreateQuery parser;
        ASTPtr ast
            = parseQuery(parser, stmt.data(), stmt.data() + stmt.size(), "from syncSchema " + table_info.name, 0);

        auto * ast_create_query = typeid_cast<ASTCreateQuery *>(ast.get());
        ast_create_query->attach = true;
        ast_create_query->if_not_exists = true;
        ast_create_query->database = "db_2";

        return ast;
    }

    static String getDatabaseName() { return "db_2"; }

    static String getTableName() { return "t_88"; }

    static void recreateMetadataPath()
    {
        String path = TiFlashTestEnv::getContext()->getPath();
        auto p = path + "/metadata/";
        TiFlashTestEnv::tryRemovePath(p, /*recreate=*/true);
        p = path + "/data/";
        TiFlashTestEnv::tryRemovePath(p, /*recreate=*/true);
    }


protected:
    LoggerPtr log;
    Context & context;
};

TEST_F(InterperCreateQueryTiFlashTest, MultiThreadCreateSameTable)
try
{
    // use 600 thread to create the same table at the same time
    std::thread threads[600];
    for (auto & thread : threads)
    {
        thread = std::thread([&] {
            auto ast = getASTCreateQuery();
            InterpreterCreateQuery interpreter(ast, context);
            interpreter.setInternal(true);
            interpreter.setForceRestoreData(false);
            interpreter.execute();

            // check table exist
            ASSERT_TRUE(context.isTableExist(getDatabaseName(), getTableName()));
        });
    }

    for (auto & thread : threads)
    {
        thread.join();
    }
}
CATCH

} // namespace DB::tests
