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

#include <Common/FailPoint.h>
#include <Common/UniThreadPool.h>
#include <Databases/DatabaseTiFlash.h>
#include <IO/FileProvider/ReadBufferFromRandomAccessFileBuilder.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/File.h>
#include <Storages/IManageableStorage.h>
#include <Storages/IStorage.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/TMTStorages.h>
#include <Storages/KVStore/Types.h>
#include <Storages/MutableSupport.h>
#include <Storages/registerStorages.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Schema/SchemaNameMapper.h>
#include <TiDB/Schema/TiDB.h>
#include <common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
} // namespace ErrorCodes

namespace FailPoints
{
extern const char exception_before_rename_table_old_meta_removed[];
extern const char force_context_path[];
} // namespace FailPoints

extern String createDatabaseStmt(Context & context, const TiDB::DBInfo & db_info, const SchemaNameMapper & name_mapper);

namespace tests
{
class DatabaseTiFlashTest : public ::testing::Test
{
public:
    static void SetUpTestCase()
    {
        try
        {
            registerStorages();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registed, ignore exception here.
        }

        FailPointHelper::enableFailPoint(FailPoints::force_context_path);
    }

    static void TearDownTestCase() { FailPointHelper::disableFailPoint(FailPoints::force_context_path); }

    DatabaseTiFlashTest()
        : log(&Poco::Logger::get("DatabaseTiFlashTest"))
    {}

    void SetUp() override { recreateMetadataPath(); }

    void TearDown() override
    {
        // Clean all database from context.
        auto ctx = TiFlashTestEnv::getContext();
        for (const auto & [name, db] : ctx->getDatabases())
        {
            ctx->detachDatabase(name);
            db->shutdown();
        }
    }

    static void recreateMetadataPath()
    {
        String path = TiFlashTestEnv::getContext()->getPath();
        auto p = path + "/metadata/";
        TiFlashTestEnv::tryRemovePath(p, /*recreate=*/true);
        p = path + "/data/";
        TiFlashTestEnv::tryRemovePath(p, /*recreate=*/true);
    }

protected:
    Poco::Logger * log;
};

namespace
{
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
        throw Exception(error_msg, ErrorCodes::SYNTAX_ERROR);
    return ast;
}
} // namespace

TEST_F(DatabaseTiFlashTest, CreateDBAndTable)
try
{
    const String db_name = "db_1";
    auto ctx = TiFlashTestEnv::getContext();

    {
        // Create database
        const String statement = "CREATE DATABASE " + db_name + " ENGINE=TiFlash";
        ASTPtr ast = parseCreateStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterCreateQuery interpreter(ast, *ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    auto db = ctx->tryGetDatabase(db_name);
    ASSERT_NE(db, nullptr);
    EXPECT_EQ(db->getEngineName(), "TiFlash");
    EXPECT_TRUE(db->empty(*ctx));

    const String tbl_name = "t_111";
    {
        /// Create table
        ParserCreateQuery parser;
        const String stmt = "CREATE TABLE `" + db_name + "`.`" + tbl_name
            + "`("
              "c_custkey Int32,"
              "c_acctbal Decimal(15, 2),"
              "c_comment String"
              ") ENGINE = DeltaMerge(c_custkey)";
        ASTPtr ast = parseQuery(parser, stmt, 0);

        InterpreterCreateQuery interpreter(ast, *ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    EXPECT_FALSE(db->empty(*ctx));
    EXPECT_TRUE(db->isTableExist(*ctx, tbl_name));

    {
        // Get storage from database
        auto storage = db->tryGetTable(*ctx, tbl_name);
        ASSERT_NE(storage, nullptr);

        EXPECT_EQ(storage->getName(), MutSup::delta_tree_storage_name);
        EXPECT_EQ(storage->getTableName(), tbl_name);

        auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
        EXPECT_EQ(managed_storage->getDatabaseName(), db_name);
    }

    {
        // Drop table
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = db_name;
        drop_query->table = tbl_name;
        drop_query->if_exists = false;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, *ctx);
        drop_interpreter.execute();

        auto storage = db->tryGetTable(*ctx, tbl_name);
        ASSERT_EQ(storage, nullptr);
    }

    {
        // Drop database
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = db_name;
        drop_query->if_exists = false;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, *ctx);
        drop_interpreter.execute();

        auto db = ctx->tryGetDatabase(db_name);
        ASSERT_EQ(db, nullptr);
    }
}
CATCH

TEST_F(DatabaseTiFlashTest, RenameTable)
try
{
    const String db_name = "db_1";
    auto ctx = TiFlashTestEnv::getContext();

    {
        // Create database
        const String statement = "CREATE DATABASE " + db_name + " ENGINE=TiFlash";
        ASTPtr ast = parseCreateStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterCreateQuery interpreter(ast, *ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    auto db = ctx->tryGetDatabase(db_name);
    ASSERT_NE(db, nullptr);
    EXPECT_EQ(db->getEngineName(), "TiFlash");
    EXPECT_TRUE(db->empty(*ctx));

    const String tbl_name = "t_111";
    {
        /// Create table
        ParserCreateQuery parser;
        const String stmt = "CREATE TABLE `" + db_name + "`.`" + tbl_name
            + "`("
              "c_custkey Int32,"
              "c_acctbal Decimal(15, 2),"
              "c_comment String"
              ") ENGINE = DeltaMerge(c_custkey)";
        ASTPtr ast = parseQuery(parser, stmt, 0);

        InterpreterCreateQuery interpreter(ast, *ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    EXPECT_FALSE(db->empty(*ctx));
    EXPECT_TRUE(db->isTableExist(*ctx, tbl_name));

    {
        // Get storage from database
        auto storage = db->tryGetTable(*ctx, tbl_name);
        ASSERT_NE(storage, nullptr);

        EXPECT_EQ(storage->getName(), MutSup::delta_tree_storage_name);
        EXPECT_EQ(storage->getTableName(), tbl_name);

        auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
        EXPECT_EQ(managed_storage->getDatabaseName(), db_name);
    }

    const String to_tbl_display_name = "tbl_test";
    {
        // Rename table
        typeid_cast<DatabaseTiFlash *>(db.get())
            ->renameTable(*ctx, tbl_name, *db, tbl_name, db_name, to_tbl_display_name);

        auto storage = db->tryGetTable(*ctx, tbl_name);
        ASSERT_NE(storage, nullptr);
        EXPECT_EQ(storage->getName(), MutSup::delta_tree_storage_name);
        EXPECT_EQ(storage->getTableName(), tbl_name);

        auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
        EXPECT_EQ(managed_storage->getDatabaseName(), db_name);
    }

    {
        // Drop table
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = db_name;
        drop_query->table = tbl_name;
        drop_query->if_exists = false;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, *ctx);
        drop_interpreter.execute();

        auto storage = db->tryGetTable(*ctx, tbl_name);
        ASSERT_EQ(storage, nullptr);
    }

    {
        // Drop database
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = db_name;
        drop_query->if_exists = false;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, *ctx);
        drop_interpreter.execute();

        auto db = ctx->tryGetDatabase(db_name);
        ASSERT_EQ(db, nullptr);
    }
}
CATCH

TEST_F(DatabaseTiFlashTest, RenameTableBetweenDatabase)
try
{
    const String db_name = "db_1";
    const String db2_name = "db_2";
    auto ctx = TiFlashTestEnv::getContext();

    {
        // Create database
        const String statement = "CREATE DATABASE " + db_name + " ENGINE=TiFlash";
        ASTPtr ast = parseCreateStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterCreateQuery interpreter(ast, *ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    {
        // Create database2
        const String statement = "CREATE DATABASE " + db2_name + " ENGINE=TiFlash";
        ASTPtr ast = parseCreateStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterCreateQuery interpreter(ast, *ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    auto db = ctx->tryGetDatabase(db_name);
    ASSERT_NE(db, nullptr);
    EXPECT_EQ(db->getEngineName(), "TiFlash");
    EXPECT_TRUE(db->empty(*ctx));

    auto db2 = ctx->tryGetDatabase(db2_name);
    ASSERT_NE(db2, nullptr);
    EXPECT_EQ(db2->getEngineName(), "TiFlash");
    EXPECT_TRUE(db2->empty(*ctx));

    const String tbl_name = "t_111";
    {
        /// Create table
        ParserCreateQuery parser;
        const String stmt = "CREATE TABLE `" + db_name + "`.`" + tbl_name
            + "`("
              "c_custkey Int32,"
              "c_acctbal Decimal(15, 2),"
              "c_comment String"
              ") ENGINE = DeltaMerge(c_custkey)";
        ASTPtr ast = parseQuery(parser, stmt, 0);

        InterpreterCreateQuery interpreter(ast, *ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    EXPECT_FALSE(db->empty(*ctx));
    EXPECT_TRUE(db->isTableExist(*ctx, tbl_name));

    {
        // Get storage from database
        auto storage = db->tryGetTable(*ctx, tbl_name);
        ASSERT_NE(storage, nullptr);

        EXPECT_EQ(storage->getName(), MutSup::delta_tree_storage_name);
        EXPECT_EQ(storage->getTableName(), tbl_name);

        auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
        EXPECT_EQ(managed_storage->getDatabaseName(), db_name);
    }

    const String to_tbl_display_name = "tbl_test";
    {
        // Rename table
        typeid_cast<DatabaseTiFlash *>(db.get())
            ->renameTable(*ctx, tbl_name, *db2, tbl_name, db2_name, to_tbl_display_name);

        auto old_storage = db->tryGetTable(*ctx, tbl_name);
        ASSERT_EQ(old_storage, nullptr);

        auto storage = db2->tryGetTable(*ctx, tbl_name);
        ASSERT_NE(storage, nullptr);
        EXPECT_EQ(storage->getName(), MutSup::delta_tree_storage_name);
        EXPECT_EQ(storage->getTableName(), tbl_name);

        auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
        EXPECT_EQ(managed_storage->getDatabaseName(), db2_name);
    }

    {
        // Drop table
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = db2_name;
        drop_query->table = tbl_name;
        drop_query->if_exists = false;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, *ctx);
        drop_interpreter.execute();

        auto storage = db2->tryGetTable(*ctx, tbl_name);
        ASSERT_EQ(storage, nullptr);
    }

    {
        // Drop database
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = db_name;
        drop_query->if_exists = false;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, *ctx);
        drop_interpreter.execute();

        auto db = ctx->tryGetDatabase(db_name);
        ASSERT_EQ(db, nullptr);
    }

    {
        // Drop database
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = db2_name;
        drop_query->if_exists = false;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, *ctx);
        drop_interpreter.execute();

        auto db2 = ctx->tryGetDatabase(db_name);
        ASSERT_EQ(db2, nullptr);
    }
}
CATCH


TEST_F(DatabaseTiFlashTest, AtomicRenameTableBetweenDatabase)
try
{
    const String db_name = "db_1";
    const String db2_name = "db_2";
    auto ctx = TiFlashTestEnv::getContext();

    {
        // Create database
        const String statement = "CREATE DATABASE " + db_name + " ENGINE=TiFlash";
        ASTPtr ast = parseCreateStatement(statement);
        InterpreterCreateQuery interpreter(ast, *ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    {
        // Create database2
        const String statement = "CREATE DATABASE " + db2_name + " ENGINE=TiFlash";
        ASTPtr ast = parseCreateStatement(statement);
        InterpreterCreateQuery interpreter(ast, *ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    auto db = ctx->getDatabase(db_name);

    auto db2 = ctx->getDatabase(db2_name);

    const String tbl_name = "t_111";
    {
        /// Create table
        ParserCreateQuery parser;
        const String stmt = "CREATE TABLE `" + db_name + "`.`" + tbl_name + "`" + R"(
            (i Nullable(Int32), f Nullable(Float32), _tidb_rowid Int64) ENGINE = DeltaMerge(_tidb_rowid,
            '{"cols":[{"comment":"","default":null,"id":1,"name":{"L":"i","O":"i"},"offset":0,"origin_default":null,"state":5,"type":{"Decimal":0,"Elems":null,"Flag":0,"Flen":11,"Tp":3}},{"comment":"","default":null,"id":2,"name":{"L":"f","O":"f"},"offset":1,"origin_default":null,"state":5,"type":{"Decimal":-1,"Elems":null,"Flag":0,"Flen":12,"Tp":4}}],"comment":"","id":50,"name":{"L":"t","O":"t"},"partition":null,"pk_is_handle":false,"schema_version":27,"state":5,"update_timestamp":415992658599346193}')
            )";
        ASTPtr ast = parseQuery(parser, stmt, 0);

        InterpreterCreateQuery interpreter(ast, *ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    EXPECT_FALSE(db->empty(*ctx));
    EXPECT_TRUE(db->isTableExist(*ctx, tbl_name));

    // Rename table to another database, and mock crash by failed point
    FailPointHelper::enableFailPoint(FailPoints::exception_before_rename_table_old_meta_removed);
    ASSERT_THROW(
        typeid_cast<DatabaseTiFlash *>(db.get())->renameTable(*ctx, tbl_name, *db2, tbl_name, db2_name, tbl_name),
        DB::Exception);

    {
        // After fail point triggled we should have both meta file in disk
        Poco::File old_meta_file{db->getTableMetadataPath(tbl_name)};
        ASSERT_TRUE(old_meta_file.exists());
        Poco::File new_meta_file(db2->getTableMetadataPath(tbl_name));
        ASSERT_TRUE(new_meta_file.exists());
        // Old table should remain in db
        auto old_storage = db->tryGetTable(*ctx, tbl_name);
        ASSERT_NE(old_storage, nullptr);
        // New table is not exists in db2
        auto new_storage = db2->tryGetTable(*ctx, tbl_name);
        ASSERT_EQ(new_storage, nullptr);
    }

    {
        // If we loadTable for db2, new table meta should be removed.
        ThreadPool thread_pool(2);
        db2->loadTables(*ctx, &thread_pool, true);

        Poco::File new_meta_file(db2->getTableMetadataPath(tbl_name));
        ASSERT_FALSE(new_meta_file.exists());

        auto storage = db2->tryGetTable(*ctx, tbl_name);
        ASSERT_EQ(storage, nullptr);
    }

    {
        // Drop table
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = db_name;
        drop_query->table = tbl_name;
        drop_query->if_exists = false;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, *ctx);
        drop_interpreter.execute();

        auto storage = db->tryGetTable(*ctx, tbl_name);
        ASSERT_EQ(storage, nullptr);
    }
}
CATCH

TEST_F(DatabaseTiFlashTest, RenameTableOnlyUpdateDisplayName)
try
{
    const String db_name = "db_1";
    auto ctx = TiFlashTestEnv::getContext();

    {
        // Create database
        const String statement = "CREATE DATABASE " + db_name + " ENGINE=TiFlash";
        ASTPtr ast = parseCreateStatement(statement);
        InterpreterCreateQuery interpreter(ast, *ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    auto db = ctx->getDatabase(db_name);

    const String tbl_name = "t_111";
    {
        /// Create table
        ParserCreateQuery parser;
        const String stmt = "CREATE TABLE `" + db_name + "`.`" + tbl_name + "`" + R"(
            (i Nullable(Int32), f Nullable(Float32), _tidb_rowid Int64) ENGINE = DeltaMerge(_tidb_rowid,
            '{"cols":[{"comment":"","default":null,"id":1,"name":{"L":"i","O":"i"},"offset":0,"origin_default":null,"state":5,"type":{"Decimal":0,"Elems":null,"Flag":0,"Flen":11,"Tp":3}},{"comment":"","default":null,"id":2,"name":{"L":"f","O":"f"},"offset":1,"origin_default":null,"state":5,"type":{"Decimal":-1,"Elems":null,"Flag":0,"Flen":12,"Tp":4}}],"comment":"","id":50,"name":{"L":"t","O":"t"},"partition":null,"pk_is_handle":false,"schema_version":27,"state":5,"update_timestamp":415992658599346193}')
            )";
        ASTPtr ast = parseQuery(parser, stmt, 0);

        InterpreterCreateQuery interpreter(ast, *ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    EXPECT_FALSE(db->empty(*ctx));
    EXPECT_TRUE(db->isTableExist(*ctx, tbl_name));

    {
        // Get storage from database
        auto storage = db->tryGetTable(*ctx, tbl_name);
        ASSERT_NE(storage, nullptr);

        EXPECT_EQ(storage->getName(), MutSup::delta_tree_storage_name);
        EXPECT_EQ(storage->getTableName(), tbl_name);

        auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
        EXPECT_EQ(managed_storage->getDatabaseName(), db_name);
        EXPECT_EQ(managed_storage->getTableInfo().name, "t");
    }

    const String new_display_tbl_name = "accounts";
    {
        // Rename table with only display table name updated.
        typeid_cast<DatabaseTiFlash *>(db.get())
            ->renameTable(*ctx, tbl_name, *db, tbl_name, db_name, new_display_tbl_name);

        auto storage = db->tryGetTable(*ctx, tbl_name);
        ASSERT_NE(storage, nullptr);
        EXPECT_EQ(storage->getName(), MutSup::delta_tree_storage_name);
        EXPECT_EQ(storage->getTableName(), tbl_name);

        auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
        EXPECT_EQ(managed_storage->getDatabaseName(), db_name);
        EXPECT_EQ(managed_storage->getTableInfo().name, new_display_tbl_name); // check display name

        auto tbl_meta = db->getTableMetadataPath(tbl_name);
        ASSERT_TRUE(Poco::File(tbl_meta).exists());
    }

    ASTPtr create_db_ast;
    {
        // Detach database and attach, we should get that table
        auto deatched_db = ctx->detachDatabase(db_name);

        // Attach database
        create_db_ast = deatched_db->getCreateDatabaseQuery(*ctx);
        deatched_db->shutdown();
        deatched_db.reset();
    }
    {
        InterpreterCreateQuery interpreter(create_db_ast, *ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();

        // Get database
        auto db = ctx->tryGetDatabase(db_name);
        ASSERT_NE(db, nullptr);
        EXPECT_TRUE(db->isTableExist(*ctx, tbl_name));
        EXPECT_EQ(db->getEngineName(), "TiFlash");
        // Get storage from database
        auto storage = db->tryGetTable(*ctx, tbl_name);
        ASSERT_NE(storage, nullptr);
        EXPECT_EQ(storage->getName(), MutSup::delta_tree_storage_name);
        EXPECT_EQ(storage->getTableName(), tbl_name);

        auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
        EXPECT_EQ(managed_storage->getDatabaseName(), db_name);
        EXPECT_EQ(managed_storage->getTableInfo().name, new_display_tbl_name);
    }
}
CATCH

TEST_F(DatabaseTiFlashTest, ISSUE4596)
try
{
    const String db_name = "db_1";
    auto ctx = TiFlashTestEnv::getContext();

    {
        // Create database
        const String statement = "CREATE DATABASE " + db_name + " ENGINE=TiFlash";
        ASTPtr ast = parseCreateStatement(statement);
        InterpreterCreateQuery interpreter(ast, *ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    auto db = ctx->getDatabase(db_name);

    const String tbl_name = "t_111";
    {
        /// Create table
        ParserCreateQuery parser;
        const String stmt = fmt::format("CREATE TABLE `{}`.`{}` ", db_name, tbl_name) +
            R"stmt( 
                (`id` Int32,`b` String) Engine = DeltaMerge((`id`),
                    '{
                        "cols":[{
                            "comment":"",
                            "default":null,
                            "default_bit":null,
                            "id":1,
                            "name":{
                                "L":"id",
                                "O":"id"
                            },
                            "offset":0,
                            "origin_default":null,
                            "state":5,
                            "type":{
                                "Charset":"binary",
                                "Collate":"binary",
                                "Decimal":0,
                                "Elems":null,
                                "Flag":515,
                                "Flen":16,
                                "Tp":3
                            }
                        },
                        {
                            "comment":"",
                            "default":"",
                            "default_bit":null,
                            "id":15,
                            "name":{
                                "L":"b",
                                "O":"b"
                            },
                            "offset":12,
                            "origin_default":"",
                            "state":5,
                            "type":{
                                "Charset":"binary",
                                "Collate":"binary",
                                "Decimal":0,
                                "Elems":null,
                                "Flag":4225,
                                "Flen":-1,
                                "Tp":251
                            }
                        }],
                        "comment":"",
                        "id":330,
                        "index_info":[],
                        "is_common_handle":false,
                        "name":{
                            "L":"test",
                            "O":"test"
                        },
                        "partition":null,
                        "pk_is_handle":true,
                        "schema_version":465,
                        "state":5,
                        "update_timestamp":99999
                    }'
                )
            )stmt";
        ASTPtr ast = parseQuery(parser, stmt, 0);

        InterpreterCreateQuery interpreter(ast, *ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    EXPECT_FALSE(db->empty(*ctx));
    EXPECT_TRUE(db->isTableExist(*ctx, tbl_name));

    {
        // Get storage from database
        auto storage = db->tryGetTable(*ctx, tbl_name);
        ASSERT_NE(storage, nullptr);

        EXPECT_EQ(storage->getName(), MutSup::delta_tree_storage_name);
        EXPECT_EQ(storage->getTableName(), tbl_name);

        auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
        EXPECT_EQ(managed_storage->getDatabaseName(), db_name);
        EXPECT_EQ(managed_storage->getTableInfo().name, "test");
    }
}
CATCH

TEST_F(DatabaseTiFlashTest, ISSUE1055)
try
{
    CHECK_TESTS_WITH_DATA_ENABLED;

    // Generated by running these SQL on cluster version v4.0.0~v4.0.3
    // > create table test.t(a int primary key);
    // > alter table test.t change a a2 int;

    const String db_name = "db_1";
    auto ctx = TiFlashTestEnv::getContext();

    {
        // Create database
        const String statement = "CREATE DATABASE " + db_name + " ENGINE=TiFlash";
        ASTPtr ast = parseCreateStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterCreateQuery interpreter(ast, *ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    auto db = ctx->tryGetDatabase(db_name);
    ASSERT_NE(db, nullptr);

    String meta_path;
    {
        auto paths = TiFlashTestEnv::findTestDataPath("issue-1055");
        ASSERT_EQ(paths.size(), 1UL);
        meta_path = paths[0];
    }
    auto db_data_path = TiFlashTestEnv::getContext()->getPath() + "/data/";
    DatabaseLoading::loadTable(*ctx, *db, meta_path, db_name, db_data_path, "TiFlash", "t_45.sql", false);

    // Get storage from database
    const auto * tbl_name = "t_45";
    auto storage = db->tryGetTable(*ctx, tbl_name);
    ASSERT_NE(storage, nullptr);
    EXPECT_EQ(storage->getName(), MutSup::delta_tree_storage_name);
    EXPECT_EQ(storage->getTableName(), tbl_name);

    auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
    auto sd = managed_storage->getPrimarySortDescription();
    ASSERT_EQ(sd.size(), 1UL);
    EXPECT_EQ(sd[0].column_name, "a2");
}
CATCH

TEST_F(DatabaseTiFlashTest, ISSUE1093)
try
{
    // The json info get by `curl http://{tidb-ip}:{tidb-status-port}/schema`
    const std::vector<std::pair<String, String>> cases = {
        //
        {R"raw(x`f"n)raw", R"r({
  "id": 49,
  "db_name": {
   "O": "x`f\"n",
   "L": "x`f\"n"
  },
  "charset": "utf8mb4",
  "collate": "utf8mb4_bin",
  "state": 5
})r"},
        {R"raw(x'x)raw", R"r({
  "id": 72,
  "db_name": {
   "O": "x'x",
   "L": "x'x"
  },
  "charset": "utf8mb4",
  "collate": "utf8mb4_bin",
  "state": 5
})r"},
        {R"raw(x"x)raw", R"r({
  "id": 70,
  "db_name": {
   "O": "x\"x",
   "L": "x\"x"
  },
  "charset": "utf8mb4",
  "collate": "utf8mb4_bin",
  "state": 5
})r"},
        {R"raw(a~!@#$%^&*()_+-=[]{}\|'",./<>?)raw", R"r({
  "id": 76,
  "db_name": {
   "O": "a~!@#$%^\u0026*()_+-=[]{}\\|'\",./\u003c\u003e?",
   "L": "a~!@#$%^\u0026*()_+-=[]{}\\|'\",./\u003c\u003e?"
  },
  "charset": "utf8mb4",
  "collate": "utf8mb4_bin",
  "state": 5
})r"},
    };

    for (const auto & [expect_name, json_str] : cases)
    {
        TiDB::DBInfoPtr db_info = std::make_shared<TiDB::DBInfo>(json_str, NullspaceID);
        ASSERT_NE(db_info, nullptr);
        ASSERT_EQ(db_info->name, expect_name);

        const auto seri = db_info->serialize();

        {
            auto deseri = std::make_shared<TiDB::DBInfo>(seri, NullspaceID);
            ASSERT_NE(deseri, nullptr);
            ASSERT_EQ(deseri->name, expect_name);
        }

        auto ctx = TiFlashTestEnv::getContext();
        auto name_mapper = SchemaNameMapper();
        const String statement = createDatabaseStmt(*ctx, *db_info, name_mapper);
        ASTPtr ast = parseCreateStatement(statement);

        InterpreterCreateQuery interpreter(ast, *ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();

        auto db = ctx->getDatabase(name_mapper.mapDatabaseName(*db_info));
        ASSERT_NE(db, nullptr);
        EXPECT_EQ(db->getEngineName(), "TiFlash");
        auto * flash_db = typeid_cast<DatabaseTiFlash *>(db.get());
        auto & db_info_get = flash_db->getDatabaseInfo();
        ASSERT_EQ(db_info_get.name, expect_name);
    }
}
CATCH

// metadata/${db_name}.sql
String getDatabaseMetadataPath(const String & base_path)
{
    return (endsWith(base_path, "/") ? base_path.substr(0, base_path.size() - 1) : base_path) + ".sql";
}

String readFile(Context & ctx, const String & file)
{
    String res;
    auto in = ReadBufferFromRandomAccessFileBuilder::build(ctx.getFileProvider(), file, EncryptionPath(file, ""));
    readStringUntilEOF(res, in);
    return res;
}

DatabasePtr detachThenAttach(Context & ctx, const String & db_name, DatabasePtr && db, Poco::Logger * log)
{
    auto meta = readFile(ctx, getDatabaseMetadataPath(db->getMetadataPath()));
    LOG_DEBUG(log, "After tombstone [meta={}]", meta);
    {
        // Detach and load again
        auto detach_query = std::make_shared<ASTDropQuery>();
        detach_query->detach = true;
        detach_query->database = db_name;
        detach_query->if_exists = false;
        ASTPtr ast_detach_query = detach_query;
        InterpreterDropQuery detach_interpreter(ast_detach_query, ctx);
        detach_interpreter.execute();
    }
    {
        ASTPtr ast = parseCreateStatement(meta);
        InterpreterCreateQuery interpreter(ast, ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    db = ctx.getDatabase(db_name);
    return std::move(db);
}

TEST_F(DatabaseTiFlashTest, Tombstone)
try
{
    const String db_name = "db_1";
    auto ctx = TiFlashTestEnv::getContext();

    const Strings statements = {
        // test case for reading old format metadata without tombstone
        "CREATE DATABASE " + db_name + " ENGINE = TiFlash",
        "CREATE DATABASE " + db_name + R"(
 ENGINE = TiFlash('{"charset":"utf8mb4","collate":"utf8mb4_bin","db_name":{"L":"test_db","O":"test_db"},"id":1010,"state":5}', 1)
)",
        // test case for reading metadata with tombstone
        "CREATE DATABASE " + db_name + R"(
 ENGINE = TiFlash('{"charset":"utf8mb4","collate":"utf8mb4_bin","db_name":{"L":"test_db","O":"test_db"},"id":1010,"state":5}', 1, 12345)
)",
    };

    size_t case_no = 0;
    for (const auto & statement : statements)
    {
        {
            // Cleanup: Drop database if exists
            auto drop_query = std::make_shared<ASTDropQuery>();
            drop_query->database = db_name;
            drop_query->if_exists = true;
            ASTPtr ast_drop_query = drop_query;
            InterpreterDropQuery drop_interpreter(ast_drop_query, *ctx);
            drop_interpreter.execute();
        }

        {
            // Create database
            ASTPtr ast = parseCreateStatement(statement);
            InterpreterCreateQuery interpreter(ast, *ctx);
            interpreter.setInternal(true);
            interpreter.setForceRestoreData(false);
            interpreter.execute();
        }

        auto db = ctx->getDatabase(db_name);
        auto meta = readFile(*ctx, getDatabaseMetadataPath(db->getMetadataPath()));
        LOG_DEBUG(log, "After create [meta={}]", meta);

        DB::Timestamp tso = 1000;
        db->alterTombstone(*ctx, tso, nullptr);
        EXPECT_TRUE(db->isTombstone());
        EXPECT_EQ(db->getTombstone(), tso);
        if (case_no != 0)
        {
            auto db_tiflash = std::dynamic_pointer_cast<DatabaseTiFlash>(db);
            ASSERT_NE(db_tiflash, nullptr);
            auto db_info = db_tiflash->getDatabaseInfo();
            ASSERT_EQ(db_info.name, "test_db"); // not changed
        }

        // Try restore from disk
        db = detachThenAttach(*ctx, db_name, std::move(db), log);
        EXPECT_TRUE(db->isTombstone());
        EXPECT_EQ(db->getTombstone(), tso);

        // Recover, usually recover with a new database name
        auto new_db_info = std::make_shared<TiDB::DBInfo>(
            R"json({"charset":"utf8mb4","collate":"utf8mb4_bin","db_name":{"L":"test_new_db","O":"test_db"},"id":1010,"state":5})json",
            NullspaceID);
        db->alterTombstone(*ctx, 0, new_db_info);
        EXPECT_FALSE(db->isTombstone());
        if (case_no != 0)
        {
            auto db_tiflash = std::dynamic_pointer_cast<DatabaseTiFlash>(db);
            ASSERT_NE(db_tiflash, nullptr);
            auto db_info = db_tiflash->getDatabaseInfo();
            ASSERT_EQ(db_info.name, "test_new_db"); // changed by the `new_db_info`
        }

        // Try restore from disk
        db = detachThenAttach(*ctx, db_name, std::move(db), log);
        EXPECT_FALSE(db->isTombstone());
        if (case_no != 0)
        {
            auto db_tiflash = std::dynamic_pointer_cast<DatabaseTiFlash>(db);
            ASSERT_NE(db_tiflash, nullptr);
            auto db_info = db_tiflash->getDatabaseInfo();
            ASSERT_EQ(db_info.name, "test_new_db"); // changed by the `new_db_info`
        }

        case_no += 1;
    }
}
CATCH

} // namespace tests
} // namespace DB
