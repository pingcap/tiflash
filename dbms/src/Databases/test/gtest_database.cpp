#include <Common/FailPoint.h>
#include <Databases/DatabaseTiFlash.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/File.h>
#include <Storages/IManageableStorage.h>
#include <Storages/IStorage.h>
#include <Storages/MutableSupport.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TMTStorages.h>
#include <Storages/registerStorages.h>
#include <common/ThreadPool.h>
#include <test_utils/TiflashTestBasic.h>

#include <optional>

namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
} // namespace ErrorCodes

namespace FailPoints
{
extern const char exception_before_rename_table_old_meta_removed[];
}

namespace tests
{

class DatabaseTiFlash_test : public ::testing::Test
{
public:
    constexpr static const char * TEST_DB_NAME = "test";

    static void SetUpTestCase()
    {
        registerStorages();
        fiu_init(0); // init failpoint
    }

    DatabaseTiFlash_test() {}

    void SetUp() override { recreateMetadataPath(); }

    void TearDown() override
    {
        // Clean all database from context.
        auto & ctx = TiFlashTestEnv::getContext();
        for (const auto & [name, db] : ctx.getDatabases())
        {
            ctx.detachDatabase(name);
            db->shutdown();
        }
    }

    void recreateMetadataPath() const
    {
        String path = TiFlashTestEnv::getContext().getPath();

        auto p = path + "/metadata/";

        if (Poco::File file(p); file.exists())
            file.remove(true);
        Poco::File{p}.createDirectory();

        p = path + "/data/";
        if (Poco::File file(p); file.exists())
            file.remove(true);
        Poco::File{p}.createDirectory();
    }
};

namespace
{
ASTPtr parseCreateStatement(const String & statement)
{
    ParserCreateQuery parser;
    const char * pos = statement.data();
    std::string error_msg;
    auto ast = tryParseQuery(parser,
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

TEST_F(DatabaseTiFlash_test, CreateDBAndTable)
try
{
    TiFlashTestEnv::setupLogger();

    const String db_name = "db_1";
    auto ctx = TiFlashTestEnv::getContext();

    {
        // Create database
        const String statement = "CREATE DATABASE " + db_name + " ENGINE=TiFlash";
        ASTPtr ast = parseCreateStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterCreateQuery interpreter(ast, ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    auto db = ctx.tryGetDatabase(db_name);
    ASSERT_NE(db, nullptr);
    EXPECT_EQ(db->getEngineName(), "TiFlash");
    EXPECT_TRUE(db->empty(ctx));

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

        InterpreterCreateQuery interpreter(ast, ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    EXPECT_FALSE(db->empty(ctx));
    EXPECT_TRUE(db->isTableExist(ctx, tbl_name));

    {
        // Get storage from database
        auto storage = db->tryGetTable(ctx, tbl_name);
        ASSERT_NE(storage, nullptr);

        EXPECT_EQ(storage->getName(), MutableSupport::delta_tree_storage_name);
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
        InterpreterDropQuery drop_interpreter(ast_drop_query, ctx);
        drop_interpreter.execute();

        auto storage = db->tryGetTable(ctx, tbl_name);
        ASSERT_EQ(storage, nullptr);
    }

    {
        // Drop database
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = db_name;
        drop_query->if_exists = false;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, ctx);
        drop_interpreter.execute();

        auto db = ctx.tryGetDatabase(db_name);
        ASSERT_EQ(db, nullptr);
    }
}
CATCH

TEST_F(DatabaseTiFlash_test, RenameTable)
try
{
    TiFlashTestEnv::setupLogger();

    const String db_name = "db_1";
    auto ctx = TiFlashTestEnv::getContext();

    {
        // Create database
        const String statement = "CREATE DATABASE " + db_name + " ENGINE=TiFlash";
        ASTPtr ast = parseCreateStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterCreateQuery interpreter(ast, ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    auto db = ctx.tryGetDatabase(db_name);
    ASSERT_NE(db, nullptr);
    EXPECT_EQ(db->getEngineName(), "TiFlash");
    EXPECT_TRUE(db->empty(ctx));

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

        InterpreterCreateQuery interpreter(ast, ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    EXPECT_FALSE(db->empty(ctx));
    EXPECT_TRUE(db->isTableExist(ctx, tbl_name));

    {
        // Get storage from database
        auto storage = db->tryGetTable(ctx, tbl_name);
        ASSERT_NE(storage, nullptr);

        EXPECT_EQ(storage->getName(), MutableSupport::delta_tree_storage_name);
        EXPECT_EQ(storage->getTableName(), tbl_name);

        auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
        EXPECT_EQ(managed_storage->getDatabaseName(), db_name);
    }

    const String to_tbl_name = "t_112";
    {
        // Rename table
        typeid_cast<DatabaseTiFlash *>(db.get())->renameTable(ctx, tbl_name, *db, to_tbl_name, db_name, to_tbl_name);

        auto old_storage = db->tryGetTable(ctx, tbl_name);
        ASSERT_EQ(old_storage, nullptr);

        auto storage = db->tryGetTable(ctx, to_tbl_name);
        ASSERT_NE(storage, nullptr);
        EXPECT_EQ(storage->getName(), MutableSupport::delta_tree_storage_name);
        EXPECT_EQ(storage->getTableName(), to_tbl_name);

        auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
        EXPECT_EQ(managed_storage->getDatabaseName(), db_name);
    }

    {
        // Drop table
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = db_name;
        drop_query->table = to_tbl_name;
        drop_query->if_exists = false;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, ctx);
        drop_interpreter.execute();

        auto storage = db->tryGetTable(ctx, to_tbl_name);
        ASSERT_EQ(storage, nullptr);
    }

    {
        // Drop database
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = db_name;
        drop_query->if_exists = false;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, ctx);
        drop_interpreter.execute();

        auto db = ctx.tryGetDatabase(db_name);
        ASSERT_EQ(db, nullptr);
    }
}
CATCH

TEST_F(DatabaseTiFlash_test, RenameTableBetweenDatabase)
try
{
    TiFlashTestEnv::setupLogger();

    const String db_name = "db_1";
    const String db2_name = "db_2";
    auto ctx = TiFlashTestEnv::getContext();

    {
        // Create database
        const String statement = "CREATE DATABASE " + db_name + " ENGINE=TiFlash";
        ASTPtr ast = parseCreateStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterCreateQuery interpreter(ast, ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    {
        // Create database2
        const String statement = "CREATE DATABASE " + db2_name + " ENGINE=TiFlash";
        ASTPtr ast = parseCreateStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterCreateQuery interpreter(ast, ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    auto db = ctx.tryGetDatabase(db_name);
    ASSERT_NE(db, nullptr);
    EXPECT_EQ(db->getEngineName(), "TiFlash");
    EXPECT_TRUE(db->empty(ctx));

    auto db2 = ctx.tryGetDatabase(db2_name);
    ASSERT_NE(db2, nullptr);
    EXPECT_EQ(db2->getEngineName(), "TiFlash");
    EXPECT_TRUE(db2->empty(ctx));

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

        InterpreterCreateQuery interpreter(ast, ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    EXPECT_FALSE(db->empty(ctx));
    EXPECT_TRUE(db->isTableExist(ctx, tbl_name));

    {
        // Get storage from database
        auto storage = db->tryGetTable(ctx, tbl_name);
        ASSERT_NE(storage, nullptr);

        EXPECT_EQ(storage->getName(), MutableSupport::delta_tree_storage_name);
        EXPECT_EQ(storage->getTableName(), tbl_name);

        auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
        EXPECT_EQ(managed_storage->getDatabaseName(), db_name);
    }

    const String to_tbl_name = "t_112";
    {
        // Rename table
        typeid_cast<DatabaseTiFlash *>(db.get())->renameTable(ctx, tbl_name, *db2, to_tbl_name, db2_name, to_tbl_name);

        auto old_storage = db->tryGetTable(ctx, tbl_name);
        ASSERT_EQ(old_storage, nullptr);

        auto storage = db2->tryGetTable(ctx, to_tbl_name);
        ASSERT_NE(storage, nullptr);
        EXPECT_EQ(storage->getName(), MutableSupport::delta_tree_storage_name);
        EXPECT_EQ(storage->getTableName(), to_tbl_name);

        auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
        EXPECT_EQ(managed_storage->getDatabaseName(), db2_name);
    }

    {
        // Drop table
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = db2_name;
        drop_query->table = to_tbl_name;
        drop_query->if_exists = false;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, ctx);
        drop_interpreter.execute();

        auto storage = db2->tryGetTable(ctx, to_tbl_name);
        ASSERT_EQ(storage, nullptr);
    }

    {
        // Drop database
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = db_name;
        drop_query->if_exists = false;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, ctx);
        drop_interpreter.execute();

        auto db = ctx.tryGetDatabase(db_name);
        ASSERT_EQ(db, nullptr);
    }

    {
        // Drop database
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = db2_name;
        drop_query->if_exists = false;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, ctx);
        drop_interpreter.execute();

        auto db2 = ctx.tryGetDatabase(db_name);
        ASSERT_EQ(db2, nullptr);
    }
}
CATCH


TEST_F(DatabaseTiFlash_test, AtomicRenameTableBetweenDatabase)
try
{
    TiFlashTestEnv::setupLogger();

    const String db_name = "db_1";
    const String db2_name = "db_2";
    auto ctx = TiFlashTestEnv::getContext();

    {
        // Create database
        const String statement = "CREATE DATABASE " + db_name + " ENGINE=TiFlash";
        ASTPtr ast = parseCreateStatement(statement);
        InterpreterCreateQuery interpreter(ast, ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    {
        // Create database2
        const String statement = "CREATE DATABASE " + db2_name + " ENGINE=TiFlash";
        ASTPtr ast = parseCreateStatement(statement);
        InterpreterCreateQuery interpreter(ast, ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    auto db = ctx.getDatabase(db_name);

    auto db2 = ctx.getDatabase(db2_name);

    const String tbl_name = "t_111";
    {
        /// Create table
        ParserCreateQuery parser;
        const String stmt = "CREATE TABLE `" + db_name + "`.`" + tbl_name + "`" + R"(
            (i Nullable(Int32), f Nullable(Float32), _tidb_rowid Int64) ENGINE = DeltaMerge(_tidb_rowid,
            '{"cols":[{"comment":"","default":null,"id":1,"name":{"L":"i","O":"i"},"offset":0,"origin_default":null,"state":5,"type":{"Decimal":0,"Elems":null,"Flag":0,"Flen":11,"Tp":3}},{"comment":"","default":null,"id":2,"name":{"L":"f","O":"f"},"offset":1,"origin_default":null,"state":5,"type":{"Decimal":-1,"Elems":null,"Flag":0,"Flen":12,"Tp":4}}],"comment":"","id":50,"name":{"L":"t","O":"t"},"partition":null,"pk_is_handle":false,"schema_version":27,"state":5,"update_timestamp":415992658599346193}')
            )";
        ASTPtr ast = parseQuery(parser, stmt, 0);

        InterpreterCreateQuery interpreter(ast, ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    EXPECT_FALSE(db->empty(ctx));
    EXPECT_TRUE(db->isTableExist(ctx, tbl_name));

    const String to_tbl_name = "t_112";
    // Rename table to another database, and mock crash by failed point
    FailPointHelper::enableFailPoint(FailPoints::exception_before_rename_table_old_meta_removed);
    ASSERT_THROW(
        typeid_cast<DatabaseTiFlash *>(db.get())->renameTable(ctx, tbl_name, *db2, to_tbl_name, db2_name, to_tbl_name), DB::Exception);

    {
        // After fail point triggled we should have both meta file in disk
        Poco::File old_meta_file{db->getTableMetadataPath(tbl_name)};
        ASSERT_TRUE(old_meta_file.exists());
        Poco::File new_meta_file(db2->getTableMetadataPath(to_tbl_name));
        ASSERT_TRUE(new_meta_file.exists());
        // Old table should remain in db
        auto old_storage = db->tryGetTable(ctx, tbl_name);
        ASSERT_NE(old_storage, nullptr);
        // New table is not exists in db2
        auto new_storage = db2->tryGetTable(ctx, tbl_name);
        ASSERT_EQ(new_storage, nullptr);
    }

    {
        // If we loadTable for db2, new table meta should be removed.
        ThreadPool thread_pool(2);
        db2->loadTables(ctx, &thread_pool, true);

        Poco::File new_meta_file(db2->getTableMetadataPath(to_tbl_name));
        ASSERT_FALSE(new_meta_file.exists());

        auto storage = db2->tryGetTable(ctx, to_tbl_name);
        ASSERT_EQ(storage, nullptr);
    }

    {
        // Drop table
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = db_name;
        drop_query->table = tbl_name;
        drop_query->if_exists = false;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, ctx);
        drop_interpreter.execute();

        auto storage = db->tryGetTable(ctx, tbl_name);
        ASSERT_EQ(storage, nullptr);
    }
}
CATCH

TEST_F(DatabaseTiFlash_test, RenameTableOnlyUpdateDisplayName)
try
{
    TiFlashTestEnv::setupLogger();

    const String db_name = "db_1";
    auto ctx = TiFlashTestEnv::getContext();

    {
        // Create database
        const String statement = "CREATE DATABASE " + db_name + " ENGINE=TiFlash";
        ASTPtr ast = parseCreateStatement(statement);
        InterpreterCreateQuery interpreter(ast, ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    auto db = ctx.getDatabase(db_name);

    const String tbl_name = "t_111";
    {
        /// Create table
        ParserCreateQuery parser;
        const String stmt = "CREATE TABLE `" + db_name + "`.`" + tbl_name + "`" + R"(
            (i Nullable(Int32), f Nullable(Float32), _tidb_rowid Int64) ENGINE = DeltaMerge(_tidb_rowid,
            '{"cols":[{"comment":"","default":null,"id":1,"name":{"L":"i","O":"i"},"offset":0,"origin_default":null,"state":5,"type":{"Decimal":0,"Elems":null,"Flag":0,"Flen":11,"Tp":3}},{"comment":"","default":null,"id":2,"name":{"L":"f","O":"f"},"offset":1,"origin_default":null,"state":5,"type":{"Decimal":-1,"Elems":null,"Flag":0,"Flen":12,"Tp":4}}],"comment":"","id":50,"name":{"L":"t","O":"t"},"partition":null,"pk_is_handle":false,"schema_version":27,"state":5,"update_timestamp":415992658599346193}')
            )";
        ASTPtr ast = parseQuery(parser, stmt, 0);

        InterpreterCreateQuery interpreter(ast, ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    EXPECT_FALSE(db->empty(ctx));
    EXPECT_TRUE(db->isTableExist(ctx, tbl_name));

    {
        // Get storage from database
        auto storage = db->tryGetTable(ctx, tbl_name);
        ASSERT_NE(storage, nullptr);

        EXPECT_EQ(storage->getName(), MutableSupport::delta_tree_storage_name);
        EXPECT_EQ(storage->getTableName(), tbl_name);

        auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
        EXPECT_EQ(managed_storage->getDatabaseName(), db_name);
        EXPECT_EQ(managed_storage->getTableInfo().name, "t");
    }

    const String new_display_tbl_name = "accounts";
    {
        // Rename table with only display table name updated.
        typeid_cast<DatabaseTiFlash *>(db.get())->renameTable(ctx, tbl_name, *db, tbl_name, db_name, new_display_tbl_name);

        auto storage = db->tryGetTable(ctx, tbl_name);
        ASSERT_NE(storage, nullptr);
        EXPECT_EQ(storage->getName(), MutableSupport::delta_tree_storage_name);
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
        auto deatched_db = ctx.detachDatabase(db_name);

        // Attach database
        create_db_ast = deatched_db->getCreateDatabaseQuery(ctx);
        deatched_db->shutdown();
        deatched_db.reset();
    }
    {
        InterpreterCreateQuery interpreter(create_db_ast, ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();

        // Get database
        auto db = ctx.tryGetDatabase(db_name);
        ASSERT_NE(db, nullptr);
        EXPECT_TRUE(db->isTableExist(ctx, tbl_name));
        EXPECT_EQ(db->getEngineName(), "TiFlash");
        // Get storage from database
        auto storage = db->tryGetTable(ctx, tbl_name);
        ASSERT_NE(storage, nullptr);
        EXPECT_EQ(storage->getName(), MutableSupport::delta_tree_storage_name);
        EXPECT_EQ(storage->getTableName(), tbl_name);

        auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
        EXPECT_EQ(managed_storage->getDatabaseName(), db_name);
        EXPECT_EQ(managed_storage->getTableInfo().name, new_display_tbl_name);
    }
}
CATCH

TEST_F(DatabaseTiFlash_test, ISSUE_1055)
try
{
    CHECK_TESTS_WITH_DATA_ENABLED;

    // Generated by running these SQL on cluster version v4.0.0~v4.0.3
    // > create table test.t(a int primary key);
    // > alter table test.t change a a2 int;

    TiFlashTestEnv::setupLogger();
    const String db_name = "db_1";
    auto ctx = TiFlashTestEnv::getContext();

    {
        // Create database
        const String statement = "CREATE DATABASE " + db_name + " ENGINE=TiFlash";
        ASTPtr ast = parseCreateStatement(statement);
        ASSERT_NE(ast, nullptr);
        InterpreterCreateQuery interpreter(ast, ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    auto db = ctx.tryGetDatabase(db_name);
    ASSERT_NE(db, nullptr);

    String meta_path;
    {
        auto paths = TiFlashTestEnv::findTestDataPath("issue-1055");
        ASSERT_EQ(paths.size(), 1UL);
        meta_path = paths[0];
    }
    auto db_data_path = TiFlashTestEnv::getContext().getPath() + "/data/";
    DatabaseLoading::loadTable(ctx, *db, meta_path, db_name, db_data_path, "TiFlash", "t_45.sql", false);

    // Get storage from database
    const auto tbl_name = "t_45";
    auto storage = db->tryGetTable(ctx, tbl_name);
    ASSERT_NE(storage, nullptr);
    EXPECT_EQ(storage->getName(), MutableSupport::delta_tree_storage_name);
    EXPECT_EQ(storage->getTableName(), tbl_name);

    auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
    auto sd = managed_storage->getPrimarySortDescription();
    ASSERT_EQ(sd.size(), 1UL);
    EXPECT_EQ(sd[0].column_name, "a2");
}
CATCH

} // namespace tests
} // namespace DB
