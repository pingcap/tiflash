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
#include <test_utils/TiflashTestBasic.h>

#include <optional>

namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
} // namespace ErrorCodes

namespace tests
{

class DatabaseTiFlash_test : public ::testing::Test
{
public:
    constexpr static const char * TEST_DB_NAME = "test";

    DatabaseTiFlash_test() { registerStorages(); }

    void SetUp() override { recreateMetadataPath(); }

    void recreateMetadataPath() const
    {
        String path = TiFlashTestEnv::getContext().getPath() + "metadata/";
        if (Poco::File file(path); file.exists())
            file.remove(true);
        Poco::File(path).createDirectory();
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
        const String statement = "CREATE DATABASE IF NOT EXISTS " + db_name + " ENGINE=TiFlash";
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

} // namespace tests
} // namespace DB
