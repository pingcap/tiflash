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
#include <Databases/DatabaseTiFlash.h>
#include <Encryption/ReadBufferFromFileProvider.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/File.h>
#include <Storages/IManageableStorage.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/ProxyFFICommon.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TMTStorages.h>
#include <Storages/Transaction/tests/region_helper.h>
#include <Storages/registerStorages.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Schema/SchemaNameMapper.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
} // namespace ErrorCodes

namespace FailPoints
{
} // namespace FailPoints

namespace tests
{
class SyncStatusTest : public ::testing::Test
{
public:
    SyncStatusTest()
        = default;
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
    }
    void SetUp() override
    {
        recreateMetadataPath();
    }

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
};


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

TableID createDBAndTable(String db_name, String table_name)
{
    auto ctx = TiFlashTestEnv::getContext();
    {
        // Create database
        const String statement = "CREATE DATABASE " + db_name + " ENGINE=TiFlash";
        ASTPtr ast = parseCreateStatement(statement);
        EXPECT_NE(ast, nullptr);
        InterpreterCreateQuery interpreter(ast, *ctx);
        interpreter.setInternal(true);
        interpreter.setForceRestoreData(false);
        interpreter.execute();
    }

    auto db = ctx->tryGetDatabase(db_name);
    EXPECT_NE(db, nullptr);
    EXPECT_EQ(db->getEngineName(), "TiFlash");
    EXPECT_TRUE(db->empty(*ctx));

    {
        /// Create table
        ParserCreateQuery parser;
        const String stmt = "CREATE TABLE `" + db_name + "`.`" + table_name
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
    EXPECT_TRUE(db->isTableExist(*ctx, table_name));

    TableID table_id;
    {
        // Get storage from database
        auto storage = db->tryGetTable(*ctx, table_name);
        EXPECT_NE(storage, nullptr);

        StorageDeltaMergePtr storage_ptr = std::static_pointer_cast<StorageDeltaMerge>(storage);
        table_id = storage_ptr->getTableInfo().id;

        EXPECT_EQ(storage->getName(), MutableSupport::delta_tree_storage_name);
        EXPECT_EQ(storage->getTableName(), table_name);

        auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
        EXPECT_EQ(managed_storage->getDatabaseName(), db_name);
    }
    return table_id;
}

void dropDataBase(String db_name)
{
    auto ctx = TiFlashTestEnv::getContext();
    auto drop_query = std::make_shared<ASTDropQuery>();
    drop_query->database = db_name;
    drop_query->if_exists = false;
    ASTPtr ast_drop_query = drop_query;
    InterpreterDropQuery drop_interpreter(ast_drop_query, *ctx);
    drop_interpreter.execute();

    auto db = ctx->tryGetDatabase(db_name);
    ASSERT_EQ(db, nullptr);
}

void createRegions(size_t region_num, TableID table_id)
{
    auto & tmt = TiFlashTestEnv::getContext()->getTMTContext();
    for (size_t i = 0; i < region_num; i++)
    {
        auto region = makeRegion(i, RecordKVFormat::genKey(table_id, i), RecordKVFormat::genKey(table_id, i + region_num + 10));
        tmt.getRegionTable().shrinkRegionRange(*region);
    }
}

void makeRegionsLag(size_t lag_num)
{
    auto & tmt = TiFlashTestEnv::getContext()->getTMTContext();
    for (size_t i = 0; i < lag_num; i++)
    {
        tmt.getRegionTable().updateSafeTS(i, (RegionTable::SafeTsDiffThreshold + 1) << TsoPhysicalShiftBits, 0);
    }
}

TEST_F(SyncStatusTest, TestLagRegion)
try
{
    TableID table_id = createDBAndTable("db_1", "t_1");
    createRegions(20, table_id);
    makeRegionsLag(10);
    EngineStoreServerWrap store_server_wrap{};
    store_server_wrap.tmt = &TiFlashTestEnv::getContext()->getTMTContext();
    auto helper = GetEngineStoreServerHelper(&store_server_wrap);
    String path = fmt::format("/tiflash/sync-status/{}", table_id);
    auto res = helper.fn_handle_http_request(&store_server_wrap, BaseBuffView{path.data(), path.length()}, BaseBuffView{path.data(), path.length()}, BaseBuffView{"", 0});
    EXPECT_EQ(res.status, HttpRequestStatus::Ok);
    // normal region count is 10.
    EXPECT_EQ(res.res.view.data[0], '1');
    EXPECT_EQ(res.res.view.data[1], '0');
    delete (static_cast<RawCppString *>(res.res.inner.ptr));
    dropDataBase("db_1");
}
CATCH

TEST_F(SyncStatusTest, TestNormalRegion)
try
{
    TableID table_id = createDBAndTable("db_1", "t_1");
    createRegions(20, table_id);
    EngineStoreServerWrap store_server_wrap{};
    store_server_wrap.tmt = &TiFlashTestEnv::getContext()->getTMTContext();
    auto helper = GetEngineStoreServerHelper(&store_server_wrap);
    String path = fmt::format("/tiflash/sync-status/{}", table_id);
    auto res = helper.fn_handle_http_request(&store_server_wrap, BaseBuffView{path.data(), path.length()}, BaseBuffView{path.data(), path.length()}, BaseBuffView{"", 0});
    EXPECT_EQ(res.status, HttpRequestStatus::Ok);
    // normal region count is 20.
    EXPECT_EQ(res.res.view.data[0], '2');
    EXPECT_EQ(res.res.view.data[1], '0');
    delete (static_cast<RawCppString *>(res.res.inner.ptr));
    dropDataBase("db_1");
}
CATCH

} // namespace tests
} // namespace DB
