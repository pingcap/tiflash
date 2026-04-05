// Copyright 2024 PingCAP, Inc.
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
#include <Common/StringUtils/StringRefUtils.h>
#include <Databases/DatabaseTiFlash.h>
#include <Debug/MockKVStore/MockUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IManageableStorage.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/FFI/ProxyFFICommon.h>
#include <Storages/KVStore/FFI/ProxyFFIStatusService.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/TMTStorages.h>
#include <Storages/KVStore/Types.h>
#include <Storages/KVStore/tests/region_kvstore_test.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/registerStorages.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Schema/SchemaNameMapper.h>
#include <TiDB/Schema/TiDBSchemaManager.h>

namespace DB::ErrorCodes
{
extern const int SYNTAX_ERROR;
} // namespace DB::ErrorCodes

namespace DB::FailPoints
{
extern const char sync_schema_request_failure[];
extern const char force_return_store_status[];
} // namespace DB::FailPoints

namespace DB::tests
{
class StatusServerTest : public ::testing::Test
{
public:
    StatusServerTest() = default;
    static void SetUpTestCase()
    {
        try
        {
            registerStorages();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already register, ignore exception here.
        }
    }
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

    static void releaseResp(const EngineStoreServerHelper & helper, HttpRequestRes && res)
    {
        // release all raw cpp ptr in HttpRequestRes
        helper.fn_gc_raw_cpp_ptr(res.res.inner.ptr, res.res.inner.type);
        helper.fn_gc_raw_cpp_ptr(res.api_name.inner.ptr, res.api_name.inner.type);
    }
};

TEST_F(StatusServerTest, TestSyncSchema)
try
{
    auto ctx = TiFlashTestEnv::getContext();
    auto pd_client = ctx->getGlobalContext().getTMTContext().getPDClient();

    MockTiDB::instance().newDataBase("db_1");
    auto cols = ColumnsDescription({
        {"col1", typeFromString("Int64")},
    });
    auto table_id = MockTiDB::instance().newTable("db_1", "t_1", cols, pd_client->getTS(), "");
    auto schema_syncer = ctx->getTMTContext().getSchemaSyncerManager();
    KeyspaceID keyspace_id = NullspaceID;
    schema_syncer->syncSchemas(ctx->getGlobalContext(), keyspace_id);

    EngineStoreServerWrap store_server_wrap{};
    store_server_wrap.tmt = &ctx->getTMTContext();
    auto helper = GetEngineStoreServerHelper(&store_server_wrap);

    {
        String path = fmt::format("/tiflash/sync-schema/keyspace/{}/table/{}", keyspace_id, table_id);
        auto res = helper.fn_handle_http_request(
            &store_server_wrap,
            BaseBuffView{path.data(), path.length()},
            BaseBuffView{path.data(), path.length()},
            BaseBuffView{"", 0});
        EXPECT_EQ(res.status, HttpRequestStatus::Ok);
        // normal errmsg is nil.
        EXPECT_EQ(res.res.view.len, 0);
        releaseResp(helper, std::move(res));
    }

    {
        // do sync table schema twice
        String path = fmt::format("/tiflash/sync-schema/keyspace/{}/table/{}", keyspace_id, table_id);
        auto res = helper.fn_handle_http_request(
            &store_server_wrap,
            BaseBuffView{path.data(), path.length()},
            BaseBuffView{path.data(), path.length()},
            BaseBuffView{"", 0});
        EXPECT_EQ(res.status, HttpRequestStatus::Ok) << magic_enum::enum_name(res.status);
        // normal errmsg is nil.
        EXPECT_EQ(res.res.view.len, 0);
        releaseResp(helper, std::move(res));
    }

    {
        // test wrong table ID
        TableID wrong_table_id = table_id + 1;
        String path = fmt::format("/tiflash/sync-schema/keyspace/{}/table/{}", keyspace_id, wrong_table_id);
        auto res_err = helper.fn_handle_http_request(
            &store_server_wrap,
            BaseBuffView{path.data(), path.length()},
            BaseBuffView{path.data(), path.length()},
            BaseBuffView{"", 0});
        EXPECT_EQ(res_err.status, HttpRequestStatus::InternalError) << magic_enum::enum_name(res_err.status);
        StringRef sr(res_err.res.view.data, res_err.res.view.len);
        EXPECT_EQ(sr.toString(), "{\"errMsg\":\"sync schema failed\"}");
        releaseResp(helper, std::move(res_err));
    }

    // test sync schema failed
    {
        String path = fmt::format("/tiflash/sync-schema/keyspace/{}/table/{}", keyspace_id, table_id);
        FailPointHelper::enableFailPoint(FailPoints::sync_schema_request_failure);
        auto res_err1 = helper.fn_handle_http_request(
            &store_server_wrap,
            BaseBuffView{path.data(), path.length()},
            BaseBuffView{path.data(), path.length()},
            BaseBuffView{"", 0});
        EXPECT_EQ(res_err1.status, HttpRequestStatus::InternalError) << magic_enum::enum_name(res_err1.status);
        StringRef sr(res_err1.res.view.data, res_err1.res.view.len);
        EXPECT_EQ(sr.toString(), "{\"errMsg\":\"Fail point FailPoints::sync_schema_request_failure is triggered.\"}");
        releaseResp(helper, std::move(res_err1));
    }

    dropDataBase("db_1");
}
CATCH

TEST_F(StatusServerTest, TestReadyz)
try
{
    auto ctx = TiFlashTestEnv::getContext();

    EngineStoreServerWrap store_server_wrap{};
    store_server_wrap.tmt = &ctx->getTMTContext();
    auto helper = GetEngineStoreServerHelper(&store_server_wrap);

    {
        FailPointHelper::enableFailPoint(FailPoints::force_return_store_status, TMTContext::StoreStatus::Running);
        SCOPE_EXIT(FailPointHelper::disableFailPoint(FailPoints::force_return_store_status));
        // is ready for serving
        String path = "/tiflash/readyz";
        String query = "verbose";
        auto res = helper.fn_handle_http_request(
            &store_server_wrap,
            BaseBuffView{path.data(), path.length()},
            BaseBuffView{query.data(), query.length()},
            BaseBuffView{"", 0});
        EXPECT_EQ(res.status, HttpRequestStatus::Ok) << magic_enum::enum_name(res.status);
        // normal response body is non-nil.
        EXPECT_NE(res.res.view.len, 0);
        // normal response contains store_status ok info
        EXPECT_NE(std::string_view(res.res.view.data).find("[+]store_status ok"), std::string_view::npos);
        releaseResp(helper, std::move(res));
    }

    {
        FailPointHelper::enableFailPoint(FailPoints::force_return_store_status, TMTContext::StoreStatus::Ready);
        SCOPE_EXIT(FailPointHelper::disableFailPoint(FailPoints::force_return_store_status));
        String path = "/tiflash/readyz";
        String query = "verbose";
        auto res = helper.fn_handle_http_request(
            &store_server_wrap,
            BaseBuffView{path.data(), path.length()},
            BaseBuffView{query.data(), query.length()},
            BaseBuffView{"", 0});
        EXPECT_EQ(res.status, HttpRequestStatus::InternalError) << magic_enum::enum_name(res.status);
        // normal response body is non-nil.
        EXPECT_NE(res.res.view.len, 0);
        // error response contains store_status fail info
        EXPECT_NE(std::string_view(res.res.view.data).find("[-]store_status fail:"), std::string_view::npos);
        releaseResp(helper, std::move(res));
    }
}
CATCH


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

        EXPECT_EQ(storage->getName(), MutSup::delta_tree_storage_name);
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
        auto region = RegionBench::makeRegionForTable(i, table_id, i, i + region_num + 10);
        tmt.getRegionTable().shrinkRegionRange(*region);
    }
}

void makeRegionsLag(size_t lag_num)
{
    auto & tmt = TiFlashTestEnv::getContext()->getTMTContext();
    for (size_t i = 0; i < lag_num; i++)
    {
        tmt.getRegionTable().safeTsMgr().updateSafeTS(
            i,
            (SafeTsManager::SafeTsDiffThreshold + 1) << TsoPhysicalShiftBits,
            0);
    }
}

TEST_F(StatusServerTest, TestSyncStatusLagRegion)
try
{
    TableID table_id = createDBAndTable("db_1", "t_1");
    // create 20 region for the table_id, but 10 regions are lagged
    createRegions(20, table_id);
    makeRegionsLag(10);
    EngineStoreServerWrap store_server_wrap{};
    store_server_wrap.tmt = &TiFlashTestEnv::getContext()->getTMTContext();
    auto helper = GetEngineStoreServerHelper(&store_server_wrap);
    String path = fmt::format("/tiflash/sync-status/{}", table_id);
    auto res = helper.fn_handle_http_request(
        &store_server_wrap,
        BaseBuffView{path.data(), path.length()},
        BaseBuffView{path.data(), path.length()},
        BaseBuffView{"", 0});
    EXPECT_EQ(res.status, HttpRequestStatus::Ok);
    {
        // normal region count is 10.
        StringRef sr(res.res.view.data, res.res.view.len);
        EXPECT_TRUE(startsWith(sr, "10\n"));
        EXPECT_EQ(res.res.view.len, 33);
        sr = removePrefix(sr, 3);
        // parse the region_ids
        std::stringstream ss(sr.toString());
        std::set<RegionID> region_ids;
        RegionID region_id;
        while (ss >> region_id)
        {
            ASSERT_GE(region_id, 10);
            ASSERT_LT(region_id, 20);
            region_ids.insert(region_id);
        }
        ASSERT_EQ(region_ids.size(), 10) << fmt::format("{} ", region_ids);
    }
    releaseResp(helper, std::move(res));
    dropDataBase("db_1");
}
CATCH

TEST_F(StatusServerTest, TestSyncStatusNormalRegion)
try
{
    TableID table_id = createDBAndTable("db_1", "t_1");
    createRegions(20, table_id);
    EngineStoreServerWrap store_server_wrap{};
    store_server_wrap.tmt = &TiFlashTestEnv::getContext()->getTMTContext();
    auto helper = GetEngineStoreServerHelper(&store_server_wrap);
    String path = fmt::format("/tiflash/sync-status/{}", table_id);
    auto res = helper.fn_handle_http_request(
        &store_server_wrap,
        BaseBuffView{path.data(), path.length()},
        BaseBuffView{path.data(), path.length()},
        BaseBuffView{"", 0});
    EXPECT_EQ(res.status, HttpRequestStatus::Ok);
    {
        // normal region count is 20.
        StringRef sr(res.res.view.data, res.res.view.len);
        EXPECT_TRUE(startsWith(sr, "20\n"));
        EXPECT_EQ(res.res.view.len, 53);
        sr = removePrefix(sr, 3);
        // parse the region_ids
        std::stringstream ss(sr.toString());
        std::set<RegionID> region_ids;
        RegionID region_id;
        while (ss >> region_id)
        {
            ASSERT_GE(region_id, 0);
            ASSERT_LT(region_id, 20);
            region_ids.insert(region_id);
        }
        ASSERT_EQ(region_ids.size(), 20) << fmt::format("{} ", region_ids);
    }
    releaseResp(helper, std::move(res));
    dropDataBase("db_1");
}
CATCH

TEST_F(StatusServerTest, TestParseHttpQueryMap)
{
    {
        std::string_view query = "key1=val1&key2=val2&key3=val3";
        auto query_map = parseHttpQueryMap(query);
        ASSERT_EQ(query_map.size(), 3);
        ASSERT_EQ(query_map["key1"], "val1");
        ASSERT_EQ(query_map["key2"], "val2");
        ASSERT_EQ(query_map["key3"], "val3");
    }
    {
        std::string_view query = "key1=val1&key2=&key3=val3";
        auto query_map = parseHttpQueryMap(query);
        ASSERT_EQ(query_map.size(), 3);
        ASSERT_EQ(query_map["key1"], "val1");
        ASSERT_EQ(query_map["key2"], "");
        ASSERT_EQ(query_map["key3"], "val3");
    }
    {
        std::string_view query = "key1=val1&key2&key3=val3";
        auto query_map = parseHttpQueryMap(query);
        ASSERT_EQ(query_map.size(), 3);
        ASSERT_EQ(query_map["key1"], "val1");
        ASSERT_EQ(query_map["key2"], "");
        ASSERT_EQ(query_map["key3"], "val3");
    }
    {
        std::string_view query;
        auto query_map = parseHttpQueryMap(query);
        ASSERT_EQ(query_map.size(), 0);
    }
    {
        std::string_view query = "&";
        auto query_map = parseHttpQueryMap(query);
        ASSERT_EQ(query_map.size(), 0);
    }
    {
        std::string_view query = "key1=val1&&key3=val3";
        auto query_map = parseHttpQueryMap(query);
        ASSERT_EQ(query_map.size(), 2);
        ASSERT_EQ(query_map["key1"], "val1");
        ASSERT_EQ(query_map["key3"], "val3");
    }
    {
        std::string_view query = "key1=val1&key2=val2&key3=val3&";
        auto query_map = parseHttpQueryMap(query);
        ASSERT_EQ(query_map.size(), 3);
        ASSERT_EQ(query_map["key1"], "val1");
        ASSERT_EQ(query_map["key2"], "val2");
        ASSERT_EQ(query_map["key3"], "val3");
    }
    {
        std::string_view query = "key1=val1&key2=val2&=val3";
        auto query_map = parseHttpQueryMap(query);
        ASSERT_EQ(query_map.size(), 3);
        ASSERT_EQ(query_map["key1"], "val1");
        ASSERT_EQ(query_map["key2"], "val2");
        ASSERT_EQ(query_map[""], "val3");
    }
}

TEST_F(StatusServerTest, TestParseRemoteCacheEvictRequest)
{
    const std::string api_name = "/tiflash/remote/cache/evict";
    {
        std::string path = api_name + "/5";
        RemoteCacheEvictRequest req = parseEvictRequest(path, api_name, "");
        ASSERT_TRUE(req.err_msg.empty());
        ASSERT_EQ(req.evict_method, EvictMethod::ByFileType);
        ASSERT_EQ(req.evict_type, FileSegment::FileType::Merged);
    }
    {
        std::string path = api_name + "/type/5";
        RemoteCacheEvictRequest req = parseEvictRequest(path, api_name, "");
        ASSERT_TRUE(req.err_msg.empty());
        ASSERT_EQ(req.evict_method, EvictMethod::ByFileType);
        ASSERT_EQ(req.evict_type, FileSegment::FileType::Merged);
    }
    {
        std::string path = api_name + "/type/0";
        RemoteCacheEvictRequest req = parseEvictRequest(path, api_name, "");
        ASSERT_TRUE(req.err_msg.empty());
        ASSERT_EQ(req.evict_method, EvictMethod::ByFileType);
        ASSERT_EQ(req.evict_type, FileSegment::FileType::Unknown);
    }

    // test evict by size
    {
        std::string path = api_name + "/size/102400";
        RemoteCacheEvictRequest req = parseEvictRequest(path, api_name, "");
        ASSERT_TRUE(req.err_msg.empty());
        ASSERT_EQ(req.evict_method, EvictMethod::ByEvictSize);
        ASSERT_EQ(req.reserve_size, 102400);
        ASSERT_EQ(req.min_age, 0);
        ASSERT_EQ(req.force_evict, false);
    }
    {
        std::string path = api_name + "/size/20480";
        RemoteCacheEvictRequest req = parseEvictRequest(path, api_name, "");
        ASSERT_TRUE(req.err_msg.empty());
        ASSERT_EQ(req.evict_method, EvictMethod::ByEvictSize);
        ASSERT_EQ(req.reserve_size, 20480);
        ASSERT_EQ(req.min_age, 0);
        ASSERT_EQ(req.force_evict, false);
    }
    {
        std::string path = api_name + "/size/20480";
        // now we only check the existence of "force" in query string
        RemoteCacheEvictRequest req = parseEvictRequest(path, api_name, "force");
        ASSERT_TRUE(req.err_msg.empty());
        ASSERT_EQ(req.evict_method, EvictMethod::ByEvictSize);
        ASSERT_EQ(req.reserve_size, 20480);
        ASSERT_EQ(req.min_age, 0);
        ASSERT_EQ(req.force_evict, true);
    }
    {
        std::string path = api_name + "/size/20480";
        RemoteCacheEvictRequest req = parseEvictRequest(path, api_name, "force=true");
        ASSERT_TRUE(req.err_msg.empty());
        ASSERT_EQ(req.evict_method, EvictMethod::ByEvictSize);
        ASSERT_EQ(req.reserve_size, 20480);
        ASSERT_EQ(req.min_age, 0);
        ASSERT_EQ(req.force_evict, true);
    }
    {
        std::string path = api_name + "/size/20480";
        RemoteCacheEvictRequest req = parseEvictRequest(path, api_name, "force=false");
        ASSERT_TRUE(req.err_msg.empty());
        ASSERT_EQ(req.evict_method, EvictMethod::ByEvictSize);
        ASSERT_EQ(req.reserve_size, 20480);
        ASSERT_EQ(req.min_age, 0);
        ASSERT_EQ(req.force_evict, false);
    }
    {
        std::string path = api_name + "/size/20480";
        RemoteCacheEvictRequest req = parseEvictRequest(path, api_name, "age=123&force=true");
        ASSERT_TRUE(req.err_msg.empty());
        ASSERT_EQ(req.evict_method, EvictMethod::ByEvictSize);
        ASSERT_EQ(req.reserve_size, 20480);
        ASSERT_EQ(req.min_age, 123); // valid age param
        ASSERT_EQ(req.force_evict, true);
    }
    {
        std::string path = api_name + "/size/20480";
        RemoteCacheEvictRequest req = parseEvictRequest(path, api_name, "age=abd&force=true");
        ASSERT_TRUE(req.err_msg.empty());
        ASSERT_EQ(req.evict_method, EvictMethod::ByEvictSize);
        ASSERT_EQ(req.reserve_size, 20480);
        ASSERT_EQ(req.min_age, 0); // invalid age param, min_age is 0
        ASSERT_EQ(req.force_evict, true);
    }

    for (const auto & invalid_suffix : {
             // empty suffix
             "",
             // invalid type
             "/invalid_type",
             "/1000",
             "/-1",
             // invalid type with "/type" prefix
             "/type/invalid_type",
             "/type/1000",
             "/type/-1",
             // invalid size
             "/size/invalid_size",
             "/size/-1000",
         })
    {
        std::string path = api_name + invalid_suffix;
        RemoteCacheEvictRequest req = parseEvictRequest(path, api_name, "");
        LOG_INFO(Logger::get(), "path={} err_msg={}", path, req.err_msg);
        EXPECT_FALSE(req.err_msg.empty()) << fmt::format("path={} req={}", path, req);
    }
}

} // namespace DB::tests
