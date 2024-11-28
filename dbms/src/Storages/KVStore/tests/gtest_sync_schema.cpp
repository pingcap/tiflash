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
#include <Databases/DatabaseTiFlash.h>
#include <Interpreters/Context.h>
#include <Storages/IManageableStorage.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/FFI/ProxyFFICommon.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/TMTStorages.h>
#include <Storages/KVStore/Types.h>
#include <Storages/KVStore/tests/region_helper.h>
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
} // namespace DB::FailPoints

namespace DB::tests
{
class SyncSchemaTest : public ::testing::Test
{
public:
    SyncSchemaTest() = default;
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
};

TEST_F(SyncSchemaTest, TestNormal)
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
    String path = fmt::format("/tiflash/sync-schema/keyspace/{}/table/{}", keyspace_id, table_id);
    auto res = helper.fn_handle_http_request(
        &store_server_wrap,
        BaseBuffView{path.data(), path.length()},
        BaseBuffView{path.data(), path.length()},
        BaseBuffView{"", 0});
    EXPECT_EQ(res.status, HttpRequestStatus::Ok);
    {
        // normal errmsg is nil.
        EXPECT_EQ(res.res.view.len, 0);
    }
    delete (static_cast<RawCppString *>(res.res.inner.ptr));

    // do sync table schema twice
    {
        path = fmt::format("/tiflash/sync-schema/keyspace/{}/table/{}", keyspace_id, table_id);
        auto res = helper.fn_handle_http_request(
            &store_server_wrap,
            BaseBuffView{path.data(), path.length()},
            BaseBuffView{path.data(), path.length()},
            BaseBuffView{"", 0});
        EXPECT_EQ(res.status, HttpRequestStatus::Ok);
        {
            // normal errmsg is nil.
            EXPECT_EQ(res.res.view.len, 0);
        }
        delete (static_cast<RawCppString *>(res.res.inner.ptr));
    }

    // test wrong table ID
    {
        TableID wrong_table_id = table_id + 1;
        path = fmt::format("/tiflash/sync-schema/keyspace/{}/table/{}", keyspace_id, wrong_table_id);
        auto res_err = helper.fn_handle_http_request(
            &store_server_wrap,
            BaseBuffView{path.data(), path.length()},
            BaseBuffView{path.data(), path.length()},
            BaseBuffView{"", 0});
        EXPECT_EQ(res_err.status, HttpRequestStatus::ErrorParam);
        StringRef sr(res_err.res.view.data, res_err.res.view.len);
        {
            EXPECT_EQ(sr.toString(), "{\"errMsg\":\"sync schema failed\"}");
        }
        delete (static_cast<RawCppString *>(res_err.res.inner.ptr));
    }

    // test sync schema failed
    {
        path = fmt::format("/tiflash/sync-schema/keyspace/{}/table/{}", keyspace_id, table_id);
        FailPointHelper::enableFailPoint(FailPoints::sync_schema_request_failure);
        auto res_err1 = helper.fn_handle_http_request(
            &store_server_wrap,
            BaseBuffView{path.data(), path.length()},
            BaseBuffView{path.data(), path.length()},
            BaseBuffView{"", 0});
        EXPECT_EQ(res_err1.status, HttpRequestStatus::ErrorParam);
        StringRef sr(res_err1.res.view.data, res_err1.res.view.len);
        {
            EXPECT_EQ(
                sr.toString(),
                "{\"errMsg\":\"Fail point FailPoints::sync_schema_request_failure is triggered.\"}");
        }
        delete (static_cast<RawCppString *>(res_err1.res.inner.ptr));
    }

    dropDataBase("db_1");
}
CATCH

} // namespace DB::tests
