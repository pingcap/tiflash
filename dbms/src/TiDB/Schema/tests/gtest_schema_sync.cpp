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

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Databases/IDatabase.h>
#include <Debug/MockTiDB.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/IAST.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IManageableStorage.h>
#include <Storages/KVStore/Decode/RegionBlockReader.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Types.h>
#include <Storages/registerStorages.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <TiDB/Schema/SchemaSyncService.h>
#include <TiDB/Schema/TiDBSchemaManager.h>
#include <common/defines.h>

#include <ext/scope_guard.h>
#include <limits>

namespace DB
{
namespace FailPoints
{
extern const char exception_before_rename_table_old_meta_removed[];
extern const char force_context_path[];
extern const char force_set_num_regions_for_table[];
} // namespace FailPoints
} // namespace DB
namespace DB::tests
{
class SchemaSyncTest : public ::testing::Test
{
public:
    SchemaSyncTest()
        : global_ctx(TiFlashTestEnv::getGlobalContext())
    {}

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

    void SetUp() override
    {
        // unit test.
        // Get DBInfo/TableInfo from MockTiDB, but create table with names `t_${table_id}`
        auto cluster = std::make_shared<pingcap::kv::Cluster>();
        schema_sync_manager = std::make_unique<TiDBSchemaSyncerManager>(
            cluster,
            /*mock_getter*/ true,
            /*mock_mapper*/ false);

        // disable schema sync timer
        global_ctx.getSchemaSyncService().reset();
        recreateMetadataPath();
    }

    void TearDown() override
    {
        for (auto & [db_name, db_id] : MockTiDB::instance().getDatabases())
        {
            UNUSED(db_id);
            MockTiDB::instance().dropDB(global_ctx, db_name, false);
        }
        for (auto & db : global_ctx.getDatabases())
        {
            mustDropSyncedDatabase(db.first);
        }

        // restore schema sync timer
        if (!global_ctx.getSchemaSyncService())
            global_ctx.initializeSchemaSyncService();
    }

    // Sync schema info from TiDB/MockTiDB to TiFlash
    void refreshSchema()
    {
        try
        {
            schema_sync_manager->syncSchemas(global_ctx, NullspaceID);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::FAIL_POINT_ERROR)
            {
                return;
            }
            else
            {
                throw;
            }
        }
    }

    void refreshTableSchema(TableID table_id)
    {
        try
        {
            schema_sync_manager->syncTableSchema(global_ctx, NullspaceID, table_id);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::FAIL_POINT_ERROR)
            {
                return;
            }
            else
            {
                throw;
            }
        }
    }

    // Reset the schema syncer to mock TiFlash shutdown
    void resetSchemas() { schema_sync_manager->reset(NullspaceID); }

    // Get the TiFlash synced table
    ManageableStoragePtr mustGetSyncedTable(TableID table_id)
    {
        auto & flash_ctx = global_ctx.getTMTContext();
        auto & flash_storages = flash_ctx.getStorages();
        auto tbl = flash_storages.get(NullspaceID, table_id);
        RUNTIME_CHECK_MSG(tbl, "Can not find table in TiFlash instance! table_id={}", table_id);
        return tbl;
    }

    // Get the TiFlash synced table
    // `db_name`, `tbl_name` is the name from the TiDB-server side
    ManageableStoragePtr mustGetSyncedTableByName(const String & db_name, const String & tbl_name)
    {
        auto & flash_ctx = global_ctx.getTMTContext();
        auto & flash_storages = flash_ctx.getStorages();
        auto mock_tbl = MockTiDB::instance().getTableByName(db_name, tbl_name);
        auto tbl = flash_storages.get(NullspaceID, mock_tbl->id());
        RUNTIME_CHECK_MSG(tbl, "Can not find table in TiFlash instance! db_name={}, tbl_name={}", db_name, tbl_name);
        return tbl;
    }

    /*
     * Helper methods work with `db_${database_id}`.`t_${table_id}` in the TiFlash side
     */

    void mustDropSyncedTable(const String & db_idname, const String & tbl_idname)
    {
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = db_idname;
        drop_query->table = tbl_idname;
        drop_query->if_exists = false;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, global_ctx);
        drop_interpreter.execute();
    }

    void mustDropSyncedDatabase(const String & db_idname)
    {
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = db_idname;
        drop_query->if_exists = false;
        InterpreterDropQuery drop_interpreter(drop_query, global_ctx);
        drop_interpreter.execute();
    }

    static std::optional<Timestamp> lastGcSafePoint(const SchemaSyncServicePtr & sync_service, KeyspaceID keyspace_id)
    {
        return sync_service->lastGcSafePoint(keyspace_id);
    }

private:
    static void recreateMetadataPath()
    {
        String path = TiFlashTestEnv::getContext()->getPath();
        auto p = path + "/metadata/";
        TiFlashTestEnv::tryRemovePath(p, /*recreate=*/true);
        p = path + "/data/";
        TiFlashTestEnv::tryRemovePath(p, /*recreate=*/true);
    }

protected:
    Context & global_ctx;

    std::unique_ptr<TiDBSchemaSyncerManager> schema_sync_manager;
};

TEST_F(SchemaSyncTest, SchemaDiff)
try
{
    // Note that if we want to add new fields here, please firstly check if it is present.
    // Otherwise it will break when doing upgrading test.
    SchemaDiff diff;
    std::string data = "{\"version\":40,\"type\":31,\"schema_id\":69,\"table_id\":71,\"old_table_id\":0,\"old_schema_"
                       "id\":0,\"affected_options\":null}";
    ASSERT_NO_THROW(diff.deserialize(data));
}
CATCH

TEST_F(SchemaSyncTest, RenameTables)
try
{
    auto pd_client = global_ctx.getTMTContext().getPDClient();

    const String db_name = "mock_db";
    MockTiDB::instance().newDataBase(db_name);

    auto cols = ColumnsDescription({
        {"col1", typeFromString("String")},
        {"col2", typeFromString("Int64")},
    });
    // table_name, cols, pk_name
    std::vector<std::tuple<String, ColumnsDescription, String>> tables{
        {"t1", cols, ""},
        {"t2", cols, ""},
    };
    auto table_ids = MockTiDB::instance().newTables(db_name, tables, pd_client->getTS(), "dt");

    refreshSchema();

    for (auto table_id : table_ids)
    {
        refreshTableSchema(table_id);
    }

    TableID t1_id = mustGetSyncedTableByName(db_name, "t1")->getTableInfo().id;
    TableID t2_id = mustGetSyncedTableByName(db_name, "t2")->getTableInfo().id;

    // database_name, table_name, new_table_name
    std::vector<std::tuple<String, String, String>> table_rename_map{
        {db_name, "t1", "r1"},
        {db_name, "t2", "r2"},
    };
    MockTiDB::instance().renameTables(table_rename_map);

    refreshSchema();

    ASSERT_EQ(mustGetSyncedTable(t1_id)->getTableInfo().name, "r1");
    ASSERT_EQ(mustGetSyncedTable(t2_id)->getTableInfo().name, "r2");
}
CATCH

TEST_F(SchemaSyncTest, PhysicalDropTable)
try
{
    auto pd_client = global_ctx.getTMTContext().getPDClient();

    const String db_name = "mock_db";
    MockTiDB::instance().newDataBase(db_name);

    auto cols = ColumnsDescription({
        {"col1", typeFromString("String")},
        {"col2", typeFromString("Int64")},
    });
    // table_name, cols, pk_name
    std::vector<std::tuple<String, ColumnsDescription, String>> tables{
        {"t1", cols, ""},
        {"t2", cols, ""},
    };
    auto table_ids = MockTiDB::instance().newTables(db_name, tables, pd_client->getTS(), "dt");

    refreshSchema();
    for (auto table_id : table_ids)
    {
        refreshTableSchema(table_id);
    }

    mustGetSyncedTableByName(db_name, "t1");
    mustGetSyncedTableByName(db_name, "t2");

    MockTiDB::instance().dropTable(global_ctx, db_name, "t1", true);

    refreshSchema();
    for (auto table_id : table_ids)
    {
        refreshTableSchema(table_id);
    }

    // Create a temporary context with ddl sync task disabled
    auto ctx = DB::tests::TiFlashTestEnv::getContext();
    ctx->getSettingsRef().ddl_sync_interval_seconds = 0;
    auto sync_service = std::make_shared<SchemaSyncService>(*ctx);
    sync_service->shutdown(); // shutdown the background tasks

    // run gc with safepoint == 0, will be skip
    ASSERT_FALSE(sync_service->gc(0, NullspaceID));
    ASSERT_TRUE(sync_service->gc(10000000, NullspaceID));
    // run gc with the same safepoint, will be skip
    ASSERT_FALSE(sync_service->gc(10000000, NullspaceID));
    // run gc for another keyspace with same safepoint, will be executed
    ASSERT_TRUE(sync_service->gc(10000000, 1024));
    // run gc with changed safepoint
    ASSERT_TRUE(sync_service->gc(20000000, 1024));
    // run gc with the same safepoint
    ASSERT_FALSE(sync_service->gc(20000000, 1024));
}
CATCH

TEST_F(SchemaSyncTest, PhysicalDropTableMeetsUnRemovedRegions)
try
{
    auto pd_client = global_ctx.getTMTContext().getPDClient();

    const String db_name = "mock_db";
    MockTiDB::instance().newDataBase(db_name);

    auto cols = ColumnsDescription({
        {"col1", typeFromString("String")},
        {"col2", typeFromString("Int64")},
    });
    // table_name, cols, pk_name
    std::vector<std::tuple<String, ColumnsDescription, String>> tables{
        {"t1", cols, ""},
    };
    auto table_ids = MockTiDB::instance().newTables(db_name, tables, pd_client->getTS(), "dt");

    refreshSchema();
    for (auto table_id : table_ids)
    {
        refreshTableSchema(table_id);
    }

    mustGetSyncedTableByName(db_name, "t1");

    MockTiDB::instance().dropTable(global_ctx, db_name, "t1", true);

    refreshSchema();
    for (auto table_id : table_ids)
    {
        refreshTableSchema(table_id);
    }

    // prevent the storage instance from being physically removed
    FailPointHelper::enableFailPoint(
        FailPoints::force_set_num_regions_for_table,
        std::vector<RegionID>{1001, 1002, 1003});
    SCOPE_EXIT({ FailPointHelper::disableFailPoint(FailPoints::force_set_num_regions_for_table); });

    // Create a temporary context with ddl sync task disabled
    auto ctx = DB::tests::TiFlashTestEnv::getContext();
    ctx->getSettingsRef().ddl_sync_interval_seconds = 0;
    auto sync_service = std::make_shared<SchemaSyncService>(*ctx);
    sync_service->shutdown(); // shutdown the background tasks

    {
        // ensure gc_safe_point cache is empty
        auto last_gc_safe_point = lastGcSafePoint(sync_service, NullspaceID);
        ASSERT_FALSE(last_gc_safe_point.has_value());
    }

    // Run GC, but the table is not physically dropped because `force_set_num_regions_for_table`
    ASSERT_FALSE(sync_service->gc(std::numeric_limits<UInt64>::max(), NullspaceID));
    {
        // gc_safe_point cache is not updated
        auto last_gc_safe_point = lastGcSafePoint(sync_service, NullspaceID);
        ASSERT_FALSE(last_gc_safe_point.has_value());
    }

    // ensure the table is not physically dropped
    size_t num_remain_tables = 0;
    for (auto table_id : table_ids)
    {
        auto storage = global_ctx.getTMTContext().getStorages().get(NullspaceID, table_id);
        ASSERT_TRUE(storage->isTombstone());
        ++num_remain_tables;
    }
    ASSERT_EQ(num_remain_tables, 1);
}
CATCH

TEST_F(SchemaSyncTest, RenamePartitionTable)
try
{
    auto pd_client = global_ctx.getTMTContext().getPDClient();

    const String db_name = "mock_db";
    const String tbl_name = "mock_part_tbl";

    auto cols = ColumnsDescription({
        {"col1", typeFromString("String")},
        {"col2", typeFromString("Int64")},
    });

    MockTiDB::instance().newDataBase(db_name);
    auto logical_table_id = MockTiDB::instance().newTable(db_name, tbl_name, cols, pd_client->getTS(), "", "dt");
    auto part1_id
        = MockTiDB::instance().newPartition(logical_table_id, "red", pd_client->getTS(), /*is_add_part*/ true);
    auto part2_id
        = MockTiDB::instance().newPartition(logical_table_id, "blue", pd_client->getTS(), /*is_add_part*/ true);

    // TODO: write some data


    refreshSchema();
    refreshTableSchema(logical_table_id);
    refreshTableSchema(part1_id);
    refreshTableSchema(part2_id);

    // check partition table are created
    // TODO: read from partition table
    {
        auto logical_tbl = mustGetSyncedTable(logical_table_id);
        ASSERT_EQ(logical_tbl->getTableInfo().name, tbl_name);
        auto part1_tbl = mustGetSyncedTable(part1_id);
        ASSERT_EQ(part1_tbl->getTableInfo().name, fmt::format("t_{}", part1_id));
        auto part2_tbl = mustGetSyncedTable(part2_id);
        ASSERT_EQ(part2_tbl->getTableInfo().name, fmt::format("t_{}", part2_id));
    }

    const String new_tbl_name = "mock_part_tbl_renamed";
    MockTiDB::instance().renameTable(db_name, tbl_name, new_tbl_name);
    refreshSchema();

    // check partition table are renamed
    // TODO: read from renamed partition table
    {
        auto logical_tbl = mustGetSyncedTable(logical_table_id);
        ASSERT_EQ(logical_tbl->getTableInfo().name, new_tbl_name);
        auto part1_tbl = mustGetSyncedTable(part1_id);
        ASSERT_EQ(part1_tbl->getTableInfo().name, fmt::format("t_{}", part1_id));
        auto part2_tbl = mustGetSyncedTable(part2_id);
        ASSERT_EQ(part2_tbl->getTableInfo().name, fmt::format("t_{}", part2_id));
    }
}
CATCH

TEST_F(SchemaSyncTest, PartitionTableRestart)
try
{
    auto pd_client = global_ctx.getTMTContext().getPDClient();

    const String db_name = "mock_db";
    const String tbl_name = "mock_part_tbl";

    auto cols = ColumnsDescription({
        {"col_1", typeFromString("String")},
        {"col_2", typeFromString("Int64")},
    });

    auto db_id = MockTiDB::instance().newDataBase(db_name);
    auto logical_table_id = MockTiDB::instance().newTable(db_name, tbl_name, cols, pd_client->getTS(), "", "dt");
    auto part1_id
        = MockTiDB::instance().newPartition(logical_table_id, "red", pd_client->getTS(), /*is_add_part*/ true);
    auto part2_id
        = MockTiDB::instance().newPartition(logical_table_id, "green", pd_client->getTS(), /*is_add_part*/ true);
    auto part3_id
        = MockTiDB::instance().newPartition(logical_table_id, "blue", pd_client->getTS(), /*is_add_part*/ true);

    refreshSchema();
    refreshTableSchema(logical_table_id);
    refreshTableSchema(part1_id);
    refreshTableSchema(part2_id);
    refreshTableSchema(part3_id);
    {
        mustGetSyncedTable(part1_id);
        mustGetSyncedTable(part2_id);
        mustGetSyncedTable(part3_id);
        mustGetSyncedTable(logical_table_id);
    }

    // schema syncer guarantees logical table creation at last, so there won't be cases
    // that logical table exists whereas physical table not.
    // mock that part3 and logical table is not created before restart
    {
        mustDropSyncedTable(fmt::format("db_{}", db_id), fmt::format("t_{}", part3_id));
        mustDropSyncedTable(fmt::format("db_{}", db_id), fmt::format("t_{}", logical_table_id));
    }

    resetSchemas();

    // add column
    MockTiDB::instance()
        .addColumnToTable(db_name, tbl_name, NameAndTypePair{"col_3", typeFromString("Nullable(Int8)")}, Field{});
    const String new_tbl_name = "mock_part_tbl_1";
    MockTiDB::instance().renameTable(db_name, tbl_name, new_tbl_name);
    refreshSchema();
    refreshTableSchema(logical_table_id);
    refreshTableSchema(part1_id);
    refreshTableSchema(part2_id);
    refreshTableSchema(part3_id);

    {
        auto part1_tbl = mustGetSyncedTable(part1_id);
        ASSERT_EQ(part1_tbl->getTableInfo().name, fmt::format("t_{}", part1_id));
        auto part2_tbl = mustGetSyncedTable(part2_id);
        ASSERT_EQ(part2_tbl->getTableInfo().name, fmt::format("t_{}", part2_id));
        auto part3_tbl = mustGetSyncedTable(part3_id);
        ASSERT_EQ(part3_tbl->getTableInfo().name, fmt::format("t_{}", part3_id));
        auto logical_tbl = mustGetSyncedTable(logical_table_id);
        ASSERT_EQ(logical_tbl->getTableInfo().name, new_tbl_name);
    }


    // drop one partition
    resetSchemas();
    MockTiDB::instance().dropPartition(db_name, new_tbl_name, part1_id);
    refreshSchema();
    refreshTableSchema(logical_table_id);
    refreshTableSchema(part1_id);
    refreshTableSchema(part2_id);
    refreshTableSchema(part3_id);
    auto part1_tbl = mustGetSyncedTable(part1_id);
    ASSERT_EQ(part1_tbl->isTombstone(), true);
}
CATCH

} // namespace DB::tests
