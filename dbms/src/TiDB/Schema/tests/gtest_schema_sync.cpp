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
#include <Common/SyncPoint/SyncPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>
#include <Databases/IDatabase.h>
#include <Debug/MockKVStore/MockUtils.h>
#include <Debug/MockTiDB.h>
#include <Debug/TiFlashTestEnv.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/IAST.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Index/VectorIndex/tests/gtest_dm_vector_index_utils.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/IManageableStorage.h>
#include <Storages/KVStore/Decode/PartitionStreams.h>
#include <Storages/KVStore/Decode/RegionBlockReader.h>
#include <Storages/KVStore/MultiRaft/Spill/RegionUncommittedDataList.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Types.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/registerStorages.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Decode/RowCodec.h>
#include <TiDB/Schema/SchemaBuilder.h>
#include <TiDB/Schema/SchemaSyncService.h>
#include <TiDB/Schema/TiDBSchemaManager.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <grpcpp/server_builder.h>
#include <gtest/gtest.h>
#include <kvproto/tikvpb.grpc.pb.h>

#include <atomic>
#include <ext/scope_guard.h>
#include <future>
#include <limits>
#include <memory>

namespace DB
{
namespace FailPoints
{
extern const char exception_before_rename_table_old_meta_removed[];
extern const char force_context_path[];
extern const char force_get_dropped_table_info_in_schema_sync[];
extern const char force_set_num_regions_for_table[];
extern const char random_ddl_fail_when_rename_partitions[];
extern const char exception_before_schema_store_update[];
extern const char exception_before_schema_metadata_persist[];
} // namespace FailPoints

// defined in StorageSystemDTLocalIndexes.cpp
std::optional<DM::LocalIndexesStats> getLocalIndexesStatsFromStorage(const StorageDeltaMergePtr & dm_storage);
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
                LOG_WARNING(Logger::get(), "{}", e.message());
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
                LOG_WARNING(Logger::get(), "{}", e.message());
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

    DatabasePtr getTiFlashDatabase(const String & db_name)
    {
        auto [ok, db_id] = MockTiDB::instance().getDBIDByName(db_name);
        if (!ok)
            return nullptr;
        return global_ctx.tryGetDatabase(fmt::format("db_{}", db_id));
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


TEST_F(SchemaSyncTest, SchemaReadWatermarkLifecycle)
{
    const String db_name = "watermark_db";
    MockTiDB::instance().newDataBase(db_name);
    const auto [exists, db_id] = MockTiDB::instance().getDBIDByName(db_name);
    ASSERT_TRUE(exists);
    auto cols = ColumnsDescription({{"col1", typeFromString("Int64")}, {"col2", typeFromString("Int64")}});
    const auto table_id = MockTiDB::instance().newTable(db_name, "t", cols, 10, "");
    refreshSchema();
    auto storage = mustGetSyncedTable(table_id);
    auto snapshot = [&] {
        auto lock = storage->lockStructureForShare(getThreadNameAndID());
        return storage->getSchemaSnapshotAndBlockForDecoding(lock, false, true).first;
    };
    EXPECT_EQ(snapshot()->schema_read_tso, 0);

    MockSchemaGetter getter;
    DatabaseInfoCache databases;
    TableIDMap table_id_map(Logger::get("SchemaReadWatermarkTest"));
    SchemaBuilder<MockSchemaGetter, SchemaNameMapper> builder(getter, global_ctx, databases, table_id_map);
    getter.read_tso = 100;
    ASSERT_TRUE(builder.applyTable(db_id, table_id, table_id, false));
    auto old_snapshot = snapshot();
    EXPECT_EQ(old_snapshot->schema_read_tso, 100);
    // Same schema content, but another complete read may advance the watermark.
    getter.read_tso = 200;
    ASSERT_TRUE(builder.applyTable(db_id, table_id, table_id, false));
    EXPECT_EQ(snapshot()->schema_read_tso, 200);
    EXPECT_EQ(old_snapshot->schema_read_tso, 100);
    EXPECT_NE(snapshot()->decoding_schema_epoch, old_snapshot->decoding_schema_epoch);

    // A delayed older request cannot overwrite the newer schema/certificate.
    const auto published_snapshot = snapshot();
    getter.read_tso = 150;
    ASSERT_TRUE(builder.applyTable(db_id, table_id, table_id, false));
    EXPECT_EQ(snapshot(), published_snapshot);
    EXPECT_EQ(snapshot()->schema_read_tso, 200);

    // Reads without a current snapshot proof cannot certify cached schema.
    getter.read_tso = 0;
    ASSERT_TRUE(builder.applyTable(db_id, table_id, table_id, false));
    EXPECT_EQ(snapshot()->schema_read_tso, 0);
    getter.read_tso = 300;
    ASSERT_TRUE(builder.applyTable(db_id, table_id, table_id, false));
    EXPECT_EQ(snapshot()->schema_read_tso, 300);
    EXPECT_EQ(old_snapshot->schema_read_tso, 100);
}

TEST_F(SchemaSyncTest, SchemaReadWatermarkApplyAfterSchemaFailure)
{
    const String db_name = "watermark_schema_failure_db";
    MockTiDB::instance().newDataBase(db_name);
    const auto columns = ColumnsDescription(
        {{"col1", typeFromString("Int64")}, {"col2", typeFromString("Int64")}, {"col3", typeFromString("Int64")}});
    for (bool replace_column_id : {false, true})
    {
        for (const auto * failpoint :
             {FailPoints::exception_before_schema_store_update, FailPoints::exception_before_schema_metadata_persist})
        {
            SCOPED_TRACE(fmt::format("replace_column_id={} failpoint={}", replace_column_id, failpoint));
            const auto table_name = fmt::format("t_{}_{}", replace_column_id, failpoint);
            const auto table_id = MockTiDB::instance().newTable(db_name, table_name, columns, 10, "");
            refreshSchema();
            refreshTableSchema(table_id);
            auto storage = mustGetSyncedTable(table_id);
            auto old_info = storage->getTableInfo();
            ASSERT_EQ(old_info.columns.size(), 3);
            auto snapshot = [&] {
                auto lock = storage->lockStructureForShare(getThreadNameAndID());
                return storage->getSchemaSnapshotAndBlockForDecoding(lock, false, true).first;
            };
            auto apply_schema = [&](TiDB::TableInfo & info, UInt64 tso) {
                auto lock = storage->lockForAlter(getThreadNameAndID());
                storage->alterSchemaChange(
                    lock,
                    info,
                    storage->getDatabaseName(),
                    storage->getTableName(),
                    global_ctx,
                    tso);
            };
            apply_schema(old_info, 100);
            const auto old_snapshot = snapshot();
            auto new_info = old_info;
            const auto removed_id = new_info.columns.back().id;
            if (replace_column_id)
                ++new_info.columns.back().id;
            else
                new_info.columns.pop_back();
            MockTiDB::instance().getTableByName(db_name, table_name)->table_info = new_info;
            {
                FailPointHelper::enableFailPoint(failpoint);
                SCOPE_EXIT({ FailPointHelper::disableFailPoint(failpoint); });
                EXPECT_THROW(apply_schema(new_info, 200), DB::Exception);
            }

            const auto failed_snapshot = snapshot();
            if (failpoint == FailPoints::exception_before_schema_store_update)
            {
                EXPECT_EQ(failed_snapshot, old_snapshot);
                EXPECT_EQ(failed_snapshot->schema_read_tso, 100);
            }
            else
            {
                EXPECT_NE(failed_snapshot, old_snapshot);
                EXPECT_EQ(failed_snapshot->getColId2DefPosMap().count(removed_id), 0);
                EXPECT_EQ(failed_snapshot->schema_read_tso, 0);
            }
            auto region = RegionBench::makeRegionForTable(999, table_id, 0, 100);
            const auto sync_before = GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value();
            HandleID handle = 1;
            auto apply_rows = [&](const TiDB::TableInfo & info, const std::vector<Field> & values, UInt64 tso) {
                for (bool row_v2 : {false, true})
                {
                    SCOPED_TRACE(row_v2);
                    WriteBufferFromOwnString encoded;
                    if (row_v2)
                        encodeRowV2(info, values, encoded);
                    else
                        encodeRowV1(info, values, encoded);
                    auto value = std::make_shared<const TiKVValue>(encoded.releaseStr());
                    auto raw_key = RecordKVFormat::decodeTiKVKey(RecordKVFormat::genKey(table_id, handle++));
                    RegionDataReadInfoList rows;
                    rows.emplace_back(RecordKVFormat::getRawTiDBPK(raw_key), UInt8(Region::PutFlag), tso, value);
                    EXPECT_NO_THROW(writeRegionDataToStorage(global_ctx, region, rows, Logger::get()));
                }
            };
            // A failure before the store update must keep the old snapshot usable.
            // A failure after the update rebuilds from matching new columns/info,
            // with no watermark until persistence succeeds.
            apply_rows(old_info, {Int64(111), Int64(222), Int64(333)}, 50);
            const bool columns_updated = failpoint == FailPoints::exception_before_schema_metadata_persist;
            if (columns_updated)
            {
                // Without a watermark, an unknown old column must take the
                // schema-sync/force-decode fallback for both row formats.
                EXPECT_EQ(GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value(), sync_before + 2);
                EXPECT_EQ(snapshot()->schema_read_tso, 0); // Mock syncs do not provide a read TSO.
            }
            else
                EXPECT_EQ(snapshot(), failed_snapshot);
            const bool needs_sync = replace_column_id && failpoint == FailPoints::exception_before_schema_store_update;
            if (needs_sync)
            {
                // A new column beyond the retained watermark must still reach
                // the normal decode-failure -> schema-sync retry path.
                apply_rows(new_info, {Int64(111), Int64(222), Int64(444)}, 125);
            }
            apply_schema(new_info, 150);
            const auto new_snapshot = snapshot();
            EXPECT_EQ(new_snapshot->schema_read_tso, 150);
            EXPECT_EQ(new_snapshot->getColId2DefPosMap().count(removed_id), 0);
            std::vector<Field> values{Int64(111), Int64(222)};
            if (replace_column_id)
                values.emplace_back(Int64(444));
            apply_rows(new_info, values, 125);
            const auto expected_syncs = columns_updated ? 2 : (replace_column_id ? 1 : 0);
            EXPECT_EQ(GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value(), sync_before + expected_syncs);

            auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
            auto streams
                = dm_storage->getStore()
                      ->readRaw(global_ctx, global_ctx.getSettingsRef(), *new_snapshot->column_defines, 1, true);
            size_t count = 0;
            for (auto & stream : streams)
            {
                stream->readPrefix();
                while (auto block = stream->read())
                {
                    for (size_t row = 0; row < block.rows(); ++row)
                    {
                        EXPECT_EQ((*block.getByName("col1").column)[row].safeGet<Int64>(), 111);
                        EXPECT_EQ((*block.getByName("col2").column)[row].safeGet<Int64>(), 222);
                        if (replace_column_id)
                        {
                            const auto row_handle
                                = (*block.getByName(MutSup::extra_handle_column_name).column)[row].safeGet<Int64>();
                            EXPECT_EQ(
                                (*block.getByName("col3").column)[row].safeGet<Int64>(),
                                row_handle <= 2 ? 0 : 444);
                        }
                        ++count;
                    }
                }
                stream->readSuffix();
            }
            EXPECT_EQ(count, needs_sync ? 6 : 4);
        }
    }
}

TEST_F(SchemaSyncTest, SchemaReadWatermarkPartialUpdateFailurePreservesNewColumn)
try
{
    const String db_name = "watermark_partial_failure_db";
    MockTiDB::instance().newDataBase(db_name);
    for (bool row_v2 : {false, true})
    {
        SCOPED_TRACE(row_v2);
        const auto table_name = fmt::format("t_{}", row_v2);
        const auto columns = ColumnsDescription({{"col1", typeFromString("Int64")}});
        const auto table_id = MockTiDB::instance().newTable(db_name, table_name, columns, 10, "");
        refreshSchema();
        refreshTableSchema(table_id);
        auto storage = mustGetSyncedTable(table_id);
        // Emulate a replica-info update copying local metadata before a complete
        // schema read introduces col2. Fixing that caller's race is separate.
        auto stale_info = storage->getTableInfo();
        ASSERT_EQ(stale_info.columns.size(), 1);
        MockTiDB::instance().addColumnToTable(db_name, table_name, {"col2", typeFromString("Int64")}, Field(Int64(0)));
        auto full_info = MockTiDB::instance().getTableByName(db_name, table_name)->table_info;
        const auto new_column_id = full_info.columns.back().id;
        auto apply_schema = [&](TiDB::TableInfo & info, UInt64 tso) {
            auto lock = storage->lockForAlter(getThreadNameAndID());
            storage
                ->alterSchemaChange(lock, info, storage->getDatabaseName(), storage->getTableName(), global_ctx, tso);
        };
        auto snapshot = [&] {
            auto lock = storage->lockStructureForShare(getThreadNameAndID());
            return storage->getSchemaSnapshotAndBlockForDecoding(lock, false, true).first;
        };
        apply_schema(full_info, 200);
        const auto full_snapshot = snapshot();
        ASSERT_EQ(full_snapshot->schema_read_tso, 200);
        ASSERT_EQ(full_snapshot->getColId2DefPosMap().count(new_column_id), 1);
        ++stale_info.replica_info.count;
        {
            FailPointHelper::enableFailPoint(FailPoints::exception_before_schema_metadata_persist);
            SCOPE_EXIT({ FailPointHelper::disableFailPoint(FailPoints::exception_before_schema_metadata_persist); });
            EXPECT_THROW(apply_schema(stale_info, 0), DB::Exception);
        }
        const auto failed_snapshot = snapshot();
        ASSERT_EQ(failed_snapshot->getColId2DefPosMap().count(new_column_id), 0);
        EXPECT_EQ(failed_snapshot->schema_read_tso, 0);
        EXPECT_EQ(full_snapshot->schema_read_tso, 200);

        WriteBufferFromOwnString encoded;
        if (row_v2)
            encodeRowV2(full_info, {Int64(111), Int64(222)}, encoded);
        else
            encodeRowV1(full_info, {Int64(111), Int64(222)}, encoded);
        auto raw_key = RecordKVFormat::decodeTiKVKey(RecordKVFormat::genKey(table_id, 1));
        RegionDataReadInfoList rows;
        rows.emplace_back(
            RecordKVFormat::getRawTiDBPK(raw_key),
            UInt8(Region::PutFlag),
            150,
            std::make_shared<const TiKVValue>(encoded.releaseStr()));
        auto region = RegionBench::makeRegionForTable(999, table_id, 0, 100);
        const auto sync_before = GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value();
        const auto skip_before = GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value();
        writeRegionDataToStorage(global_ctx, region, rows, Logger::get());
        EXPECT_EQ(GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value(), sync_before + 1);
        EXPECT_EQ(GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value(), skip_before);

        // Restoring the schema after apply cannot recover a value already skipped.
        // Check the actual stored col2 value, not just the decode/sync counters.
        apply_schema(full_info, 300);
        auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
        auto streams = dm_storage->getStore()->readRaw(
            global_ctx,
            global_ctx.getSettingsRef(),
            dm_storage->getStore()->getTableColumns(),
            1,
            true);
        size_t count = 0;
        for (auto & stream : streams)
        {
            stream->readPrefix();
            while (auto block = stream->read())
            {
                for (size_t row = 0; row < block.rows(); ++row)
                {
                    EXPECT_EQ((*block.getByName("col1").column)[row].safeGet<Int64>(), 111);
                    EXPECT_EQ((*block.getByName("col2").column)[row].safeGet<Int64>(), 222);
                    ++count;
                }
            }
            stream->readSuffix();
        }
        EXPECT_EQ(count, 1);
    }
}
CATCH

TEST_F(SchemaSyncTest, SchemaReadWatermarkCommittedApply)
{
    const String db_name = "watermark_apply_db";
    MockTiDB::instance().newDataBase(db_name);
    auto cols = ColumnsDescription({{"col1", typeFromString("Int64")}, {"col2", typeFromString("Int64")}});
    const auto table_id = MockTiDB::instance().newTable(db_name, "t", cols, 10, "");
    refreshSchema();
    auto storage = mustGetSyncedTable(table_id);
    auto table_info = storage->getTableInfo();
    ASSERT_EQ(table_info.columns.size(), 2);
    table_info.columns.back().id = 3;
    {
        auto lock = storage->lockForAlter(getThreadNameAndID());
        storage
            ->alterSchemaChange(lock, table_info, storage->getDatabaseName(), storage->getTableName(), global_ctx, 200);
    }
    auto write_info = table_info;
    auto old_column = table_info.columns.back();
    old_column.id = 2;
    old_column.name = "old_col2";
    write_info.columns.insert(write_info.columns.begin() + 1, old_column);
    WriteBufferFromOwnString encoded;
    encodeRowV2(write_info, {Int64(111), Int64(222), Int64(333)}, encoded);
    auto row_value = std::make_shared<const TiKVValue>(encoded.releaseStr());
    auto region = RegionBench::makeRegionForTable(999, table_id, 0, 100);
    const auto sync_before = GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value();
    const auto skip_before = GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value();
    for (HandleID handle : {1, 2})
    {
        auto raw_key = RecordKVFormat::decodeTiKVKey(RecordKVFormat::genKey(table_id, handle));
        RegionDataReadInfoList rows;
        rows.emplace_back(RecordKVFormat::getRawTiDBPK(raw_key), UInt8(Region::PutFlag), UInt64(100), row_value);
        writeRegionDataToStorage(global_ctx, region, rows, Logger::get("WatermarkApplyTest"));
    }
    EXPECT_EQ(GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value(), sync_before);
    EXPECT_EQ(GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value(), skip_before + 2);

    // A historical-looking row beyond R must still synchronize, even when its
    // unknown column ID is below a known column ID. Keep the mock upstream
    // schema consistent so the ordinary retry exercises the complete chain.
    MockTiDB::instance().getTableByName(db_name, "t")->table_info = table_info;
    auto raw_key = RecordKVFormat::decodeTiKVKey(RecordKVFormat::genKey(table_id, 3));
    RegionDataReadInfoList newer_rows;
    newer_rows.emplace_back(RecordKVFormat::getRawTiDBPK(raw_key), UInt8(Region::PutFlag), UInt64(300), row_value);
    writeRegionDataToStorage(global_ctx, region, newer_rows, Logger::get("WatermarkApplyTest"));
    EXPECT_EQ(GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value(), sync_before + 1);
    EXPECT_EQ(GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value(), skip_before + 2);
}


TEST_F(SchemaSyncTest, SchemaReadWatermarkPreservedAcrossTableStateChanges)
{
    const String db_name = "watermark_special_db";
    MockTiDB::instance().newDataBase(db_name);
    auto cols = ColumnsDescription({{"col1", typeFromString("Int64")}});
    for (bool partition : {false, true})
    {
        const auto table_id = MockTiDB::instance().newTable(db_name, partition ? "p" : "t", cols, 10, "");
        refreshSchema();
        refreshTableSchema(table_id);
        auto storage = mustGetSyncedTable(table_id);
        auto table_info = storage->getTableInfo();
        auto snapshot = [&] {
            auto lock = storage->lockStructureForShare(getThreadNameAndID());
            return storage->getSchemaSnapshotAndBlockForDecoding(lock, false, true).first;
        };
        auto apply = [&](UInt64 tso) {
            auto lock = storage->lockForAlter(getThreadNameAndID());
            storage->alterSchemaChange(
                lock,
                table_info,
                storage->getDatabaseName(),
                storage->getTableName(),
                global_ctx,
                tso);
        };
        apply(100);
        const auto old_schema = snapshot();
        EXPECT_EQ(old_schema->schema_read_tso, 100);
        if (partition)
        {
            table_info.is_partition_table = true;
            table_info.belonging_table_id = table_id + 100;
            apply(200);
            EXPECT_EQ(snapshot()->schema_read_tso, 200);
            table_info.is_partition_table = false;
            table_info.belonging_table_id = InvalidTableID;
        }
        else
        {
            auto tombstone = [&](bool recover) {
                AlterCommand command;
                command.type = recover ? AlterCommand::RECOVER : AlterCommand::TOMBSTONE;
                command.tombstone = 200;
                auto lock = storage->lockForAlter(getThreadNameAndID());
                AlterCommands commands;
                commands.push_back(command);
                storage->alter(lock, commands, storage->getDatabaseName(), storage->getTableName(), global_ctx);
            };
            tombstone(false);
            EXPECT_EQ(snapshot(), old_schema);
            apply(200);
            const auto tombstone_schema = snapshot();
            EXPECT_EQ(tombstone_schema->schema_read_tso, 200);
            tombstone(true);
            EXPECT_EQ(snapshot(), tombstone_schema);
        }
        apply(300);
        EXPECT_EQ(snapshot()->schema_read_tso, 300);
        EXPECT_EQ(old_schema->schema_read_tso, 100);
    }
}

class SchemaWatermarkBoundaryTest : public SchemaSyncTest
{
protected:
    void SetUp() override
    {
        SchemaSyncTest::SetUp();
        MockTiDB::instance().newDataBase(db_name);
        auto cols = ColumnsDescription({{"col1", typeFromString("Int64")}, {"col3", typeFromString("Int64")}});
        table_id = MockTiDB::instance().newTable(db_name, "t", cols, 10, "");
        refreshSchema();
        storage = mustGetSyncedTable(table_id);
        table_info = storage->getTableInfo();
        table_info.columns.back().id = 3;
        applySchema(table_info, 100);
    }

    void applySchema(const TiDB::TableInfo & info, UInt64 tso)
    {
        MockTiDB::instance().getTableByName(db_name, "t")->table_info = info;
        MockSchemaGetter getter;
        getter.read_tso = tso;
        DatabaseInfoCache databases;
        TableIDMap mapping(Logger::get());
        SchemaBuilder<MockSchemaGetter, SchemaNameMapper> builder(getter, global_ctx, databases, mapping);
        const auto db_id = MockTiDB::instance().getDBIDByName(db_name).second;
        ASSERT_TRUE(builder.applyTable(db_id, table_id, table_id, false));
    }

    DecodingStorageSchemaSnapshotConstPtr snapshot(bool with_version = true)
    {
        auto lock = storage->lockStructureForShare(getThreadNameAndID());
        return storage->getSchemaSnapshotAndBlockForDecoding(lock, false, with_version).first;
    }

    std::shared_ptr<const TiKVValue> historicalRow(const TiDB::TableInfo & info, bool row_v2 = true)
    {
        auto write_info = info;
        auto extra = info.columns.front();
        extra.id = 2;
        extra.name = "old_col2";
        write_info.columns.insert(write_info.columns.begin() + 1, extra);
        std::vector<Field> values{Int64(111), Int64(222), Int64(333)};
        if (info.columns.size() == 3)
            values.emplace_back(Int64(444));
        WriteBufferFromOwnString encoded;
        if (row_v2)
            encodeRowV2(write_info, values, encoded);
        else
            encodeRowV1(write_info, values, encoded);
        return std::make_shared<const TiKVValue>(encoded.releaseStr());
    }

    RegionDataReadInfoList committedRows(
        const std::shared_ptr<const TiKVValue> & value,
        UInt64 commit_ts,
        HandleID handle = 1)
    {
        auto raw_key = RecordKVFormat::decodeTiKVKey(RecordKVFormat::genKey(table_id, handle));
        RegionDataReadInfoList rows;
        rows.emplace_back(RecordKVFormat::getRawTiDBPK(raw_key), UInt8(Region::PutFlag), commit_ts, value);
        return rows;
    }

    const String db_name = "watermark_boundary_db";
    TableID table_id = 0;
    ManageableStoragePtr storage;
    TiDB::TableInfo table_info;
};

TEST_F(SchemaWatermarkBoundaryTest, TombstonePreservesHistoricalColumnDecode)
{
    const auto old_schema = snapshot();
    auto region = RegionBench::makeRegionForTable(999, table_id, 0, 100);
    const auto sync_before = GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value();
    const auto skip_before = GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value();
    auto tombstone = [&](bool recover) {
        AlterCommand command;
        command.type = recover ? AlterCommand::RECOVER : AlterCommand::TOMBSTONE;
        command.tombstone = 200;
        AlterCommands commands;
        commands.push_back(command);
        auto lock = storage->lockForAlter(getThreadNameAndID());
        storage->alter(lock, commands, storage->getDatabaseName(), storage->getTableName(), global_ctx);
    };
    HandleID handle = 1;
    auto apply_rows = [&](UInt64 commit_ts) {
        for (bool row_v2 : {false, true})
        {
            SCOPED_TRACE(row_v2);
            auto rows = committedRows(historicalRow(table_info, row_v2), commit_ts, handle++);
            writeRegionDataToStorage(global_ctx, region, rows, Logger::get());
        }
    };

    tombstone(false);
    ASSERT_TRUE(storage->isTombstone());
    EXPECT_EQ(snapshot(), old_schema);
    apply_rows(50);

    // A complete read can publish a newer pair while the table is tombstoned.
    // This also exercises snapshot creation without the tombstone filter.
    applySchema(table_info, 200);
    const auto new_schema = snapshot();
    ASSERT_EQ(new_schema->schema_read_tso, 200);
    ASSERT_TRUE(storage->isTombstone());
    apply_rows(150);

    tombstone(true);
    ASSERT_FALSE(storage->isTombstone());
    EXPECT_EQ(snapshot(), new_schema);
    apply_rows(150);
    EXPECT_EQ(old_schema->schema_read_tso, 100);
    EXPECT_EQ(GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value(), sync_before);
    EXPECT_EQ(GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value(), skip_before + 6);

    auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
    auto streams = dm_storage->getStore()
                       ->readRaw(global_ctx, global_ctx.getSettingsRef(), *new_schema->column_defines, 1, true);
    size_t count = 0;
    for (auto & stream : streams)
    {
        stream->readPrefix();
        while (auto block = stream->read())
        {
            for (size_t row = 0; row < block.rows(); ++row)
            {
                EXPECT_EQ((*block.getByName("col1").column)[row].safeGet<Int64>(), 111);
                EXPECT_EQ((*block.getByName("col3").column)[row].safeGet<Int64>(), 333);
                ++count;
            }
        }
        stream->readSuffix();
    }
    EXPECT_EQ(count, 6);
}

TEST_F(SchemaWatermarkBoundaryTest, UncommittedApplyDoesNotSkipHistoricalColumns)
{
    ASSERT_EQ(snapshot()->schema_read_tso, 100);
    auto region = RegionBench::makeRegionForTable(999, table_id, 0, 100);
    for (bool row_v2 : {false, true})
    {
        SCOPED_TRACE(row_v2);
        // A schema sync may invalidate R, so certify it again before each codec.
        applySchema(table_info, 200);
        auto committed = committedRows(historicalRow(table_info, row_v2), 150);
        RegionUncommittedDataList uncommitted;
        uncommitted.getInner().emplace_back(committed.front().pk, UInt8(Region::PutFlag), committed.front().value);
        const auto sync_before = GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value();
        const auto skip_before = GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value();
        const auto covered_before = GET_METRIC(tiflash_schema_watermark_decode_count, type_covered).Value();
        // Spill is still unimplemented. Reaching that error must require the
        // existing schema-sync/forced retry, rather than granting permission.
        EXPECT_THROW(writeRegionDataToStorage(global_ctx, region, uncommitted, Logger::get()), DB::Exception);
        EXPECT_EQ(GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value(), sync_before + 1);
        EXPECT_EQ(GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value(), skip_before);
        EXPECT_EQ(GET_METRIC(tiflash_schema_watermark_decode_count, type_covered).Value(), covered_before);
    }
}

TEST_F(SchemaWatermarkBoundaryTest, SnapshotConversionDoesNotUseApplyPermission)
{
    applySchema(table_info, 200);
    for (bool row_v2 : {false, true})
    {
        auto region = RegionBench::makeRegionForTable(999, table_id, 0, 100);
        auto value = historicalRow(table_info, row_v2);
        // Snapshot conversion keeps the existing schema retry even when the
        // same cached row is covered by the ordinary committed-write watermark.
        auto key = RecordKVFormat::genKey(table_id, 1, 150);
        auto write_value = RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 140);
        const BaseBuffView key_view{key.data(), key.dataSize()};
        const BaseBuffView value_view{write_value.data(), write_value.dataSize()};
        const auto type = WriteCmdType::Put;
        const auto cf = ColumnFamilyType::Write;
        // Keep an ordinary Raft write in cache by delaying its default-CF
        // value until after the immediate flush has failed.
        EXPECT_THROW(
            region->handleWriteRaftCmd({&key_view, &value_view, &type, &cf, 1}, 6, 1, global_ctx.getTMTContext()),
            DB::Exception);
        region->insertFromSnap(
            global_ctx.getTMTContext(),
            "default",
            RecordKVFormat::genKey(table_id, 1, 140),
            TiKVValue::copyFrom(value->getStr()));
        auto cached = ReadRegionCommitCache(region, true);
        ASSERT_TRUE(cached.has_value());
        ASSERT_EQ(cached->size(), 1);
        auto schema = snapshot();
        const auto skip_before = GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value();
        EXPECT_THROW(GenRegionBlockDataWithSchema(region, schema, 0, false, global_ctx.getTMTContext()), DB::Exception);
        EXPECT_EQ(region->writeCFCount(), 1);
        const auto block = GenRegionBlockDataWithSchema(region, schema, 0, true, global_ctx.getTMTContext());
        ASSERT_EQ(block.rows(), 1);
        EXPECT_EQ((*block.getByName("col3").column)[0].safeGet<Int64>(), 333);
        EXPECT_EQ(region->writeCFCount(), 0);
        EXPECT_EQ(GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value(), skip_before);
    }
}

TEST_F(SchemaWatermarkBoundaryTest, StorageReloadRebuildsWatermarkForOlderBacklog)
{
    applySchema(table_info, 200);
    const auto old_schema = snapshot();
    auto rows = committedRows(historicalRow(table_info), 150);
    auto region = RegionBench::makeRegionForTable(999, table_id, 0, 100);
    writeRegionDataToStorage(global_ctx, region, rows, Logger::get());
    storage->flushCache(global_ctx);
    ASSERT_EQ(old_schema->schema_read_tso, 200);
    auto old_storage = storage;
    auto database = global_ctx.getDatabase(storage->getDatabaseName());
    database->shutdown();
    EXPECT_EQ(global_ctx.getTMTContext().getStorages().get(NullspaceID, table_id), nullptr);
    // Use the production metadata loader, rather than just resetting a syncer.
    database->loadTables(global_ctx, nullptr, false);
    storage = mustGetSyncedTable(table_id);
    EXPECT_NE(storage, old_storage);
    EXPECT_EQ(snapshot()->schema_read_tso, 0);
    EXPECT_EQ(
        SchemaReadWatermarkDecision(snapshot()->schema_read_tso, rows).result,
        SchemaReadWatermarkDecision::NoWatermark);
    EXPECT_EQ(old_schema->schema_read_tso, 200);
    EXPECT_EQ(storage->getTableInfo().serialize().find("schema_read_tso"), String::npos);
    applySchema(table_info, 300);
    const auto new_schema = snapshot();
    EXPECT_EQ(new_schema->schema_read_tso, 300);
    EXPECT_EQ(
        SchemaReadWatermarkDecision(new_schema->schema_read_tso, rows).result,
        SchemaReadWatermarkDecision::Covered);
    // Backlog older than both reads can use the freshly established watermark.
    // Write another handle so that reading the store verifies the new apply too.
    rows = committedRows(historicalRow(table_info), 150, 2);
    const auto sync_before = GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value();
    const auto skip_before = GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value();
    writeRegionDataToStorage(global_ctx, region, rows, Logger::get());
    EXPECT_EQ(GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value(), sync_before);
    EXPECT_EQ(GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value(), skip_before + 1);
    auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
    auto streams = dm_storage->getStore()
                       ->readRaw(global_ctx, global_ctx.getSettingsRef(), *new_schema->column_defines, 1, true);
    size_t count = 0;
    for (auto & stream : streams)
    {
        stream->readPrefix();
        while (auto block = stream->read())
        {
            for (size_t row = 0; row < block.rows(); ++row)
            {
                EXPECT_EQ((*block.getByName("col1").column)[row].safeGet<Int64>(), 111);
                EXPECT_EQ((*block.getByName("col3").column)[row].safeGet<Int64>(), 333);
                ++count;
            }
        }
        stream->readSuffix();
    }
    EXPECT_EQ(count, 2);
}

TEST_F(SchemaWatermarkBoundaryTest, FullSchemaReloadPreservesWatermarkForOlderBacklog)
{
    applySchema(table_info, 200);
    const auto old_schema = snapshot();
    auto rows = committedRows(historicalRow(table_info), 150);
    resetSchemas();
    refreshSchema();
    ASSERT_EQ(snapshot(), old_schema);
    ASSERT_EQ(snapshot()->schema_read_tso, 200);
    auto region = RegionBench::makeRegionForTable(999, table_id, 0, 100);
    const auto sync_before = GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value();
    const auto skip_before = GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value();
    writeRegionDataToStorage(global_ctx, region, rows, Logger::get());
    EXPECT_EQ(GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value(), sync_before);
    EXPECT_EQ(GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value(), skip_before + 1);
    auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
    auto streams = dm_storage->getStore()
                       ->readRaw(global_ctx, global_ctx.getSettingsRef(), *snapshot()->column_defines, 1, true);
    size_t count = 0;
    for (auto & stream : streams)
    {
        stream->readPrefix();
        while (auto block = stream->read())
        {
            for (size_t row = 0; row < block.rows(); ++row)
            {
                EXPECT_EQ((*block.getByName("col1").column)[row].safeGet<Int64>(), 111);
                EXPECT_EQ((*block.getByName("col3").column)[row].safeGet<Int64>(), 333);
                ++count;
            }
        }
        stream->readSuffix();
    }
    EXPECT_EQ(count, 1);
}

TEST_F(SchemaWatermarkBoundaryTest, RegenerateSchemaMapPreservesWatermark)
{
    applySchema(table_info, 200);
    const auto old_schema = snapshot();
    MockTiDB::instance().regenerateSchemaMap();
    refreshSchema();
    EXPECT_EQ(snapshot(), old_schema);
    EXPECT_EQ(snapshot()->schema_read_tso, 200);
    applySchema(table_info, 300);
    EXPECT_EQ(snapshot()->schema_read_tso, 300);
    EXPECT_EQ(old_schema->schema_read_tso, 200);
}

TEST_F(SchemaWatermarkBoundaryTest, CrossDatabaseRenamePreservesPublishedSchemaPair)
{
    const auto old_schema = snapshot();
    const String target_db = "watermark_moved_db";
    const auto target_id = MockTiDB::instance().newDataBase(target_db);
    MockTiDB::instance().renameTableTo(db_name, "t", target_db, "moved");
    refreshSchema();
    EXPECT_EQ(mustGetSyncedTable(table_id), storage);
    EXPECT_EQ(storage->getDatabaseName(), fmt::format("db_{}", target_id));
    EXPECT_EQ(snapshot(), old_schema);
    MockSchemaGetter getter;
    getter.read_tso = 300;
    DatabaseInfoCache databases;
    TableIDMap mapping(Logger::get());
    SchemaBuilder<MockSchemaGetter, SchemaNameMapper> builder(getter, global_ctx, databases, mapping);
    ASSERT_TRUE(builder.applyTable(target_id, table_id, table_id, false));
    EXPECT_EQ(snapshot()->schema_read_tso, 300);
    EXPECT_EQ(old_schema->schema_read_tso, 100);
    EXPECT_EQ(storage->getTableInfo().name, "moved");
}

TEST_F(SchemaWatermarkBoundaryTest, ExchangePartitionPreservesPublishedSchemaPair)
{
    const auto db_id = MockTiDB::instance().getDBIDByName(db_name).second;
    auto cols = ColumnsDescription({{"col1", typeFromString("Int64")}, {"col3", typeFromString("Int64")}});
    const auto logical_id = MockTiDB::instance().newTable(db_name, "p", cols, 10, "");
    const auto partition_id = MockTiDB::instance().newPartition(logical_id, "p0", 10, true);
    // TiDB requires matching column IDs for EXCHANGE PARTITION.
    MockTiDB::instance().getTableByName(db_name, "p")->table_info.columns.back().id = 3;
    refreshSchema();
    refreshTableSchema(logical_id);
    refreshTableSchema(partition_id);
    MockSchemaGetter getter;
    getter.read_tso = 200;
    DatabaseInfoCache databases;
    TableIDMap mapping(Logger::get());
    mapping.emplaceTableID(table_id, db_id);
    mapping.emplaceTableID(logical_id, db_id);
    mapping.emplacePartitionTableID(partition_id, logical_id);
    SchemaBuilder<MockSchemaGetter, SchemaNameMapper> builder(getter, global_ctx, databases, mapping);
    ASSERT_TRUE(builder.applyTable(db_id, table_id, table_id, false));
    ASSERT_TRUE(builder.applyTable(db_id, logical_id, logical_id, false));
    ASSERT_TRUE(builder.applyTable(db_id, logical_id, partition_id, false));
    const auto old_schema = snapshot();
    ASSERT_EQ(old_schema->schema_read_tso, 200);
    auto storage_snapshot = [&](TableID id) {
        const auto current = mustGetSyncedTable(id);
        auto lock = current->lockStructureForShare(getThreadNameAndID());
        return current->getSchemaSnapshotAndBlockForDecoding(lock, false, true).first;
    };
    const auto old_partition_schema = storage_snapshot(partition_id);
    const auto old_logical_schema = storage_snapshot(logical_id);
    EXPECT_EQ(old_partition_schema->schema_read_tso, 200);
    EXPECT_EQ(old_logical_schema->schema_read_tso, 200);

    // Publish the metadata state produced by EXCHANGE PARTITION upstream.
    auto non_partition_info = table_info;
    MockTiDB::instance().dropTable(global_ctx, db_name, "t", false);
    non_partition_info.id = partition_id;
    non_partition_info.name = "exchanged_out";
    MockTiDB::instance().addTable(db_name, std::move(non_partition_info));
    MockTiDB::instance().getTableByName(db_name, "p")->table_info.partition.definitions.front().id = table_id;
    SchemaDiff exchange{};
    exchange.type = SchemaActionType::ExchangeTablePartition;
    exchange.schema_id = db_id;
    exchange.old_schema_id = db_id;
    exchange.old_table_id = table_id;
    exchange.table_id = partition_id;
    AffectedOption affected{};
    affected.schema_id = db_id;
    affected.table_id = logical_id;
    exchange.affected_opts.push_back(affected);
    builder.applyDiff(exchange);
    EXPECT_EQ(mapping.findTableIDInPartitionMap(table_id), logical_id);
    EXPECT_EQ(mapping.findTableIDInDatabaseMap(partition_id), db_id);
    // Exchanging ownership does not change the physical rows or the published
    // schema/read pair. Keep every snapshot until a real schema update occurs.
    EXPECT_EQ(snapshot(), old_schema);
    EXPECT_EQ(storage_snapshot(partition_id), old_partition_schema);
    EXPECT_EQ(storage_snapshot(logical_id), old_logical_schema);
    ASSERT_EQ(snapshot()->schema_read_tso, 200);
    const auto sync_before = GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value();
    const auto skip_before = GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value();
    auto region = RegionBench::makeRegionForTable(999, table_id, 0, 100);
    for (bool row_v2 : {false, true})
    {
        SCOPED_TRACE(row_v2);
        auto rows = committedRows(historicalRow(table_info, row_v2), 150, row_v2 ? 2 : 1);
        writeRegionDataToStorage(global_ctx, region, rows, Logger::get());
    }
    EXPECT_EQ(GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value(), sync_before);
    EXPECT_EQ(GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value(), skip_before + 2);
    // Complete reads publish matching watermarks for normal and partition tables.
    getter.read_tso = 300;
    ASSERT_TRUE(builder.applyTable(db_id, logical_id, table_id, false));
    ASSERT_TRUE(builder.applyTable(db_id, partition_id, partition_id, false));
    ASSERT_TRUE(builder.applyTable(db_id, logical_id, logical_id, false));
    for (auto id : {table_id, partition_id, logical_id})
    {
        const auto current = mustGetSyncedTable(id);
        auto lock = current->lockStructureForShare(getThreadNameAndID());
        EXPECT_EQ(current->getSchemaSnapshotAndBlockForDecoding(lock, false, true).first->schema_read_tso, 300);
    }
    // The original normal table is now a physical partition. Rows beyond the
    // old R but covered by the new read still skip historical extra columns.
    for (bool row_v2 : {false, true})
    {
        SCOPED_TRACE(row_v2);
        auto rows = committedRows(historicalRow(table_info, row_v2), 250, row_v2 ? 4 : 3);
        writeRegionDataToStorage(global_ctx, region, rows, Logger::get());
    }
    EXPECT_EQ(GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value(), sync_before);
    EXPECT_EQ(GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value(), skip_before + 4);
    const auto new_schema = snapshot();
    auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
    auto streams = dm_storage->getStore()
                       ->readRaw(global_ctx, global_ctx.getSettingsRef(), *new_schema->column_defines, 1, true);
    size_t count = 0;
    for (auto & stream : streams)
    {
        stream->readPrefix();
        while (auto block = stream->read())
        {
            for (size_t row = 0; row < block.rows(); ++row)
            {
                EXPECT_EQ((*block.getByName("col1").column)[row].safeGet<Int64>(), 111);
                EXPECT_EQ((*block.getByName("col3").column)[row].safeGet<Int64>(), 333);
                ++count;
            }
        }
        stream->readSuffix();
    }
    EXPECT_EQ(count, 4);
    EXPECT_EQ(storage->getTableInfo().belonging_table_id, logical_id);
    EXPECT_EQ(mustGetSyncedTable(partition_id)->getTableInfo().belonging_table_id, InvalidTableID);
    EXPECT_EQ(old_schema->schema_read_tso, 200);
}

TEST_F(SchemaWatermarkBoundaryTest, ConcurrentDDLAndCommittedApplyKeepSchemaPaired)
{
    const auto old_schema = snapshot();
    auto new_info = table_info;
    auto new_column = table_info.columns.back();
    new_column.id = 4;
    new_column.name = "col4";
    new_info.columns.push_back(new_column);
    auto rows = committedRows(historicalRow(new_info), 150);
    auto region = RegionBench::makeRegionForTable(999, table_id, 0, 100);
    auto point = SyncPointCtl::enableInScope("schema_watermark_before_store_update");
    auto ddl = std::async(std::launch::async, [&] { applySchema(new_info, 200); });
    point.waitAndPause();
    EXPECT_EQ(old_schema->schema_read_tso, 100);
    EXPECT_EQ(old_schema->getColId2DefPosMap().count(4), 0);
    EXPECT_EQ(
        SchemaReadWatermarkDecision(old_schema->schema_read_tso, rows).result,
        SchemaReadWatermarkDecision::BeyondWatermark);
    const auto sync_before = GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value();
    const auto skip_before = GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value();
    std::promise<void> apply_started;
    auto apply = std::async(std::launch::async, [&] {
        apply_started.set_value();
        writeRegionDataToStorage(global_ctx, region, rows, Logger::get());
    });
    apply_started.get_future().wait();
    // A delayed request carrying the old schema must not replace the new pair.
    std::promise<void> old_request_started;
    auto delayed_old_request = std::async(std::launch::async, [&] {
        old_request_started.set_value();
        applySchema(table_info, 150);
    });
    old_request_started.get_future().wait();
    point.next();
    ddl.get();
    delayed_old_request.get();
    apply.get();
    auto new_schema = snapshot();
    EXPECT_EQ(old_schema->schema_read_tso, 100);
    EXPECT_EQ(new_schema->schema_read_tso, 200);
    EXPECT_EQ(new_schema->getColId2DefPosMap().count(4), 1);
    EXPECT_GT(new_schema->decoding_schema_epoch, old_schema->decoding_schema_epoch);
    EXPECT_EQ(GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value(), sync_before);
    EXPECT_EQ(GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value(), skip_before + 1);
    auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
    auto streams = dm_storage->getStore()
                       ->readRaw(global_ctx, global_ctx.getSettingsRef(), *new_schema->column_defines, 1, true);
    size_t count = 0;
    for (auto & stream : streams)
    {
        stream->readPrefix();
        while (auto block = stream->read())
        {
            for (size_t row = 0; row < block.rows(); ++row)
            {
                EXPECT_EQ((*block.getByName("col1").column)[row].safeGet<Int64>(), 111);
                EXPECT_EQ((*block.getByName("col3").column)[row].safeGet<Int64>(), 333);
                EXPECT_EQ((*block.getByName("col4").column)[row].safeGet<Int64>(), 444);
                ++count;
            }
        }
        stream->readSuffix();
    }
    EXPECT_EQ(count, 1);
}

namespace
{
class WatermarkMetadataPD : public pingcap::pd::MockPDClient
{
public:
    explicit WatermarkMetadataPD(String address_)
        : address(std::move(address_))
    {}

    pdpb::GetRegionResponse getRegionByKey(const std::string &) override
    {
        pdpb::GetRegionResponse response;
        auto * region = response.mutable_region();
        region->set_id(1);
        region->mutable_region_epoch()->set_conf_ver(1);
        region->mutable_region_epoch()->set_version(1);
        auto * peer = region->add_peers();
        peer->set_id(1);
        peer->set_store_id(1);
        *response.mutable_leader() = *peer;
        return response;
    }

    metapb::Store getStore(UInt64 id) override
    {
        metapb::Store store;
        store.set_id(id);
        store.set_address(address);
        return store;
    }

private:
    const String address;
};

class WatermarkMetadataService : public tikvpb::Tikv::Service
{
public:
    grpc::Status KvGet(grpc::ServerContext *, const kvrpcpb::GetRequest *, kvrpcpb::GetResponse *) override
    {
        ++snapshot_reads;
        return grpc::Status::OK; // No live metadata: the table was dropped.
    }

    grpc::Status MvccGetByKey(
        grpc::ServerContext *,
        const kvrpcpb::MvccGetByKeyRequest *,
        kvrpcpb::MvccGetByKeyResponse * response) override
    {
        ++historical_reads;
        // Deliberately return out-of-order historical versions. The real
        // TxnStructure::mvccGet must select the newest TableInfo.
        for (auto [tso, value] : {std::pair{10, old_info}, {30, latest_info}, {20, old_info}})
        {
            auto * version = response->mutable_info()->add_values();
            version->set_start_ts(tso);
            version->set_value(value);
        }
        return grpc::Status::OK;
    }

    String old_info;
    String latest_info;
    std::atomic<UInt64> snapshot_reads{0};
    std::atomic<UInt64> historical_reads{0};
};
} // namespace

TEST_F(SchemaWatermarkBoundaryTest, HistoricalMVCCFallbackPublishesLatestSchema)
{
    const auto old_schema = snapshot();
    ASSERT_EQ(old_schema->schema_read_tso, 100);
    WatermarkMetadataService service;
    service.old_info = table_info.serialize();
    auto historical_info = table_info;
    auto column = table_info.columns.back();
    column.id = 4;
    column.name = "historical_col4";
    historical_info.columns.push_back(column);
    service.latest_info = historical_info.serialize();
    grpc::ServerBuilder server_builder;
    int port = 0;
    server_builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    server_builder.RegisterService(&service);
    auto server = server_builder.BuildAndStart();
    ASSERT_NE(server, nullptr);
    SCOPE_EXIT({
        server->Shutdown();
        server->Wait();
    });

    pingcap::kv::Cluster cluster;
    // Stop the default mock cluster's workers before replacing its RPC/PD
    // endpoints. This test uses synchronous RPCs only.
    cluster.lock_resolver->stopBgResolve();
    cluster.rpc_client->stop();
    cluster.mpp_prober->stop();
    cluster.thread_pool->stop();
    cluster.pd_client = std::make_shared<WatermarkMetadataPD>(fmt::format("127.0.0.1:{}", port));
    cluster.region_cache = std::make_unique<pingcap::kv::RegionCache>(cluster.pd_client, pingcap::ClusterConfig{});
    cluster.rpc_client = std::make_unique<pingcap::kv::RpcClient>(cluster.pd_client, pingcap::ClusterConfig{});
    SchemaGetter getter(&cluster, 300, NullspaceID);
    DatabaseInfoCache databases;
    TableIDMap mapping(Logger::get());
    SchemaBuilder<SchemaGetter, SchemaNameMapper> builder(getter, global_ctx, databases, mapping);
    const auto db_id = MockTiDB::instance().getDBIDByName(db_name).second;
    EXPECT_FALSE(builder.applyTable(db_id, table_id, table_id, false));
    EXPECT_EQ(snapshot()->schema_read_tso, 100);
    EXPECT_EQ(service.historical_reads.load(), 0);
    // A delayed historical read cannot replace a newer published schema.
    SchemaGetter older_getter(&cluster, 50, NullspaceID);
    SchemaBuilder<SchemaGetter, SchemaNameMapper> older_builder(older_getter, global_ctx, databases, mapping);
    ASSERT_TRUE(older_builder.applyTable(db_id, table_id, table_id, true));
    EXPECT_EQ(snapshot(), old_schema);
    EXPECT_EQ(snapshot()->getColId2DefPosMap().count(4), 0);
    EXPECT_EQ(service.snapshot_reads.load(), 2);
    EXPECT_EQ(service.historical_reads.load(), 1);
    ASSERT_TRUE(builder.applyTable(db_id, table_id, table_id, true));
    EXPECT_EQ(service.snapshot_reads.load(), 3);
    EXPECT_EQ(service.historical_reads.load(), 2);
    EXPECT_EQ(snapshot()->schema_read_tso, 300);
    EXPECT_EQ(snapshot()->getColId2DefPosMap().count(4), 1);
    EXPECT_EQ(old_schema->schema_read_tso, 100);
    EXPECT_EQ(old_schema->getColId2DefPosMap().count(4), 0);

    const auto schema = snapshot();
    auto region = RegionBench::makeRegionForTable(999, table_id, 0, 100);
    const auto sync_before = GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value();
    const auto skip_before = GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value();
    for (bool row_v2 : {false, true})
    {
        SCOPED_TRACE(row_v2);
        auto rows = committedRows(historicalRow(historical_info, row_v2), 150, row_v2 ? 2 : 1);
        writeRegionDataToStorage(global_ctx, region, rows, Logger::get());
    }
    EXPECT_EQ(GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Value(), sync_before);
    EXPECT_EQ(GET_METRIC(tiflash_schema_watermark_decode_count, type_skipped_extra_columns).Value(), skip_before + 2);
    auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
    auto streams
        = dm_storage->getStore()->readRaw(global_ctx, global_ctx.getSettingsRef(), *schema->column_defines, 1, true);
    size_t count = 0;
    for (auto & stream : streams)
    {
        stream->readPrefix();
        while (auto block = stream->read())
        {
            for (size_t row = 0; row < block.rows(); ++row)
            {
                EXPECT_EQ((*block.getByName("col1").column)[row].safeGet<Int64>(), 111);
                EXPECT_EQ((*block.getByName("col3").column)[row].safeGet<Int64>(), 333);
                EXPECT_EQ((*block.getByName("historical_col4").column)[row].safeGet<Int64>(), 444);
                ++count;
            }
        }
        stream->readSuffix();
    }
    EXPECT_EQ(count, 2);
}

TEST_F(SchemaSyncTest, SchemaDiff)
try
{
    // Note that if we want to add new fields here, please firstly check if it is present.
    // Otherwise it will break when doing upgrading test.
    SchemaDiff diff;
    std::string data = "{\"version\":40,\"type\":31,\"schema_id\":69,\"table_id\":71,\"old_table_id\":0,\"old_schema_"
                       "id\":0,\"affected_options\":null}";
    ASSERT_NO_THROW(diff.deserialize(data));

    const auto assert_mv_action = [](const Int32 action, const SchemaActionType expected_action) {
        SchemaDiff mv_diff;
        const auto mv_data = fmt::format(
            "{{\"version\":40,\"type\":{},\"schema_id\":69,\"table_id\":71,\"old_table_id\":70,\"old_schema_id\":0,"
            "\"affected_options\":null}}",
            action);
        ASSERT_NO_THROW(mv_diff.deserialize(mv_data));
        ASSERT_EQ(mv_diff.type, expected_action);
    };
    assert_mv_action(85, SchemaActionType::ActionCreateMaterializedViewLog);
    assert_mv_action(86, SchemaActionType::ActionCreateMaterializedView);
    assert_mv_action(87, SchemaActionType::ActionDropMaterializedViewLog);
    assert_mv_action(88, SchemaActionType::ActionDropMaterializedView);
    assert_mv_action(89, SchemaActionType::ActionAlterMaterializedViewRefresh);
    assert_mv_action(90, SchemaActionType::ActionAlterMaterializedViewLogPurge);
    assert_mv_action(91, SchemaActionType::ActionAlterMaterializedViewAttributes);
    assert_mv_action(92, SchemaActionType::ActionMViewRefreshOutOfPlaceCutover);
    assert_mv_action(93, SchemaActionType::ActionCreateMaterializedViewShadow);
    assert_mv_action(94, SchemaActionType::ActionDropMaterializedViewShadow);
}
CATCH

TEST_F(SchemaSyncTest, MaterializedViewShadowDDL)
try
{
    auto pd_client = global_ctx.getTMTContext().getPDClient();
    const String db_name = "mock_db";
    const String mv_name = "mv";
    const String shadow_name = "__mv_shadow";
    MockTiDB::instance().newDataBase(db_name);

    const auto columns = ColumnsDescription({{"a", typeFromString("Int64")}});
    const auto old_mv_id = MockTiDB::instance().newTable(db_name, mv_name, columns, pd_client->getTS(), "");
    const auto shadow_id = MockTiDB::instance().newTable(db_name, shadow_name, columns, pd_client->getTS(), "");
    const auto cleanup_shadow_id
        = MockTiDB::instance().newTable(db_name, "__mv_shadow_cleanup", columns, pd_client->getTS(), "");
    const auto [db_exists, database_id] = MockTiDB::instance().getDBIDByName(db_name);
    ASSERT_TRUE(db_exists);

    refreshSchema();
    refreshTableSchema(old_mv_id);
    refreshTableSchema(shadow_id);
    refreshTableSchema(cleanup_shadow_id);

    MockSchemaGetter getter;
    DatabaseInfoCache databases;
    TableIDMap table_id_map(Logger::get("MaterializedViewShadowDDL"));
    SchemaBuilder<MockSchemaGetter, SchemaNameMapper> builder(getter, global_ctx, databases, table_id_map);

    builder.applyDiff(SchemaDiff{
        .type = SchemaActionType::ActionCreateMaterializedViewShadow,
        .schema_id = database_id,
        .table_id = shadow_id,
    });
    ASSERT_EQ(table_id_map.findTableIDInDatabaseMap(shadow_id), database_id);

    builder.applyDiff(SchemaDiff{
        .type = SchemaActionType::ActionDropMaterializedViewShadow,
        .schema_id = database_id,
        .table_id = cleanup_shadow_id,
    });
    ASSERT_TRUE(mustGetSyncedTable(cleanup_shadow_id)->isTombstone());

    auto shadow_table = MockTiDB::instance().getTableByName(db_name, shadow_name);
    shadow_table->table_info.name = mv_name;
    SCOPE_EXIT({ shadow_table->table_info.name = shadow_name; });
    builder.applyDiff(SchemaDiff{
        .type = SchemaActionType::ActionMViewRefreshOutOfPlaceCutover,
        .schema_id = database_id,
        .table_id = shadow_id,
        .old_table_id = old_mv_id,
    });

    ASSERT_TRUE(mustGetSyncedTable(old_mv_id)->isTombstone());
    ASSERT_FALSE(mustGetSyncedTable(shadow_id)->isTombstone());
    ASSERT_EQ(mustGetSyncedTable(shadow_id)->getTableInfo().name, mv_name);
}
CATCH

TEST_F(SchemaSyncTest, CreateMaterializedViewRollbackSchemaDiffs)
try
{
    auto pd_client = global_ctx.getTMTContext().getPDClient();

    const String db_name = "mock_db";
    MockTiDB::instance().newDataBase(db_name);

    auto cols = ColumnsDescription({
        {"col1", typeFromString("String")},
        {"col2", typeFromString("Int64")},
    });
    auto [db_exists, db_id] = MockTiDB::instance().getDBIDByName(db_name);
    ASSERT_TRUE(db_exists);

    MockSchemaGetter getter;
    DatabaseInfoCache databases;
    TableIDMap table_id_map(Logger::get("SchemaSyncTest"));
    SchemaBuilder<MockSchemaGetter, SchemaNameMapper> builder(getter, global_ctx, databases, table_id_map);

    const std::vector<std::pair<String, SchemaActionType>> create_actions{
        {"mv_rollback", SchemaActionType::ActionCreateMaterializedView},
        {"mlog_rollback", SchemaActionType::ActionCreateMaterializedViewLog},
    };
    for (const auto & [table_name, create_action] : create_actions)
    {
        const auto table_id = MockTiDB::instance().newTable(db_name, table_name, cols, pd_client->getTS(), "");

        SchemaDiff create_diff;
        create_diff.type = create_action;
        create_diff.schema_id = db_id;
        create_diff.table_id = table_id;
        builder.applyDiff(create_diff);

        ASSERT_TRUE(table_id_map.tableIDInDatabaseIdMap(table_id));
        ASSERT_TRUE(builder.applyTable(db_id, table_id, table_id, true));
        ASSERT_FALSE(mustGetSyncedTable(table_id)->isTombstone());

        // TiDB may reload MV metadata in phase 2 using table_id == old_table_id;
        // this remains a create/update diff, not the rollback encoding.
        SchemaDiff reload_diff;
        reload_diff.type = create_action;
        reload_diff.schema_id = db_id;
        reload_diff.table_id = table_id;
        reload_diff.old_table_id = table_id;
        builder.applyDiff(reload_diff);
        ASSERT_FALSE(mustGetSyncedTable(table_id)->isTombstone());

        // Simulate TiDB removing the just-created metadata as part of DDL rollback.
        MockTiDB::instance().dropTable(global_ctx, db_name, table_name, /*drop_regions=*/false);

        SchemaDiff rollback_diff;
        rollback_diff.type = create_action;
        rollback_diff.schema_id = db_id;
        rollback_diff.table_id = 0;
        rollback_diff.old_table_id = table_id;
        builder.applyDiff(rollback_diff);

        ASSERT_TRUE(mustGetSyncedTable(table_id)->isTombstone()) << table_name;
    }
}
CATCH

TEST_F(SchemaSyncTest, RenameTables)
try
{
    auto pd_client = global_ctx.getTMTContext().getPDClient();

    const String db_name = "mock_db";
    MockTiDB::instance().newDataBase(db_name);

    auto cols = ColumnsDescription({
        {"col1", typeFromString(DataTypeString::getDefaultName())},
        {"col2", typeFromString("Int64")},
    });
    // table_name, cols, pk_name
    std::vector<std::tuple<String, ColumnsDescription, String>> tables{
        {"t1", cols, ""},
        {"t2", cols, ""},
    };
    auto table_ids = MockTiDB::instance().newTables(db_name, tables, pd_client->getTS());

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
        {"col1", typeFromString(DataTypeString::getDefaultName())},
        {"col2", typeFromString("Int64")},
    });
    // table_name, cols, pk_name
    std::vector<std::tuple<String, ColumnsDescription, String>> tables{
        {"t1", cols, ""},
        {"t2", cols, ""},
    };
    auto table_ids = MockTiDB::instance().newTables(db_name, tables, pd_client->getTS());

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
        {"col1", typeFromString(DataTypeString::getDefaultName())},
        {"col2", typeFromString("Int64")},
    });
    // table_name, cols, pk_name
    std::vector<std::tuple<String, ColumnsDescription, String>> tables{
        {"t1", cols, ""},
    };
    auto table_ids = MockTiDB::instance().newTables(db_name, tables, pd_client->getTS());

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

TEST_F(SchemaSyncTest, SyncDroppedTableSchemaAfterDropBeforeStorageCreated)
try
{
    // Mock the case that:
    // 1. create table and add tiflash replica
    // 2. drop table
    // 3. raft command is sync to tiflash after the table is dropped

    auto pd_client = global_ctx.getTMTContext().getPDClient();

    const String db_name = "mock_db";
    MockTiDB::instance().newDataBase(db_name);
    refreshSchema();

    auto cols = ColumnsDescription({
        {"a", typeFromString("Int32")},
        {"b", typeFromString("Decimal(5,2)")},
        {"c", typeFromString("Nullable(String)")},
        {"d", typeFromString("Nullable(Int32)")},
    });
    auto table_id = MockTiDB::instance().newTable(db_name, "t4", cols, pd_client->getTS(), "a");

    // Mimic `__refresh_schemas()` after CREATE TABLE / SET TiFlash replica. For a non-partition table,
    // this only registers table_id mapping; it does not create the StorageDeltaMerge instance yet.
    refreshSchema();
    ASSERT_EQ(global_ctx.getTMTContext().getStorages().get(NullspaceID, table_id), nullptr);

    auto dropped_table_info = MockTiDB::instance().getTableInfoByID(table_id);
    ASSERT_NE(dropped_table_info, nullptr);
    dropped_table_info->state = TiDB::StateDeleteOnly;
    FailPointHelper::enableFailPoint(FailPoints::force_get_dropped_table_info_in_schema_sync, dropped_table_info);
    SCOPE_EXIT({ FailPointHelper::disableFailPoint(FailPoints::force_get_dropped_table_info_in_schema_sync); });

    // Mimic DROP TABLE before a raft command creates local storage. The failpoint above injects the
    // dropped table info for the later table-level schema sync, matching the real MVCC path.
    MockTiDB::instance().dropTable(global_ctx, db_name, "t4", /*drop_regions=*/false);

    // Mimic `__refresh_schemas()` after DROP TABLE. Since the local storage does not exist yet,
    // applyDropTable should ignore the diff and still leave no local storage.
    refreshSchema();
    ASSERT_EQ(global_ctx.getTMTContext().getStorages().get(NullspaceID, table_id), nullptr);

    // Mimic a raft command arriving after DROP TABLE. The first non-MVCC table lookup fails, then
    // syncTableSchema retries with the injected dropped table info and creates a tombstone storage.
    refreshTableSchema(table_id);

    auto storage = mustGetSyncedTable(table_id);
    ASSERT_TRUE(storage->isTombstone());
    ASSERT_GT(storage->getTombstone(), 0);
    ASSERT_EQ(storage->getTableInfo().name, "t4");
}
CATCH

TEST_F(SchemaSyncTest, RenamePartitionTable)
try
{
    auto pd_client = global_ctx.getTMTContext().getPDClient();

    const String db_name = "mock_db";
    const String tbl_name = "mock_part_tbl";

    auto cols = ColumnsDescription({
        {"col1", typeFromString(DataTypeString::getDefaultName())},
        {"col2", typeFromString("Int64")},
    });

    MockTiDB::instance().newDataBase(db_name);
    auto logical_table_id = MockTiDB::instance().newTable(db_name, tbl_name, cols, pd_client->getTS(), "");
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

TEST_F(SchemaSyncTest, RenamePartitionTableAcrossDatabase)
try
{
    auto pd_client = global_ctx.getTMTContext().getPDClient();

    const String db_name = "mock_db";
    const String new_db_name = "mock_new_db";
    const String tbl_name = "mock_part_tbl";

    auto cols = ColumnsDescription({
        {"col1", typeFromString(DataTypeString::getDefaultName())},
        {"col2", typeFromString("Int64")},
    });

    MockTiDB::instance().newDataBase(db_name);
    MockTiDB::instance().newDataBase(new_db_name);

    auto [logical_table_id, physical_table_ids] = MockTiDB::instance().newPartitionTable( //
        db_name,
        tbl_name,
        cols,
        pd_client->getTS(),
        "",
        {"red", "blue", "yellow"});

    ASSERT_EQ(physical_table_ids.size(), 3);

    refreshSchema();
    refreshTableSchema(logical_table_id);
    refreshTableSchema(physical_table_ids[0]);
    // refreshTableSchema(physical_table_ids[1]); // mock that physical_table[1] have no data neither read
    refreshTableSchema(physical_table_ids[2]);

    // check partition table are created
    {
        auto logical_tbl = mustGetSyncedTable(logical_table_id);
        ASSERT_EQ(logical_tbl->getTableInfo().name, tbl_name);
        auto part0_tbl = mustGetSyncedTable(physical_table_ids[0]);
        auto part2_tbl = mustGetSyncedTable(physical_table_ids[2]);

        // physical_table[1] is not created
        auto db = getTiFlashDatabase(db_name);
        ASSERT_NE(db, nullptr);
        EXPECT_TRUE(db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[0])));
        EXPECT_FALSE(db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[1])));
        EXPECT_TRUE(db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[2])));
    }

    // Rename the partition table across database
    const String new_tbl_name = "mock_part_tbl_renamed";
    MockTiDB::instance().renameTableTo(db_name, tbl_name, new_db_name, new_tbl_name);
    refreshSchema();

    {
        auto logical_tbl = mustGetSyncedTable(logical_table_id);
        ASSERT_EQ(logical_tbl->getTableInfo().name, new_tbl_name);
        auto part1_tbl = mustGetSyncedTable(physical_table_ids[0]);
        auto part2_tbl = mustGetSyncedTable(physical_table_ids[2]);
    }
    {
        // All partitions are not exist in the old database
        auto db = getTiFlashDatabase(db_name);
        ASSERT_NE(db, nullptr);
        EXPECT_FALSE(db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[0])));
        EXPECT_FALSE(db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[1])));
        EXPECT_FALSE(db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[2])));

        // All partitions should be rename to the new database
        auto new_db = getTiFlashDatabase(new_db_name);
        ASSERT_NE(new_db, nullptr);
        EXPECT_TRUE(new_db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[0])));
        // physical_table[1] still not created
        EXPECT_FALSE(new_db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[1])));
        EXPECT_TRUE(new_db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[2])));
    }

    {
        // Mock that new data write into physical_table[1], it should be created under the new_database
        refreshTableSchema(physical_table_ids[1]);

        // All partitions should belong to the new database
        auto new_db = getTiFlashDatabase(new_db_name);
        ASSERT_NE(new_db, nullptr);
        EXPECT_TRUE(new_db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[0])));
        // physical_table[1] now is created
        EXPECT_TRUE(new_db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[1])));
        EXPECT_TRUE(new_db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[2])));

        auto db = getTiFlashDatabase(db_name);
        ASSERT_NE(db, nullptr);
        EXPECT_FALSE(db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[0])));
        EXPECT_FALSE(db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[1])));
        EXPECT_FALSE(db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[2])));
    }
}
CATCH

TEST_F(SchemaSyncTest, RenamePartitionTableAcrossDatabaseWithRestart)
try
{
    auto pd_client = global_ctx.getTMTContext().getPDClient();

    const String db_name = "mock_db";
    const String new_db_name = "mock_new_db";
    const String tbl_name = "mock_part_tbl";

    auto cols = ColumnsDescription({
        {"col1", typeFromString(DataTypeString::getDefaultName())},
        {"col2", typeFromString("Int64")},
    });

    MockTiDB::instance().newDataBase(db_name);
    MockTiDB::instance().newDataBase(new_db_name);

    auto [logical_table_id, physical_table_ids] = MockTiDB::instance().newPartitionTable( //
        db_name,
        tbl_name,
        cols,
        pd_client->getTS(),
        "",
        {"red", "blue", "yellow"});

    ASSERT_EQ(physical_table_ids.size(), 3);

    refreshSchema();
    refreshTableSchema(logical_table_id);
    refreshTableSchema(physical_table_ids[0]);
    // refreshTableSchema(physical_table_ids[1]); // mock that physical_table[1] have no data neither read
    refreshTableSchema(physical_table_ids[2]);

    // check partition table are created
    {
        auto logical_tbl = mustGetSyncedTable(logical_table_id);
        ASSERT_EQ(logical_tbl->getTableInfo().name, tbl_name);
        auto part0_tbl = mustGetSyncedTable(physical_table_ids[0]);
        auto part2_tbl = mustGetSyncedTable(physical_table_ids[2]);

        // physical_table[1] is not created
        auto db = getTiFlashDatabase(db_name);
        ASSERT_NE(db, nullptr);
        EXPECT_TRUE(db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[0])));
        EXPECT_FALSE(db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[1])));
        EXPECT_TRUE(db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[2])));
    }

    // mock failures happen when renaming partitions of a partitioned table
    FailPointHelper::enableRandomFailPoint(FailPoints::random_ddl_fail_when_rename_partitions, 0.5);
    SCOPE_EXIT({ FailPointHelper::disableFailPoint(FailPoints::random_ddl_fail_when_rename_partitions); });

    // Rename the partition table across database
    const String new_tbl_name = "mock_part_tbl_renamed";
    MockTiDB::instance().renameTableTo(db_name, tbl_name, new_db_name, new_tbl_name);
    refreshSchema();

    Strings not_renamed_tbls;
    {
        // All partitions should belong to the new database
        auto new_db = getTiFlashDatabase(new_db_name);
        ASSERT_NE(new_db, nullptr);
        for (const auto & tbl : Strings{
                 fmt::format("t_{}", physical_table_ids[0]),
                 fmt::format("t_{}", physical_table_ids[2]),
             })
        {
            if (!new_db->isTableExist(global_ctx, tbl))
                not_renamed_tbls.push_back(tbl);
        }
    }
    LOG_WARNING(
        Logger::get(),
        "mock that these partitions are not renamed before restart, not_renamed_tables={}",
        not_renamed_tbls);

    if (!not_renamed_tbls.empty())
    {
        // mock that tiflash restart and run the fix logic
        resetSchemas();
        refreshSchema();
    }

    {
        auto logical_tbl = mustGetSyncedTable(logical_table_id);
        ASSERT_EQ(logical_tbl->getTableInfo().name, new_tbl_name);
        auto part1_tbl = mustGetSyncedTable(physical_table_ids[0]);
        auto part2_tbl = mustGetSyncedTable(physical_table_ids[2]);
    }
    {
        // All partitions are not exist in the old database
        auto db = getTiFlashDatabase(db_name);
        ASSERT_NE(db, nullptr);
        EXPECT_FALSE(db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[0])));
        EXPECT_FALSE(db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[1])));
        EXPECT_FALSE(db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[2])));

        // All partitions should be rename to the new database
        auto new_db = getTiFlashDatabase(new_db_name);
        ASSERT_NE(new_db, nullptr);
        EXPECT_TRUE(new_db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[0])));
        // physical_table[1] still not created
        EXPECT_FALSE(new_db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[1])));
        EXPECT_TRUE(new_db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[2])));
    }

    {
        // Mock that new data write into physical_table[1], it should be created under the new_database
        refreshTableSchema(physical_table_ids[1]);

        // All partitions should belong to the new database
        auto new_db = getTiFlashDatabase(new_db_name);
        ASSERT_NE(new_db, nullptr);
        EXPECT_TRUE(new_db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[0])));
        // physical_table[1] now is created
        EXPECT_TRUE(new_db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[1])));
        EXPECT_TRUE(new_db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[2])));

        auto db = getTiFlashDatabase(db_name);
        ASSERT_NE(db, nullptr);
        EXPECT_FALSE(db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[0])));
        EXPECT_FALSE(db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[1])));
        EXPECT_FALSE(db->isTableExist(global_ctx, fmt::format("t_{}", physical_table_ids[2])));
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
        {"col_1", typeFromString(DataTypeString::getDefaultName())},
        {"col_2", typeFromString("Int64")},
    });

    auto db_id = MockTiDB::instance().newDataBase(db_name);
    auto logical_table_id = MockTiDB::instance().newTable(db_name, tbl_name, cols, pd_client->getTS(), "");
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

TEST_F(SchemaSyncTest, VectorIndex)
try
{
    auto pd_client = global_ctx.getTMTContext().getPDClient();

    const String db_name = "mock_db";
    MockTiDB::instance().newDataBase(db_name);

    auto cols = ColumnsDescription({
        {"col1", typeFromString("Int64")},
        {"vec", typeFromString("Array(Float32)")},
    });

    // table_name, cols, pk_name
    auto t1_id = MockTiDB::instance().newTable(db_name, "t1", cols, pd_client->getTS(), "");
    refreshSchema();

    auto vector_index = std::make_shared<const TiDB::VectorIndexDefinition>(TiDB::VectorIndexDefinition{
        .kind = tipb::VectorIndexKind::HNSW,
        .dimension = 3,
        .distance_metric = tipb::VectorDistanceMetric::L2,
    });

    DM::tests::DeltaMergeStoreVectorBase dmsv;
    StorageDeltaMergePtr storage = std::static_pointer_cast<StorageDeltaMerge>(mustGetSyncedTable(t1_id));
    dmsv.store = storage->getStore();
    dmsv.db_context = std::make_shared<Context>(global_ctx.getGlobalContext());
    dmsv.vec_column_name = cols.getAllPhysical().back().name;
    dmsv.vec_column_id = mustGetSyncedTable(t1_id)->getTableInfo().getColumnID(dmsv.vec_column_name);
    const size_t num_rows_write = vector_index->dimension;
    // write to store
    dmsv.writeWithVecData(num_rows_write);
    // trigger mergeDelta for all segments
    dmsv.triggerMergeDelta();

    // add a vector index
    IndexID idx_id = 11;
    MockTiDB::instance().addVectorIndexToTable(db_name, "t1", idx_id, cols.getAllPhysical().back(), 0, vector_index);

    // sync schema, the VectorIndex in TableInfo is not get updated
    refreshSchema();
    auto idx_infos = mustGetSyncedTable(t1_id)->getTableInfo().index_infos;
    ASSERT_EQ(idx_infos.size(), 0);

    // sync table schema, the VectorIndex in TableInfo should be updated
    refreshTableSchema(t1_id);
    auto tbl_info = mustGetSyncedTable(t1_id)->getTableInfo();
    tbl_info = mustGetSyncedTable(t1_id)->getTableInfo();
    idx_infos = tbl_info.index_infos;
    ASSERT_EQ(idx_infos.size(), 1);
    for (const auto & idx : idx_infos)
    {
        ASSERT_EQ(idx.id, idx_id);
        ASSERT_NE(idx.vector_index, nullptr);
        ASSERT_EQ(idx.vector_index->kind, vector_index->kind);
        ASSERT_EQ(idx.vector_index->dimension, vector_index->dimension);
        ASSERT_EQ(idx.vector_index->distance_metric, vector_index->distance_metric);
    }

    // test read with ANN query after add a vector index
    {
        // check stable index has built for all segments
        dmsv.waitStableLocalIndexReady();
        LOG_INFO(Logger::get(), "waitStableLocalIndexReady done");
        const auto range = DM::RowKeyRange::newAll(dmsv.store->is_common_handle, dmsv.store->rowkey_column_size);

        // read from store
        {
            dmsv.read(
                range,
                DM::EMPTY_FILTER,
                createVecFloat32Column<Array>(
                    {{1.0, 2.0, 3.0}, {0.0, 0.0, 0.0}, {1.0, 2.0, 3.5}},
                    dmsv.vec_column_name,
                    dmsv.vec_column_id));
        }

        auto ann_query_info = DM::tests::DeltaMergeStoreVectorBase::annQueryInfoTopK({
            .top_k = 1,
            .column_id = dmsv.vec_column_id,
            .index_id = idx_id,
        });

        // read with ANN query
        {
            SCOPED_TRACE(fmt::format("after add vector index, read with ANN query 1"));
            ann_query_info->set_top_k(1);
            ann_query_info->set_ref_vec_f32(dmsv.encodeVectorFloat32({1.0, 2.0, 3.5}));

            auto filter = std::make_shared<DM::PushDownExecutor>(ann_query_info);

            dmsv.read(range, filter, createVecFloat32Column<Array>({{1.0, 2.0, 3.5}}));
        }

        // read with ANN query
        {
            SCOPED_TRACE(fmt::format("after add vector index, read with ANN query 2"));
            ann_query_info->set_top_k(1);
            ann_query_info->set_ref_vec_f32(dmsv.encodeVectorFloat32({1.0, 2.0, 3.8}));

            auto filter = std::make_shared<DM::PushDownExecutor>(ann_query_info);

            dmsv.read(range, filter, createVecFloat32Column<Array>({{1.0, 2.0, 3.5}}));
        }
    }

    // drop a vector index
    MockTiDB::instance().dropVectorIndexFromTable(db_name, "t1", idx_id);

    // sync schema, the VectorIndex in TableInfo is not get updated
    {
        refreshSchema();
        idx_infos = mustGetSyncedTable(t1_id)->getTableInfo().index_infos;
        ASSERT_EQ(idx_infos.size(), 1);
        for (const auto & idx : idx_infos)
        {
            if (idx.vector_index)
            {
                ASSERT_EQ(idx.vector_index->kind, vector_index->kind);
                ASSERT_EQ(idx.vector_index->dimension, vector_index->dimension);
                ASSERT_EQ(idx.vector_index->distance_metric, vector_index->distance_metric);
            }
        }
    }

    // sync table schema, the VectorIndex in TableInfo should be updated
    {
        refreshTableSchema(t1_id);
        idx_infos = mustGetSyncedTable(t1_id)->getTableInfo().index_infos;
        ASSERT_EQ(idx_infos.size(), 0);
    }
}
CATCH

TEST_F(SchemaSyncTest, SyncTableWithVectorIndexCase1)
try
{
    auto pd_client = global_ctx.getTMTContext().getPDClient();

    const String db_name = "mock_db";
    MockTiDB::instance().newDataBase(db_name);

    auto cols = ColumnsDescription({
        {"col1", typeFromString("Int64")},
        {"vec", typeFromString("Array(Float32)")},
    });
    auto t1_id = MockTiDB::instance().newTable(db_name, "t1", cols, pd_client->getTS(), "");
    refreshSchema();

    // The `StorageDeltaMerge` is created but `DeltaMergeStore` is not inited
    StorageDeltaMergePtr storage = std::static_pointer_cast<StorageDeltaMerge>(mustGetSyncedTable(t1_id));
    {
        // The `DeltaMergeStore` is not inited
        ASSERT_EQ(nullptr, storage->getStoreIfInited());
        auto stats = getLocalIndexesStatsFromStorage(storage);
        ASSERT_FALSE(stats.has_value());
    }

    // add a vector index
    IndexID idx_id = 11;
    auto vector_index = std::make_shared<const TiDB::VectorIndexDefinition>(TiDB::VectorIndexDefinition{
        .kind = tipb::VectorIndexKind::HNSW,
        .dimension = 3,
        .distance_metric = tipb::VectorDistanceMetric::L2,
    });
    MockTiDB::instance().addVectorIndexToTable(db_name, "t1", idx_id, cols.getAllPhysical().back(), 0, vector_index);

    // sync table schema, the VectorIndex in TableInfo should be updated
    refreshTableSchema(t1_id);
    {
        // The `DeltaMergeStore` is not inited
        ASSERT_EQ(nullptr, storage->getStoreIfInited());
        auto stats = getLocalIndexesStatsFromStorage(storage);
        ASSERT_TRUE(stats.has_value());
        ASSERT_EQ(stats->size(), 1);
        auto & s = (*stats)[0];
        ASSERT_EQ(s.index_id, idx_id);
        ASSERT_EQ(s.rows_delta_indexed, 0);
        ASSERT_EQ(s.rows_delta_not_indexed, 0);
        ASSERT_EQ(s.rows_stable_indexed, 0);
        ASSERT_EQ(s.rows_stable_not_indexed, 0);
    }

    auto tbl_info = mustGetSyncedTable(t1_id)->getTableInfo();
    auto idx_infos = tbl_info.index_infos;
    ASSERT_EQ(idx_infos.size(), 1);
    for (const auto & idx : idx_infos)
    {
        ASSERT_EQ(idx.id, idx_id);
        ASSERT_NE(idx.vector_index, nullptr);
        ASSERT_EQ(idx.vector_index->kind, vector_index->kind);
        ASSERT_EQ(idx.vector_index->dimension, vector_index->dimension);
        ASSERT_EQ(idx.vector_index->distance_metric, vector_index->distance_metric);
    }
}
CATCH

TEST_F(SchemaSyncTest, SyncTableWithVectorIndexCase2)
try
{
    auto pd_client = global_ctx.getTMTContext().getPDClient();

    const String db_name = "mock_db";
    MockTiDB::instance().newDataBase(db_name);

    // The table is created and vector index is added. After that, the table info is synced to TiFlash
    auto cols = ColumnsDescription({
        {"col1", typeFromString("Int64")},
        {"vec", typeFromString("Array(Float32)")},
    });
    auto t1_id = MockTiDB::instance().newTable(db_name, "t1", cols, pd_client->getTS(), "");
    IndexID idx_id = 11;
    auto vector_index = std::make_shared<const TiDB::VectorIndexDefinition>(TiDB::VectorIndexDefinition{
        .kind = tipb::VectorIndexKind::HNSW,
        .dimension = 3,
        .distance_metric = tipb::VectorDistanceMetric::L2,
    });
    MockTiDB::instance().addVectorIndexToTable(db_name, "t1", idx_id, cols.getAllPhysical().back(), 0, vector_index);

    // Synced with mock tidb, and create the StorageDeltaMerge instance
    refreshTableSchema(t1_id);
    {
        // The `DeltaMergeStore` is not inited
        StorageDeltaMergePtr storage = std::static_pointer_cast<StorageDeltaMerge>(mustGetSyncedTable(t1_id));
        ASSERT_EQ(nullptr, storage->getStoreIfInited());
        auto stats = getLocalIndexesStatsFromStorage(storage);
        ASSERT_TRUE(stats.has_value());
        ASSERT_EQ(stats->size(), 1);
        auto & s = (*stats)[0];
        ASSERT_EQ(s.index_id, idx_id);
        ASSERT_EQ(s.rows_delta_indexed, 0);
        ASSERT_EQ(s.rows_delta_not_indexed, 0);
        ASSERT_EQ(s.rows_stable_indexed, 0);
        ASSERT_EQ(s.rows_stable_not_indexed, 0);
    }


    auto tbl_info = mustGetSyncedTable(t1_id)->getTableInfo();
    auto idx_infos = tbl_info.index_infos;
    ASSERT_EQ(idx_infos.size(), 1);
    for (const auto & idx : idx_infos)
    {
        ASSERT_EQ(idx.id, idx_id);
        ASSERT_NE(idx.vector_index, nullptr);
        ASSERT_EQ(idx.vector_index->kind, vector_index->kind);
        ASSERT_EQ(idx.vector_index->dimension, vector_index->dimension);
        ASSERT_EQ(idx.vector_index->distance_metric, vector_index->distance_metric);
    }
}
CATCH

} // namespace DB::tests
