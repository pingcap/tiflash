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
#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Flash/Disaggregated/MockS3LockClient.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/ExternalDTFileInfo.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/DeltaMerge/tests/MultiSegmentTestUtil.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/Disagg/CheckpointInfo.h>
#include <Storages/KVStore/MultiRaft/Disagg/FastAddPeerCache.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/tests/region_helper.h>
#include <Storages/Page/PageConstants.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalPageStorageService.h>
#include <Storages/PathPool.h>
#include <Storages/S3/S3Common.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <aws/s3/model/CreateBucketRequest.h>

namespace DB
{
namespace FailPoints
{
extern const char force_use_dmfile_format_v3[];
extern const char force_stop_background_checkpoint_upload[];
} // namespace FailPoints
namespace DM
{
extern DMFilePtr writeIntoNewDMFile(
    DMContext & dm_context,
    const ColumnDefinesPtr & schema_snap,
    const BlockInputStreamPtr & input_stream,
    UInt64 file_id,
    const String & parent_path);
namespace tests
{
// Simple test suit for DeltaMergeStoreTestFastAddPeer.
class DeltaMergeStoreTestFastAddPeer
    : public DB::base::TiFlashStorageTestBasic
    , public testing::WithParamInterface<KeyspaceID>
{
public:
    DeltaMergeStoreTestFastAddPeer()
        : keyspace_id(GetParam())
    {}

    void SetUp() override
    {
        FailPointHelper::enableFailPoint(FailPoints::force_use_dmfile_format_v3);
        FailPointHelper::enableFailPoint(FailPoints::force_stop_background_checkpoint_upload);
        DB::tests::TiFlashTestEnv::enableS3Config();
        auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
        ASSERT_TRUE(::DB::tests::TiFlashTestEnv::createBucketIfNotExist(*s3_client));
        TiFlashStorageTestBasic::SetUp();
        auto & global_context = TiFlashTestEnv::getGlobalContext();
        if (global_context.getSharedContextDisagg()->remote_data_store == nullptr)
        {
            already_initialize_data_store = false;
            global_context.getSharedContextDisagg()->initRemoteDataStore(
                global_context.getFileProvider(),
                /*s3_enabled*/ true);
            ASSERT_TRUE(global_context.getSharedContextDisagg()->remote_data_store != nullptr);
        }
        else
        {
            already_initialize_data_store = true;
        }
        if (global_context.getWriteNodePageStorage() == nullptr)
        {
            already_initialize_write_ps = false;
            orig_mode = global_context.getPageStorageRunMode();
            global_context.setPageStorageRunMode(PageStorageRunMode::UNI_PS);
            global_context.tryReleaseWriteNodePageStorageForTest();
            global_context.initializeWriteNodePageStorageIfNeed(global_context.getPathPool());
        }
        else
        {
            already_initialize_write_ps = true;
        }
        resetStoreId(current_store_id);
        global_context.getSharedContextDisagg()->initFastAddPeerContext(25);
    }

    void TearDown() override
    {
        FailPointHelper::disableFailPoint(FailPoints::force_use_dmfile_format_v3);
        FailPointHelper::disableFailPoint(FailPoints::force_stop_background_checkpoint_upload);
        auto & global_context = TiFlashTestEnv::getGlobalContext();
        if (!already_initialize_data_store)
        {
            global_context.getSharedContextDisagg()->remote_data_store = nullptr;
        }
        if (!already_initialize_write_ps)
        {
            global_context.setPageStorageRunMode(orig_mode);
        }
        auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
        ::DB::tests::TiFlashTestEnv::deleteBucket(*s3_client);
        DB::tests::TiFlashTestEnv::disableS3Config();
    }

    void resetStoreId(UInt64 store_id)
    {
        auto kvstore = db_context->getTMTContext().getKVStore();
        {
            auto meta_store = metapb::Store{};
            meta_store.set_id(store_id);
            kvstore->setStore(meta_store);
        }
    }

    DeltaMergeStorePtr reload(
        const ColumnDefinesPtr & pre_define_columns = {},
        bool is_common_handle = false,
        size_t rowkey_column_size = 1)
    {
        TiFlashStorageTestBasic::reload();
        auto kvstore = db_context->getTMTContext().getKVStore();
        auto store_id = kvstore->getStoreID();
        if (auto ps = DB::tests::TiFlashTestEnv::getGlobalContext().getWriteNodePageStorage(); ps)
        {
            auto mock_s3lock_client
                = std::make_shared<DB::S3::MockS3LockClient>(DB::S3::ClientFactory::instance().sharedTiFlashClient());
            ps->initLocksLocalManager(store_id, mock_s3lock_client);
        }
        ColumnDefinesPtr cols;
        if (!pre_define_columns)
            cols = DMTestEnv::getDefaultColumns(
                is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);
        else
            cols = pre_define_columns;

        ColumnDefine handle_column_define = (*cols)[0];

        DeltaMergeStorePtr s = std::make_shared<DeltaMergeStore>(
            *db_context,
            false,
            "test",
            fmt::format("t_{}", table_id),
            keyspace_id,
            table_id,
            true,
            *cols,
            handle_column_define,
            is_common_handle,
            rowkey_column_size,
            DeltaMergeStore::Settings());
        return s;
    }

protected:
    std::pair<RowKeyRange, std::vector<ExternalDTFileInfo>> genDMFile(DMContext & context, const Block & block)
    {
        auto input_stream = std::make_shared<OneBlockInputStream>(block);
        auto [store_path, file_id] = store->preAllocateIngestFile();

        auto dmfile = writeIntoNewDMFile(
            context,
            std::make_shared<ColumnDefines>(store->getTableColumns()),
            input_stream,
            file_id,
            store_path);

        auto delegator = context.path_pool->getStableDiskDelegator();
        // add the file id to delegator so later test code to retrieve the file path.
        delegator.addDTFile(file_id, /*file_size*/ 0, store_path);

        const auto & pk_column = block.getByPosition(0).column;
        auto min_pk = pk_column->getInt(0);
        auto max_pk = pk_column->getInt(block.rows() - 1);
        HandleRange range(min_pk, max_pk + 1);
        auto handle_range = RowKeyRange::fromHandleRange(range);
        auto external_file = ExternalDTFileInfo{.id = file_id, .range = handle_range};
        return {
            handle_range,
            {external_file}}; // There are some duplicated info. This is to minimize the change to our test code.
    }

    void dumpCheckpoint(UInt64 store_id)
    {
        auto temp_dir = getTemporaryPath() + "/";
        auto page_storage = db_context->getWriteNodePageStorage();
        auto wi = PS::V3::CheckpointProto::WriterInfo();
        {
            wi.set_store_id(store_id);
        }


        auto remote_store = db_context->getSharedContextDisagg()->remote_data_store;
        assert(remote_store != nullptr);
        UniversalPageStorage::DumpCheckpointOptions opts{
            .data_file_id_pattern = S3::S3Filename::newCheckpointDataNameTemplate(store_id, upload_sequence),
            .data_file_path_pattern = temp_dir + "dat_{seq}_{index}",
            .manifest_file_id_pattern = S3::S3Filename::newCheckpointManifestNameTemplate(store_id),
            .manifest_file_path_pattern = temp_dir + "mf_{seq}",
            .writer_info = wi,
            .must_locked_files = {},
            .persist_checkpoint = CheckpointUploadFunctor{
                .store_id = store_id,
                // Note that we use `upload_sequence` but not `snapshot.sequence` for
                // the S3 key.
                .sequence = upload_sequence,
                .remote_store = remote_store,
            },
            .override_sequence = upload_sequence, // override by upload_sequence
        };
        page_storage->dumpIncrementalCheckpoint(opts);
    }

    void clearData()
    {
        // clear data
        store->clearData();
        auto table_column_defines = DMTestEnv::getDefaultColumns();
        store = reload(table_column_defines);
        store->deleteRange(*db_context, db_context->getSettingsRef(), RowKeyRange::newAll(false, 1));
        store->flushCache(*db_context, RowKeyRange::newAll(false, 1), true);
        store->mergeDeltaAll(*db_context);
    }

    void verifyRows(const RowKeyRange & range, size_t rows)
    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            columns,
            {range},
            /* num_streams= */ 1,
            /* start_ts= */ std::numeric_limits<UInt64>::max(),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /* keep_order= */ false,
            /* is_fast_scan= */ false,
            /* expected_block_size= */ 1024)[0];
        ASSERT_INPUTSTREAM_NROWS(in, rows);
    }

protected:
    DeltaMergeStorePtr store;
    UInt64 current_store_id = 100;
    KeyspaceID keyspace_id;
    TableID table_id = 800;
    UInt64 upload_sequence = 1000;
    bool already_initialize_data_store = false;
    bool already_initialize_write_ps = false;
    DB::PageStorageRunMode orig_mode = PageStorageRunMode::ONLY_V3;

    constexpr static const char * TRACING_NAME = "DeltaMergeStoreTestFastAddPeer";
};

TEST_P(DeltaMergeStoreTestFastAddPeer, SimpleWriteReadAfterRestoreFromCheckPoint)
try
{
    UInt64 write_store_id = current_store_id + 1;
    resetStoreId(write_store_id);
    {
        auto table_column_defines = DMTestEnv::getDefaultColumns();

        store = reload(table_column_defines);
    }

    const size_t num_rows_write = 128;
    // write DMFile
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        store->write(*db_context, db_context->getSettingsRef(), block);
        store->flushCache(*db_context, RowKeyRange::newAll(false, 1), true);
        store->mergeDeltaAll(*db_context);
    }

    // Write ColumnFileTiny
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(num_rows_write, num_rows_write + num_rows_write, false);
        store->write(*db_context, db_context->getSettingsRef(), block);
        store->flushCache(*db_context, RowKeyRange::newAll(false, 1), true);
    }

    // write ColumnFileDeleteRange
    {
        HandleRange handle_range(0, num_rows_write / 2);
        store->deleteRange(*db_context, db_context->getSettingsRef(), RowKeyRange::fromHandleRange(handle_range));
        store->flushCache(*db_context, RowKeyRange::newAll(false, 1), true);
    }

    // write ColumnFileBig
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(
            num_rows_write + num_rows_write,
            num_rows_write + 2 * num_rows_write,
            false);
        auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
        auto [range, file_ids] = genDMFile(*dm_context, block);
        {
            // Mock DMFiles are uploaded to S3 in SSTFilesToDTFilesOutputStream
            auto remote_store = db_context->getSharedContextDisagg()->remote_data_store;
            ASSERT_NE(remote_store, nullptr);
            auto delegator = dm_context->path_pool->getStableDiskDelegator();
            for (const auto & file_id : file_ids)
            {
                auto dm_file = DMFile::restore(
                    db_context->getFileProvider(),
                    file_id.id,
                    file_id.id,
                    delegator.getDTFilePath(file_id.id),
                    DMFileMeta::ReadMode::all());
                remote_store->putDMFile(
                    dm_file,
                    S3::DMFileOID{
                        .store_id = write_store_id,
                        .keyspace_id = keyspace_id,
                        .table_id = store->physical_table_id,
                        .file_id = file_id.id,
                    },
                    true);
            }
        }
        store->ingestFiles(dm_context, range, file_ids, false);
        store->flushCache(*db_context, RowKeyRange::newAll(false, 1), true);
    }

    dumpCheckpoint(write_store_id);

    clearData();

    verifyRows(RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()), 0);

    const auto manifest_key = S3::S3Filename::newCheckpointManifest(write_store_id, upload_sequence).toFullKey();
    auto checkpoint_info = std::make_shared<CheckpointInfo>();
    checkpoint_info->remote_store_id = write_store_id;
    checkpoint_info->region_id = 1000;
    checkpoint_info->checkpoint_data_holder = buildParsedCheckpointData(*db_context, manifest_key, /*dir_seq*/ 100);
    checkpoint_info->temp_ps = checkpoint_info->checkpoint_data_holder->getUniversalPageStorage();
    resetStoreId(current_store_id);
    {
        auto table_column_defines = DMTestEnv::getDefaultColumns();

        store = reload(table_column_defines);
    }

    auto segments = store->buildSegmentsFromCheckpointInfo(
        *db_context,
        db_context->getSettingsRef(),
        RowKeyRange::newAll(false, 1),
        checkpoint_info);
    auto start = RecordKVFormat::genKey(table_id, 0);
    auto end = RecordKVFormat::genKey(table_id, 10);
    RegionPtr dummy_region = tests::makeRegion(checkpoint_info->region_id, start, end, nullptr);
    store->ingestSegmentsFromCheckpointInfo(
        *db_context,
        db_context->getSettingsRef(),
        RowKeyRange::newAll(false, 1),
        std::make_shared<CheckpointIngestInfo>(
            db_context->getTMTContext(),
            checkpoint_info->region_id,
            2333,
            checkpoint_info->remote_store_id,
            dummy_region,
            std::move(segments),
            0));

    // check data file lock exists
    {
        const auto data_key = S3::S3Filename::newCheckpointData(write_store_id, upload_sequence, 0).toFullKey();
        const auto data_key_view = S3::S3FilenameView::fromKey(data_key);
        const auto lock_prefix = data_key_view.getLockPrefix();
        auto client = S3::ClientFactory::instance().sharedTiFlashClient();
        std::set<String> lock_keys;
        S3::listPrefix(*client, lock_prefix, [&](const Aws::S3::Model::Object & object) {
            const auto & lock_key = object.GetKey();
            // also store the object.GetLastModified() for removing
            // outdated manifest objects
            lock_keys.emplace(lock_key);
            return DB::S3::PageResult{.num_keys = 1, .more = true};
        });
        // 2 lock files, 1 from write store, 1 from current store
        ASSERT_EQ(lock_keys.size(), 2);
        bool current_store_lock_exist = false;
        for (const auto & lock_key : lock_keys)
        {
            auto lock_key_view = S3::S3FilenameView::fromKey(lock_key);
            ASSERT_TRUE(lock_key_view.isLockFile());
            auto lock_info = lock_key_view.getLockInfo();
            if (lock_info.store_id == current_store_id)
                current_store_lock_exist = true;
        }
        ASSERT_TRUE(current_store_lock_exist);
    }

    verifyRows(
        RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()),
        num_rows_write / 2 + 2 * num_rows_write);

    reload();

    verifyRows(
        RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()),
        num_rows_write / 2 + 2 * num_rows_write);
}
CATCH

TEST_P(DeltaMergeStoreTestFastAddPeer, SimpleWriteReadAfterRestoreFromCheckPointWithSplit)
try
{
    auto & global_settings = TiFlashTestEnv::getGlobalContext().getSettingsRef();
    // store the old value to restore global_context settings after the test finish to avoid influence other tests
    auto old_global_settings = global_settings;

    // change the settings to make it easier to trigger splitting segments
    Settings settings;
    settings.dt_segment_limit_rows = 11;
    settings.dt_segment_limit_size = 20;
    settings.dt_segment_delta_limit_rows = 7;
    settings.dt_segment_delta_limit_size = 20;
    settings.dt_segment_force_split_size = 100;
    settings.dt_segment_delta_cache_limit_size = 20;

    // we need change the settings in both the ctx we get just below and the global_context above.
    // because when processing write request, `DeltaMergeStore` will call `checkSegmentUpdate` with the context we just get below.
    // and when initialize `DeltaMergeStore`, it will call `checkSegmentUpdate` with the global_context above.
    // so we need to make the settings in these two contexts consistent.
    global_settings = settings;
    auto old_db_context = std::move(db_context);
    db_context = DMTestEnv::getContext(settings);
    SCOPE_EXIT({
        global_settings = old_global_settings;
        db_context = std::move(old_db_context);
    });
    {
        auto table_column_defines = DMTestEnv::getDefaultColumns();

        store = reload(table_column_defines);
    }

    size_t num_rows_write = 0;
    size_t num_rows_write_per_batch = 128;
    // write until split and use a big enough finite for loop to make sure the test won't hang forever
    for (size_t i = 0; i < 100000; i++)
    {
        // write to store
        Block block
            = DMTestEnv::prepareSimpleWriteBlock(num_rows_write, num_rows_write + num_rows_write_per_batch, false);
        store->write(*db_context, settings, block);
        store->flushCache(*db_context, RowKeyRange::newAll(false, 1), true);
        num_rows_write += num_rows_write_per_batch;
        if (store->getSegmentsStats().size() > 1)
            break;
    }
    {
        ASSERT_GT(store->getSegmentsStats().size(), 1);
    }
    store->mergeDeltaAll(*db_context);

    UInt64 write_store_id = current_store_id + 1;
    dumpCheckpoint(write_store_id);

    clearData();

    verifyRows(RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()), 0);

    const auto manifest_key = S3::S3Filename::newCheckpointManifest(write_store_id, upload_sequence).toFullKey();
    auto checkpoint_info = std::make_shared<CheckpointInfo>();
    checkpoint_info->remote_store_id = write_store_id;
    checkpoint_info->region_id = 1000;
    checkpoint_info->checkpoint_data_holder = buildParsedCheckpointData(*db_context, manifest_key, /*dir_seq*/ 100);
    checkpoint_info->temp_ps = checkpoint_info->checkpoint_data_holder->getUniversalPageStorage();

    {
        auto segments = store->buildSegmentsFromCheckpointInfo(
            *db_context,
            db_context->getSettingsRef(),
            RowKeyRange::fromHandleRange(HandleRange(0, num_rows_write / 2)),
            checkpoint_info);
        auto start = RecordKVFormat::genKey(table_id, 0);
        auto end = RecordKVFormat::genKey(table_id, 10);
        RegionPtr dummy_region = tests::makeRegion(checkpoint_info->region_id, start, end, nullptr);
        store->ingestSegmentsFromCheckpointInfo(
            *db_context,
            db_context->getSettingsRef(),
            RowKeyRange::fromHandleRange(HandleRange(0, num_rows_write / 2)),
            std::make_shared<CheckpointIngestInfo>(
                db_context->getTMTContext(),
                checkpoint_info->region_id,
                2333,
                checkpoint_info->remote_store_id,
                dummy_region,
                std::move(segments),
                0));
        verifyRows(RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()), num_rows_write / 2);
    }

    {
        auto segments = store->buildSegmentsFromCheckpointInfo(
            *db_context,
            db_context->getSettingsRef(),
            RowKeyRange::fromHandleRange(HandleRange(num_rows_write / 2, num_rows_write)),
            checkpoint_info);
        auto start = RecordKVFormat::genKey(table_id, 0);
        auto end = RecordKVFormat::genKey(table_id, 10);
        RegionPtr dummy_region = tests::makeRegion(checkpoint_info->region_id, start, end, nullptr);
        store->ingestSegmentsFromCheckpointInfo(
            *db_context,
            db_context->getSettingsRef(),
            RowKeyRange::fromHandleRange(HandleRange(num_rows_write / 2, num_rows_write)),
            std::make_shared<CheckpointIngestInfo>(
                db_context->getTMTContext(),
                checkpoint_info->region_id,
                2333,
                checkpoint_info->remote_store_id,
                dummy_region,
                std::move(segments),
                0));
        verifyRows(RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()), num_rows_write);
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(Type, DeltaMergeStoreTestFastAddPeer, testing::Values(NullspaceID, 300));
} // namespace tests
} // namespace DM
} // namespace DB
