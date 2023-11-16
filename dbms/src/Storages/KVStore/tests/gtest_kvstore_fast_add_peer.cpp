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

#include <Debug/MockKVStore/MockRaftStoreProxy.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/Filter/PushDownFilter.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/MultiRaft/Disagg/FastAddPeer.h>
#include <Storages/KVStore/MultiRaft/Disagg/FastAddPeerCache.h>
#include <Storages/KVStore/Utils/AsyncTasks.h>
#include <Storages/KVStore/tests/kvstore_helper.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalPageStorageService.h>
#include <Storages/S3/CheckpointManifestS3Set.h>
#include <Storages/S3/S3Common.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <common/logger_useful.h>

#include <chrono>
#include <numeric>
#include <optional>
#include <thread>

using raft_serverpb::RaftApplyState;
using raft_serverpb::RegionLocalState;

namespace DB
{
FastAddPeerRes genFastAddPeerRes(FastAddPeerStatus status, std::string && apply_str, std::string && region_str);
FastAddPeerRes FastAddPeerImpl(
    TMTContext & tmt,
    TiFlashRaftProxyHelper * proxy_helper,
    uint64_t region_id,
    uint64_t new_peer_id);
FastAddPeerRes FastAddPeerImplIngest(
    TMTContext & tmt,
    uint64_t region_id,
    uint64_t new_peer_id,
    CheckpointRegionInfoAndData && checkpoint);
void ApplyFapSnapshotImpl(
    TMTContext & tmt,
    TiFlashRaftProxyHelper * proxy_helper,
    uint64_t region_id,
    uint64_t peer_id);

namespace tests
{
class RegionKVStoreTestFAP : public KVStoreTestBase
{
public:
    void SetUp() override
    {
        KVStoreTestBase::SetUp();
        DB::tests::TiFlashTestEnv::enableS3Config();
        auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
        ASSERT_TRUE(::DB::tests::TiFlashTestEnv::createBucketIfNotExist(*s3_client));

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
        orig_mode = global_context.getPageStorageRunMode();
        global_context.setPageStorageRunMode(PageStorageRunMode::UNI_PS);
        global_context.getSharedContextDisagg()->initFastAddPeerContext();
        KVStoreTestBase::SetUp();
    }

    void TearDown() override
    {
        auto & global_context = TiFlashTestEnv::getGlobalContext();
        if (!already_initialize_data_store)
        {
            global_context.getSharedContextDisagg()->remote_data_store = nullptr;
        }
        global_context.setPageStorageRunMode(orig_mode);
        if (!already_initialize_write_ps)
        {
            global_context.setPageStorageRunMode(orig_mode);
        }
        auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
        ::DB::tests::TiFlashTestEnv::deleteBucket(*s3_client);
        DB::tests::TiFlashTestEnv::disableS3Config();
    }

protected:
    void dumpCheckpoint()
    {
        auto & global_context = TiFlashTestEnv::getGlobalContext();
        auto temp_dir = TiFlashTestEnv::getTemporaryPath() + "/";
        auto page_storage = global_context.getWriteNodePageStorage();
        KVStore & kvs = getKVS();
        auto store_id = kvs.getStore().store_id.load();
        LOG_DEBUG(log, "dumpCheckpoint for checkpoint {}", store_id);
        auto wi = PS::V3::CheckpointProto::WriterInfo();
        {
            wi.set_store_id(store_id);
        }

        auto remote_store = global_context.getSharedContextDisagg()->remote_data_store;
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
        LOG_DEBUG(log, "!!!!! end dumpCheckpoint for checkpoint {}", store_id);
    }

protected:
    UInt64 upload_sequence = 1000;

private:
    ContextPtr context;
    bool already_initialize_data_store = false;
    bool already_initialize_write_ps = false;
    DB::PageStorageRunMode orig_mode;
};

void persistAfterWrite(
    Context & ctx,
    KVStore & kvs,
    std::unique_ptr<MockRaftStoreProxy> & proxy_instance,
    UniversalPageStoragePtr page_storage,
    uint64_t region_id,
    uint64_t index)
{
    MockRaftStoreProxy::FailCond cond;
    proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
    auto region = proxy_instance->getRegion(region_id);
    auto wb = region->persistMeta();
    page_storage->write(std::move(wb));
    // There shall be data to flush.
    ASSERT_EQ(kvs.needFlushRegionData(region_id, ctx.getTMTContext()), true);
    ASSERT_EQ(kvs.tryFlushRegionData(region_id, false, false, ctx.getTMTContext(), 0, 0, 0, 0), true);
}

TEST_F(RegionKVStoreTestFAP, RestoreRaftState)
try
{
    auto & global_context = TiFlashTestEnv::getGlobalContext();
    uint64_t region_id = 1;
    auto peer_id = 1;
    KVStore & kvs = getKVS();
    auto page_storage = global_context.getWriteNodePageStorage();

    proxy_instance->bootstrapWithRegion(kvs, global_context.getTMTContext(), region_id, std::nullopt);
    auto region = proxy_instance->getRegion(region_id);
    auto store_id = kvs.getStore().store_id.load();
    region->addPeer(store_id, peer_id, metapb::PeerRole::Learner);

    // Write some data, and persist meta.
    auto [index, term]
        = proxy_instance->normalWrite(region_id, {34}, {"v2"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
    kvs.setRegionCompactLogConfig(0, 0, 0, 0);
    persistAfterWrite(global_context, kvs, proxy_instance, page_storage, region_id, index);

    auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
    ASSERT_TRUE(::DB::tests::TiFlashTestEnv::createBucketIfNotExist(*s3_client));
    dumpCheckpoint();

    auto fap_context = global_context.getSharedContextDisagg()->fap_context;
    {
        auto [data_seq, checkpoint_data_holder] = fap_context->getNewerCheckpointData(global_context, store_id, 0);
        ASSERT_GT(data_seq, 0);
        ASSERT_TRUE(checkpoint_data_holder != nullptr);

        RaftApplyState apply_state;
        {
            // TODO: use `RaftDataReader::readRegionApplyState`?
            auto apply_state_key = UniversalPageIdFormat::toRaftApplyStateKeyInKVEngine(region_id);
            auto page = checkpoint_data_holder->getUniversalPageStorage()->read(apply_state_key);
            apply_state.ParseFromArray(page.data.begin(), page.data.size());
        }

        RegionLocalState region_state;
        {
            auto local_state_key = UniversalPageIdFormat::toRegionLocalStateKeyInKVEngine(region_id);
            auto page = checkpoint_data_holder->getUniversalPageStorage()->read(local_state_key);
            region_state.ParseFromArray(page.data.begin(), page.data.size());
        }

        ASSERT_TRUE(apply_state == region->getApply());
        ASSERT_TRUE(region_state == region->getState());
    }

    {
        auto [data_seq, checkpoint_data_holder]
            = fap_context->getNewerCheckpointData(global_context, store_id, upload_sequence);
        ASSERT_EQ(data_seq, upload_sequence);
        ASSERT_TRUE(checkpoint_data_holder == nullptr);
    }
}
CATCH


void verifyRows(Context & ctx, DM::DeltaMergeStorePtr store, const DM::RowKeyRange & range, size_t rows)
{
    const auto & columns = store->getTableColumns();
    BlockInputStreamPtr in = store->read(
        ctx,
        ctx.getSettingsRef(),
        columns,
        {range},
        /* num_streams= */ 1,
        /* max_version= */ std::numeric_limits<UInt64>::max(),
        DM::EMPTY_FILTER,
        std::vector<RuntimeFilterPtr>{},
        0,
        "KVStoreFastAddPeer",
        /* keep_order= */ false,
        /* is_fast_scan= */ false,
        /* expected_block_size= */ 1024)[0];
    ASSERT_INPUTSTREAM_NROWS(in, rows);
}

TEST_F(RegionKVStoreTestFAP, RestoreFromRestart)
try
{
    auto & global_context = TiFlashTestEnv::getGlobalContext();
    uint64_t region_id = 1;
    auto peer_id = 1;
    initStorages();
    KVStore & kvs = getKVS();
    global_context.getTMTContext().debugSetKVStore(kvstore);
    auto page_storage = global_context.getWriteNodePageStorage();
    TableID table_id = proxy_instance->bootstrapTable(global_context, kvs, global_context.getTMTContext());
    auto fap_context = global_context.getSharedContextDisagg()->fap_context;
    proxy_instance->bootstrapWithRegion(kvs, global_context.getTMTContext(), region_id, std::nullopt);
    auto proxy_helper = proxy_instance->generateProxyHelper();
    auto region = proxy_instance->getRegion(region_id);
    auto store_id = kvs.getStore().store_id.load();
    region->addPeer(store_id, peer_id, metapb::PeerRole::Learner);

    // Write some data, and persist meta.
    auto k1 = RecordKVFormat::genKey(table_id, 1, 111);
    auto && [value_write1, value_default1] = proxy_instance->generateTiKVKeyValue(111, 999);
    auto [index, term] = proxy_instance->rawWrite(
        region_id,
        {k1, k1},
        {value_default1, value_write1},
        {WriteCmdType::Put, WriteCmdType::Put},
        {ColumnFamilyType::Default, ColumnFamilyType::Write});
    kvs.setRegionCompactLogConfig(0, 0, 0, 0);
    persistAfterWrite(global_context, kvs, proxy_instance, page_storage, region_id, index);

    auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
    ASSERT_TRUE(::DB::tests::TiFlashTestEnv::createBucketIfNotExist(*s3_client));
    dumpCheckpoint();

    LOG_INFO(log, "!!!!! SEUP 1");
    const auto manifest_key = S3::S3Filename::newCheckpointManifest(kvs.getStoreID(), upload_sequence).toFullKey();
    auto checkpoint_info = std::make_shared<CheckpointInfo>();
    checkpoint_info->remote_store_id = kvs.getStoreID();
    checkpoint_info->region_id = 1000;
    checkpoint_info->checkpoint_data_holder = buildParsedCheckpointData(global_context, manifest_key, /*dir_seq*/ 100);
    checkpoint_info->temp_ps = checkpoint_info->checkpoint_data_holder->getUniversalPageStorage();
    RegionPtr kv_region = kvs.getRegion(1);
    CheckpointRegionInfoAndData mock_data = std::make_tuple(
        checkpoint_info,
        kv_region,
        kv_region->mutMeta().clonedApplyState(),
        kv_region->mutMeta().clonedRegionState());

    LOG_INFO(log, "!!!!! SEUP 2");

    FastAddPeerImplIngest(global_context.getTMTContext(), region_id, 2333, std::move(mock_data));
    fap_context->removeCheckpointIngestInfo(region_id);
    ApplyFapSnapshotImpl(global_context.getTMTContext(), proxy_helper.get(), region_id, 2333);
    {
        auto keyspace_id = kv_region->getKeyspaceID();
        auto table_id = kv_region->getMappedTableID();
        auto storage = global_context.getTMTContext().getStorages().get(keyspace_id, table_id);
        ASSERT_TRUE(storage && storage->engineType() == TiDB::StorageEngine::DT);
        auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
        auto store = dm_storage->getStore();
        verifyRows(
            global_context,
            store,
            DM::RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()),
            1);
    }
    ASSERT_TRUE(!fap_context->tryGetCheckpointIngestInfo(region_id).has_value());
}
CATCH

} // namespace tests
} // namespace DB
