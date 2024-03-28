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

#pragma once

#include <Common/FailPoint.h>
#include <Common/Logger.h>
#include <Common/SyncPoint/SyncPoint.h>
#include <Debug/MockKVStore/MockRaftStoreProxy.h>
#include <Debug/MockKVStore/MockSSTReader.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/ExternalDTFileInfo.h>
#include <Storages/DeltaMerge/GCOptions.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/DeltaMerge/tests/gtest_dm_simple_pk_test_basic.h>
#include <Storages/KVStore/Decode/PartitionStreams.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/RegionExecutionResult.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/StorageEngineType.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/tests/region_helper.h>
#include <Storages/PathPool.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/registerStorages.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>

#include <memory>

namespace DB
{
extern void GenMockSSTData(
    const TiDB::TableInfo & table_info,
    TableID table_id,
    const String & store_key,
    UInt64 start_handle,
    UInt64 end_handle,
    UInt64 num_fields = 1,
    const std::unordered_set<ColumnFamilyType> & cfs = {ColumnFamilyType::Write, ColumnFamilyType::Default});

namespace FailPoints
{
extern const char skip_check_segment_update[];
extern const char force_fail_in_flush_region_data[];
extern const char proactive_flush_force_set_type[];
extern const char pause_passive_flush_before_persist_region[];
extern const char force_set_parallel_prehandle_threshold[];
} // namespace FailPoints

namespace RegionBench
{
extern void setupPutRequest(raft_cmdpb::Request *, const std::string &, const TiKVKey &, const TiKVValue &);
extern void setupDelRequest(raft_cmdpb::Request *, const std::string &, const TiKVKey &);
} // namespace RegionBench

extern void CheckRegionForMergeCmd(const raft_cmdpb::AdminResponse & response, const RegionState & region_state);
extern void ChangeRegionStateRange(
    RegionState & region_state,
    bool source_at_left,
    const RegionState & source_region_state);

namespace tests
{
// TODO: Use another way to workaround calling the private methods on KVStore
class KVStoreTestBase : public ::testing::Test
{
public:
    KVStoreTestBase() { test_path = TiFlashTestEnv::getTemporaryPath("/region_kvs_test_base"); }

    static void SetUpTestCase() {}

    void SetUp() override
    {
        // clean data and create path pool instance
        path_pool = TiFlashTestEnv::createCleanPathPool(test_path);

        proxy_instance = std::make_unique<MockRaftStoreProxy>();
        proxy_helper = proxy_instance->generateProxyHelper();
        reloadKVSFromDisk();
        {
            auto store = metapb::Store{};
            store.set_id(1234);
            kvstore->setStore(store);
            ASSERT_EQ(kvstore->getStoreID(), store.id());
        }

        LOG_INFO(log, "Finished setup");
    }

    void TearDown() override { proxy_instance->clear(); }

protected:
    KVStore & getKVS() { return *kvstore; }
    void resetKVStoreStorage()
    {
        auto & global_ctx = TiFlashTestEnv::getGlobalContext();
        global_ctx.tryReleaseWriteNodePageStorageForTest();
        global_ctx.initializeWriteNodePageStorageIfNeed(*path_pool);
    }
    KVStore & reloadKVSFromDisk(bool with_reset = true)
    {
        auto & global_ctx = TiFlashTestEnv::getGlobalContext();
        kvstore.reset();
        if (with_reset)
            resetKVStoreStorage();
        kvstore = std::make_shared<KVStore>(global_ctx);
        // Only recreate kvstore and restore data from disk, don't recreate proxy instance
        kvstore->restore(*path_pool, proxy_helper.get());
        proxy_instance->reload();
        global_ctx.getTMTContext().getRegionTable().clear();
        return *kvstore;
    }
    // Only handle mock proxy's part. Conflicts with `debugAddRegions`.
    void createDefaultRegions() { proxy_instance->init(100); }
    void initStorages()
    {
        bool v = false;
        if (!has_init.compare_exchange_strong(v, true))
            return;
        try
        {
            registerStorages();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registed, ignore exception here.
        }
        String path = TiFlashTestEnv::getContext()->getPath();
        auto p = path + "/metadata/";
        TiFlashTestEnv::tryCreatePath(p);
        p = path + "/data/";
        TiFlashTestEnv::tryCreatePath(p);
    }
    void startReadIndexUtils(Context & ctx)
    {
        if (proxy_runner)
        {
            return;
        }
        over.store(false);
        ctx.getTMTContext().setStatusRunning();
        // Start mock proxy in other thread
        proxy_runner.reset(new std::thread([&]() { proxy_instance->testRunReadIndex(over); }));
        ASSERT_EQ(kvstore->getProxyHelper(), proxy_helper.get());
        kvstore->initReadIndexWorkers([]() { return std::chrono::milliseconds(10); }, 1);
        ASSERT_NE(kvstore->read_index_worker_manager, nullptr);
        kvstore->asyncRunReadIndexWorkers();
    }
    void stopReadIndexUtils()
    {
        kvstore->stopReadIndexWorkers();
        kvstore->releaseReadIndexWorkers();
        over = true;
        proxy_instance->mock_read_index.wakeNotifier();
        proxy_runner->join();
    }

    static void tryPersistRegion(KVStore & kvs, RegionID region_id)
    {
        if (auto region = kvs.getRegion(region_id); region)
        {
            auto region_task_lock = kvs.region_manager.genRegionTaskLock(region_id);
            kvs.persistRegion(*region, region_task_lock, PersistRegionReason::Debug, "");
        }
    }

protected:
    std::tuple<uint64_t, uint64_t, uint64_t> prepareForProactiveFlushTest();
    void testRaftMerge(Context & ctx, KVStore & kvs, TMTContext & tmt);
    static void testRaftMergeRollback(KVStore & kvs, TMTContext & tmt);
    static void dropTable(Context & ctx, TableID table_id);

    static std::unique_ptr<PathPool> createCleanPathPool(const String & path)
    {
        // Drop files on disk
        LOG_INFO(Logger::get("Test"), "Clean path {} for bootstrap", path);
        Poco::File file(path);
        if (file.exists())
            file.remove(true);
        file.createDirectories();

        auto & global_ctx = TiFlashTestEnv::getGlobalContext();
        auto path_capacity = global_ctx.getPathCapacity();
        auto provider = global_ctx.getFileProvider();
        // Create a PathPool instance on the clean directory
        Strings main_data_paths{path};
        return std::make_unique<PathPool>(main_data_paths, main_data_paths, Strings{}, path_capacity, provider);
    }

    std::atomic_bool has_init{false};
    std::string test_path;

    std::unique_ptr<PathPool> path_pool;
    std::shared_ptr<KVStore> kvstore;

    std::unique_ptr<MockRaftStoreProxy> proxy_instance;
    std::unique_ptr<TiFlashRaftProxyHelper> proxy_helper;
    std::unique_ptr<std::thread> proxy_runner;

    LoggerPtr log = DB::Logger::get("KVStoreTestBase");
    std::atomic_bool over{false};
};

} // namespace tests
} // namespace DB
