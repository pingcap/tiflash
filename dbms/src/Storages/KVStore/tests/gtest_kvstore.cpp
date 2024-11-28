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

#include <Debug/dbgTools.h>
#include <Storages/KVStore/MultiRaft/RegionsRangeIndex.h>
#include <Storages/KVStore/tests/kvstore_helper.h>

#include <regex>


namespace DB::tests
{

class RegionKVStoreOldTest : public KVStoreTestBase
{
public:
    void testRaftMerge(Context & ctx, KVStore & kvs, TMTContext & tmt);
    static void testRaftMergeRollback(KVStore & kvs, TMTContext & tmt);
    RegionKVStoreOldTest()
    {
        log = DB::Logger::get("RegionKVStoreOldTest");
        test_path = TiFlashTestEnv::getTemporaryPath("/region_kvs_old_test");
    }
};

TEST_F(RegionKVStoreOldTest, PersistenceV1)
try
{
    auto ctx = TiFlashTestEnv::getGlobalContext();
    KVStore & kvs = getKVS();
    {
        ASSERT_EQ(kvs.getRegion(0), nullptr);
        proxy_instance->bootstrapWithRegion(kvs, ctx.getTMTContext(), 1, std::nullopt);
    }
    {
        tryPersistRegion(kvs, 1);
        kvs.gcPersistedRegion(Seconds{0});
    }
    {
        // test CompactLog
        auto region = kvs.getRegion(1);
        kvs.setRegionCompactLogConfig(1000, 1000, 0, 512);

        raft_cmdpb::AdminRequest request;
        request.mutable_compact_log();
        request.set_cmd_type(::raft_cmdpb::AdminCmdType::CompactLog);
        // CompactLog always returns true now, even if we can't do a flush.
        // We use a tryFlushData to pre-filter.
        raft_cmdpb::AdminResponse response;
        ASSERT_EQ(
            kvs.handleAdminRaftCmd(std::move(request), std::move(response), 1, 5, 1, ctx.getTMTContext()),
            EngineStoreApplyRes::Persist);

        // Filter
        ASSERT_EQ(kvs.tryFlushRegionData(1, false, false, ctx.getTMTContext(), 0, 0, 0, 0), false);
    }
}
CATCH

TEST_F(RegionKVStoreOldTest, ReadIndex)
{
    createDefaultRegions();
    auto ctx = TiFlashTestEnv::getGlobalContext();
    auto log = Logger::get();

    // Start mock proxy in other thread
    std::atomic_bool over{false};
    auto proxy_runner = std::thread([&]() { proxy_instance->testRunReadIndex(over); });
    KVStore & kvs = getKVS();
    ASSERT_EQ(kvs.getProxyHelper(), proxy_helper.get());

    {
        ASSERT_EQ(kvs.getRegion(0), nullptr);
        proxy_instance->debugAddRegions(
            kvs,
            ctx.getTMTContext(),
            {1, 2, 3},
            {{
                {RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 10)},
                {RecordKVFormat::genKey(1, 10), RecordKVFormat::genKey(1, 20)},
                {RecordKVFormat::genKey(1, 30), RecordKVFormat::genKey(1, 40)},
            }});
    }
    {
        // `read_index_worker_manager` is not set, fallback to v1.
        // We don't support batch read index version 1 now
        ASSERT_EQ(kvs.read_index_worker_manager, nullptr);
        {
            auto region = kvs.getRegion(1);
            auto req = GenRegionReadIndexReq(*region, 8);
            try
            {
                auto resp = kvs.batchReadIndex({req}, 100);
                ASSERT_TRUE(false);
            }
            catch (Exception & e)
            {
                ASSERT_EQ(e.message(), "`fn_handle_batch_read_index` is deprecated");
            }
        }
        kvs.initReadIndexWorkers([]() { return std::chrono::milliseconds(10); }, 1);
        ASSERT_NE(kvs.read_index_worker_manager, nullptr);
    }
    {
        {
            // Normal async notifier
            kvs.asyncRunReadIndexWorkers();
            SCOPE_EXIT({ kvs.stopReadIndexWorkers(); });

            UInt64 tar_region_id = 9;
            proxy_instance->debugAddRegions(
                kvs,
                ctx.getTMTContext(),
                {tar_region_id},
                {{{RecordKVFormat::genKey(2, 0), RecordKVFormat::genKey(2, 10)}}});
            {
                ASSERT_EQ(proxy_instance->regions.at(tar_region_id)->getLatestCommitIndex(), 5);
                proxy_instance->regions.at(tar_region_id)->updateCommitIndex(66);
            }

            AsyncWaker::Notifier notifier;
            const std::atomic_size_t terminate_signals_counter{};
            std::thread t([&]() {
                notifier.wake();
                WaitCheckRegionReadyImpl(ctx.getTMTContext(), kvs, terminate_signals_counter, 1 / 1000.0, 20, 20 * 60);
            });
            SCOPE_EXIT({
                t.join();
                kvs.handleDestroy(tar_region_id, ctx.getTMTContext());
            });
            ASSERT_EQ(notifier.blockedWaitFor(std::chrono::milliseconds(1000 * 3600)), AsyncNotifier::Status::Normal);
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            auto tar = kvs.getRegion(tar_region_id);
            ASSERT_EQ(tar->handleWriteRaftCmd({}, 66, 6, ctx.getTMTContext()).first, EngineStoreApplyRes::None);
        }
        {
            // Async notifier error
            kvs.asyncRunReadIndexWorkers();
            SCOPE_EXIT({ kvs.stopReadIndexWorkers(); });

            auto tar_region_id = 9;
            {
                ASSERT_EQ(proxy_instance->regions.at(tar_region_id)->getLatestCommitIndex(), 66);
                proxy_instance->unsafeInvokeForTest([&](MockRaftStoreProxy & p) {
                    p.mock_read_index.region_id_to_error.emplace(tar_region_id);
                    p.regions.at(2)->updateCommitIndex(6);
                });
            }

            AsyncWaker::Notifier notifier;
            const std::atomic_size_t terminate_signals_counter{};
            std::thread t([&]() {
                notifier.wake();
                WaitCheckRegionReadyImpl(
                    ctx.getTMTContext(),
                    kvs,
                    terminate_signals_counter,
                    1 / 1000.0,
                    2 / 1000.0,
                    5 / 1000.0);
            });
            SCOPE_EXIT({ t.join(); });
            ASSERT_EQ(notifier.blockedWaitFor(std::chrono::milliseconds(1000 * 3600)), AsyncNotifier::Status::Normal);
        }

        // Test read index
        // Note `batchReadIndex` always returns latest committed index in our mock class.
        // See `RawMockReadIndexTask::poll`.
        kvs.asyncRunReadIndexWorkers();
        SCOPE_EXIT({ kvs.stopReadIndexWorkers(); });

        {
            auto region = kvs.getRegion(1);
            auto req = GenRegionReadIndexReq(*region, 8); // start_ts = 8
            auto resp = kvs.batchReadIndex({req}, 100);
            auto proxy_region = proxy_instance->getRegion(1);
            ASSERT_EQ(resp[0].first.read_index(), proxy_region->getLatestCommitIndex());
            ASSERT_EQ(5, proxy_region->getLatestCommitIndex());
            {
                auto r = region->waitIndex(
                    5,
                    0,
                    []() { return true; },
                    log);
                ASSERT_EQ(std::get<0>(r), WaitIndexStatus::Finished);
            }
            {
                auto r = region->waitIndex(
                    8,
                    1,
                    []() { return false; },
                    log);
                ASSERT_EQ(std::get<0>(r), WaitIndexStatus::Terminated);
            }
        }
        for (auto & r : proxy_instance->regions)
        {
            r.second->updateCommitIndex(667);
        }
        {
            // Found in `history_success_tasks`
            auto region = kvs.getRegion(1);
            auto req = GenRegionReadIndexReq(*region, 8);
            auto resp = kvs.batchReadIndex({req}, 100);
            ASSERT_EQ(resp[0].first.read_index(), 5);
        }
        {
            auto region = kvs.getRegion(1);
            auto req = GenRegionReadIndexReq(*region, 10);
            auto resp = kvs.batchReadIndex({req}, 100);
            ASSERT_EQ(resp[0].first.read_index(), 667);
        }
        {
            // Found updated value in `history_success_tasks`
            auto region = kvs.getRegion(1);
            auto req = GenRegionReadIndexReq(*region, 8);
            auto resp = kvs.batchReadIndex({req}, 100);
            ASSERT_EQ(resp[0].first.read_index(), 667);
        }
        {
            auto region = kvs.getRegion(2);
            auto req = GenRegionReadIndexReq(*region, 5);
            auto resp = proxy_helper->batchReadIndex({req}, 100); // v2
            ASSERT_EQ(resp[0].first.read_index(), 667); // got latest
            {
                auto r = region->waitIndex(
                    667 + 1,
                    2,
                    []() { return true; },
                    log);
                ASSERT_EQ(std::get<0>(r), WaitIndexStatus::Timeout);
            }
            {
                // Wait for a new index 667 + 1 to be applied
                AsyncWaker::Notifier notifier;
                std::thread t([&]() {
                    notifier.wake();
                    auto r = region->waitIndex(
                        667 + 1,
                        100000,
                        []() { return true; },
                        log);
                    ASSERT_EQ(std::get<0>(r), WaitIndexStatus::Finished);
                });
                SCOPE_EXIT({ t.join(); });
                ASSERT_EQ(
                    notifier.blockedWaitFor(std::chrono::milliseconds(1000 * 3600)),
                    AsyncNotifier::Status::Normal);
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
                region->handleWriteRaftCmd({}, 667 + 1, 6, ctx.getTMTContext());
            }
        }
    }
    kvs.stopReadIndexWorkers();
    kvs.releaseReadIndexWorkers();
    over = true;
    proxy_instance->mock_read_index.wakeNotifier();
    proxy_runner.join();
    ASSERT(GCMonitor::instance().checkClean());
    ASSERT(!GCMonitor::instance().empty());
}

void RegionKVStoreOldTest::testRaftMergeRollback(KVStore & kvs, TMTContext & tmt)
{
    uint64_t region_id = 7;
    {
        auto source_region = kvs.getRegion(region_id);
        auto target_region = kvs.getRegion(1);

        auto && [request, response]
            = MockRaftStoreProxy::composePrepareMerge(target_region->cloneMetaRegion(), source_region->appliedIndex());
        kvs.handleAdminRaftCmd(std::move(request), std::move(response), region_id, 31, 6, tmt);
        ASSERT_TRUE(source_region->isMerging());
    }
    {
        auto region = kvs.getRegion(region_id);
        auto && [request, response] = MockRaftStoreProxy::composeRollbackMerge(region->getMergeState().commit());
        region->setStateApplying();

        try
        {
            raft_cmdpb::AdminRequest first_request = request;
            raft_cmdpb::AdminResponse first_response = response;
            kvs.handleAdminRaftCmd(std::move(first_request), std::move(first_response), region_id, 32, 6, tmt);
            ASSERT_TRUE(false);
        }
        catch (Exception & e)
        {
            ASSERT_EQ(e.message(), "execRollbackMerge: region state is Applying, expect Merging");
        }
        ASSERT_EQ(region->peerState(), raft_serverpb::PeerState::Applying);
        region->setPeerState(raft_serverpb::PeerState::Merging);

        region->meta.region_state.getMutMergeState().set_commit(1234);
        try
        {
            kvs.handleAdminRaftCmd(std::move(request), std::move(response), region_id, 32, 6, tmt);
            ASSERT_TRUE(false);
        }
        catch (Exception & e)
        {
            ASSERT_EQ(e.message(), "execRollbackMerge: merge commit index is 1234, expect 31");
        }
        region->meta.region_state.getMutMergeState().set_commit(31);
    }
    {
        auto region = kvs.getRegion(region_id);
        auto && [request, response] = MockRaftStoreProxy::composeRollbackMerge(region->getMergeState().commit());
        kvs.handleAdminRaftCmd(std::move(request), std::move(response), region_id, 32, 6, tmt);
        ASSERT_EQ(region->peerState(), raft_serverpb::PeerState::Normal);
    }
}

static void testRaftSplit(KVStore & kvs, TMTContext & tmt, std::unique_ptr<MockRaftStoreProxy> & proxy_instance)
{
    const TableID table_id = 1;
    {
        auto region = kvs.getRegion(1);
        // row with handle_id == 3
        region->insert(
            "lock",
            RecordKVFormat::genKey(table_id, 3),
            RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 3, 20));
        region->insert("default", RecordKVFormat::genKey(table_id, 3, 5), TiKVValue("value1"));
        region->insert(
            "write",
            RecordKVFormat::genKey(table_id, 3, 8),
            RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 5));
        // row with handle_id == 8
        region->insert(
            "lock",
            RecordKVFormat::genKey(table_id, 8),
            RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 3, 20));
        region->insert("default", RecordKVFormat::genKey(table_id, 8, 5), TiKVValue("value1"));
        region->insert(
            "write",
            RecordKVFormat::genKey(table_id, 8, 8),
            RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 5));

        ASSERT_EQ(region->dataInfo(), "[write 2 lock 2 default 2 ]");
    }

    // Split region
    RegionID region_id = 1;
    RegionID region_id2 = 7;
    auto source_region = kvs.getRegion(region_id);
    auto old_epoch = source_region->getMeta().getMetaRegion().region_epoch();
    const auto & ori_source_range = source_region->getRange()->comparableKeys();
    RegionRangeKeys::RegionRange new_source_range = RegionRangeKeys::makeComparableKeys( //
        RecordKVFormat::genKey(table_id, 5),
        RecordKVFormat::genKey(table_id, 10));
    RegionRangeKeys::RegionRange new_target_range = RegionRangeKeys::makeComparableKeys( //
        RecordKVFormat::genKey(table_id, 0),
        RecordKVFormat::genKey(table_id, 5));
    auto && [request, response] = MockRaftStoreProxy::composeBatchSplit(
        {region_id, region_id2},
        regionRangeToEncodeKeys(new_source_range, new_target_range),
        old_epoch);
    kvs.handleAdminRaftCmd(raft_cmdpb::AdminRequest(request), raft_cmdpb::AdminResponse(response), 1, 20, 5, tmt);
    {
        auto mmp = kvs.getRegionsByRangeOverlap(new_target_range);
        ASSERT_TRUE(mmp.count(region_id2) != 0);
        ASSERT_EQ(mmp.size(), 1);
    }
    {
        auto mmp = kvs.getRegionsByRangeOverlap(new_source_range);
        ASSERT_TRUE(mmp.count(region_id) != 0);
        ASSERT_EQ(mmp.size(), 1);
    }
    {
        // We don't force migration of committed data from derived region to dm store.
        ASSERT_EQ(kvs.getRegion(region_id)->dataInfo(), "[write 1 lock 1 default 1 ]");
        ASSERT_EQ(kvs.getRegion(region_id2)->dataInfo(), "[lock 1 ]");
    }
    // Rollback 1 to before split
    // 7 is persisted
    {
        kvs.handleDestroy(1, tmt);
        proxy_instance->debugAddRegions(kvs, tmt, {1}, {{ori_source_range.first.key, ori_source_range.second.key}});

        auto table_id = 1;
        auto region = kvs.getRegion(1);
        region->insert(
            "lock",
            RecordKVFormat::genKey(table_id, 3),
            RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 3, 20));
        region->insert("default", RecordKVFormat::genKey(table_id, 3, 5), TiKVValue("value1"));
        region->insert(
            "write",
            RecordKVFormat::genKey(table_id, 3, 8),
            RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 5));
        region->insert(
            "lock",
            RecordKVFormat::genKey(table_id, 8),
            RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 3, 20));
        region->insert("default", RecordKVFormat::genKey(table_id, 8, 5), TiKVValue("value1"));
        region->insert(
            "write",
            RecordKVFormat::genKey(table_id, 8, 8),
            RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 5));

        ASSERT_EQ(region->dataInfo(), "[write 2 lock 2 default 2 ]");
    }
    {
        // Region 1 and 7 overlaps.
        auto mmp = kvs.getRegionsByRangeOverlap(new_target_range);
        ASSERT_TRUE(mmp.count(7) != 0);
        ASSERT_TRUE(mmp.count(1) != 0);
        ASSERT_EQ(mmp.size(), 2);
    }
    // Split again
    kvs.handleAdminRaftCmd(raft_cmdpb::AdminRequest(request), raft_cmdpb::AdminResponse(response), 1, 20, 5, tmt);
    {
        auto mmp = kvs.getRegionsByRangeOverlap(new_target_range);
        ASSERT_TRUE(mmp.count(7) != 0);
        ASSERT_EQ(mmp.size(), 1);
    }
    {
        auto mmp = kvs.getRegionsByRangeOverlap(new_source_range);
        ASSERT_TRUE(mmp.count(1) != 0);
        ASSERT_EQ(mmp.size(), 1);
    }
    {
        ASSERT_EQ(kvs.getRegion(1)->dataInfo(), "[write 1 lock 1 default 1 ]");
        ASSERT_EQ(kvs.getRegion(7)->dataInfo(), "[lock 1 ]");
    }
}

void RegionKVStoreOldTest::testRaftMerge(Context & ctx, KVStore & kvs, TMTContext & tmt)
{
    const RegionID source_region_id = 7;
    const RegionID target_region_id = 1;
    const TableID table_id = 1;
    proxy_instance->debugAddRegions(
        kvs,
        ctx.getTMTContext(),
        {target_region_id, source_region_id},
        {{RecordKVFormat::genKey(table_id, 0), RecordKVFormat::genKey(table_id, 5)},
         {RecordKVFormat::genKey(table_id, 5), RecordKVFormat::genKey(table_id, 10)}});

    {
        kvs.getRegion(target_region_id)->clearAllData();
        kvs.getRegion(source_region_id)->clearAllData();

        {
            // Region 1 with handle_id == 6
            auto region = kvs.getRegion(target_region_id);
            region->insert(
                "lock",
                RecordKVFormat::genKey(table_id, 6),
                RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 3, 20));
            region->insert("default", RecordKVFormat::genKey(table_id, 6, 5), TiKVValue("value1"));
            region->insert(
                "write",
                RecordKVFormat::genKey(table_id, 6, 8),
                RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 5));
            ASSERT_EQ(region->dataInfo(), "[write 1 lock 1 default 1 ]");
        }
        {
            // Region 7 with handle_id == 2
            auto region = kvs.getRegion(source_region_id);
            region->insert(
                "lock",
                RecordKVFormat::genKey(table_id, 2),
                RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 3, 20));
            region->insert("default", RecordKVFormat::genKey(table_id, 2, 5), TiKVValue("value1"));
            region->insert(
                "write",
                RecordKVFormat::genKey(table_id, 2, 8),
                RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 5));
            ASSERT_EQ(region->dataInfo(), "[write 1 lock 1 default 1 ]");
        }
    }

    // Prepare merge for region 7
    {
        auto source_region = kvs.getRegion(source_region_id);
        auto target_region = kvs.getRegion(target_region_id);
        auto && [request, response]
            = MockRaftStoreProxy::composePrepareMerge(target_region->cloneMetaRegion(), source_region->appliedIndex());
        kvs.handleAdminRaftCmd(std::move(request), std::move(response), source_region->id(), 35, 6, tmt);
        ASSERT_EQ(source_region->peerState(), raft_serverpb::PeerState::Merging);
    }

    {
        /// Mock that source region is Applying, reject
        auto source_region = kvs.getRegion(source_region_id);
        auto && [request, response]
            = MockRaftStoreProxy::composeCommitMerge(source_region->cloneMetaRegion(), source_region->appliedIndex());
        source_region->setStateApplying();
        const auto & source_region_meta_delegate = source_region->meta.makeRaftCommandDelegate();
        try
        {
            kvs.getRegion(target_region_id)
                ->meta.makeRaftCommandDelegate()
                .checkBeforeCommitMerge(request, source_region_meta_delegate);
            ASSERT_TRUE(false);
        }
        catch (Exception & e)
        {
            ASSERT_EQ(e.message(), "checkBeforeCommitMerge: unexpected state Applying of source 1");
        }
    }

    {
        /// Mock that source region is Normal but meta not exist, reject
        auto source_region = kvs.getRegion(source_region_id);
        auto && [request, response]
            = MockRaftStoreProxy::composeCommitMerge(source_region->cloneMetaRegion(), source_region->appliedIndex());
        source_region->setPeerState(raft_serverpb::PeerState::Normal);
        request.mutable_commit_merge()->mutable_source()->mutable_start_key()->clear();
        const auto & source_region_meta_delegate = source_region->meta.makeRaftCommandDelegate();
        try
        {
            kvs.getRegion(target_region_id)
                ->meta.makeRaftCommandDelegate()
                .checkBeforeCommitMerge(request, source_region_meta_delegate);
            ASSERT_TRUE(false);
        }
        catch (Exception & e)
        {
            ASSERT_EQ(e.message(), "checkBeforeCommitMerge: source region not match exist region meta");
        }
    }

    {
        /// Commit merge for merging region 7 -> region 1
        auto source_region = kvs.getRegion(source_region_id);
        auto && [request, response]
            = MockRaftStoreProxy::composeCommitMerge(source_region->cloneMetaRegion(), source_region->appliedIndex());

        // before commit merge
        {
            auto region_map
                = kvs.getRegionsByRangeOverlap(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
            ASSERT_TRUE(region_map.contains(target_region_id));
            ASSERT_EQ(region_map.size(), 2);
        }

        kvs.handleAdminRaftCmd(
            raft_cmdpb::AdminRequest(request),
            raft_cmdpb::AdminResponse(response),
            target_region_id,
            36,
            6,
            tmt);

        // checks after commit merge
        ASSERT_EQ(kvs.getRegion(source_region_id), nullptr);
        {
            auto mmp = kvs.getRegionsByRangeOverlap(RegionRangeKeys::makeComparableKeys(
                RecordKVFormat::genKey(table_id, 0),
                RecordKVFormat::genKey(table_id, 5)));
            ASSERT_TRUE(mmp.contains(target_region_id));
            ASSERT_EQ(mmp.size(), 1);
        }
        ASSERT_EQ(kvs.getRegion(target_region_id)->dataInfo(), "[lock 2 ]");

        // Add region 7 back and merge again
        {
            // add 7 back
            auto task_lock = kvs.genTaskLock();
            auto lock = kvs.genRegionMgrWriteLock(task_lock);
            auto region = makeRegion(
                source_region_id,
                RecordKVFormat::genKey(table_id, 0),
                RecordKVFormat::genKey(table_id, 5),
                kvs.getProxyHelper());
            lock.regions.emplace(source_region_id, region);
            lock.index.add(region);
        }
        {
            auto mmp = kvs.getRegionsByRangeOverlap(RegionRangeKeys::makeComparableKeys(
                RecordKVFormat::genKey(table_id, 0),
                RecordKVFormat::genKey(table_id, 5)));
            ASSERT_TRUE(mmp.contains(source_region_id));
            ASSERT_TRUE(mmp.contains(target_region_id));
            ASSERT_EQ(mmp.size(), 2);
        }
        kvs.handleAdminRaftCmd(
            raft_cmdpb::AdminRequest(request),
            raft_cmdpb::AdminResponse(response),
            target_region_id,
            36,
            6,
            tmt);
        {
            auto mmp = kvs.getRegionsByRangeOverlap(RegionRangeKeys::makeComparableKeys(
                RecordKVFormat::genKey(table_id, 0),
                RecordKVFormat::genKey(table_id, 5)));
            ASSERT_TRUE(mmp.contains(target_region_id));
            ASSERT_EQ(mmp.size(), 1);
        }
        ASSERT_EQ(kvs.getRegion(target_region_id)->dataInfo(), "[lock 2 ]");
    }
}

TEST_F(RegionKVStoreOldTest, RegionReadWrite)
{
    auto ctx = TiFlashTestEnv::getGlobalContext();
    TableID table_id = 100;
    KVStore & kvs = getKVS();
    UInt64 region_id = 1;
    proxy_instance->bootstrapWithRegion(
        kvs,
        ctx.getTMTContext(),
        region_id,
        std::make_optional(
            std::make_pair(RecordKVFormat::genKey(table_id, 0), RecordKVFormat::genKey(table_id, 1000))));
    auto region = kvs.getRegion(region_id);
    {
        // Test create RegionMeta.
        auto meta = RegionMeta(
            createPeer(2, true),
            createRegionInfo(666, RecordKVFormat::genKey(0, 0), RecordKVFormat::genKey(0, 1000)),
            initialApplyState());
        ASSERT_EQ(meta.peerId(), 2);
    }
    {
        // Test GenRegionReadIndexReq.
        ASSERT_TRUE(region->checkIndex(5));
        auto start_ts = 199;
        auto req = GenRegionReadIndexReq(*region, start_ts);
        ASSERT_EQ(req.ranges().size(), 1);
        ASSERT_EQ(req.start_ts(), start_ts);
        ASSERT_EQ(region->getMetaRegion().region_epoch().DebugString(), req.context().region_epoch().DebugString());
        ASSERT_EQ(region->getRange()->comparableKeys().first.key, req.ranges()[0].start_key());
        ASSERT_EQ(region->getRange()->comparableKeys().second.key, req.ranges()[0].end_key());
    }
    {
        // Test read committed and lock with CommittedScanner.
        region->insert(
            "lock",
            RecordKVFormat::genKey(table_id, 3),
            RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 3, 20));
        region->insert("default", RecordKVFormat::genKey(table_id, 3, 5), TiKVValue("value1"));
        region->insert(
            "write",
            RecordKVFormat::genKey(table_id, 3, 8),
            RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 5));
        ASSERT_EQ(1, region->writeCFCount());
        ASSERT_EQ(region->dataInfo(), "[write 1 lock 1 default 1 ]");
        {
            // There is a lock.
            auto iter = region->createCommittedScanner(true, true);
            auto lock = iter.getLockInfo({100, nullptr});
            ASSERT_NE(lock, nullptr);
            auto k = lock->intoLockInfo();
            ASSERT_EQ(k->lock_version(), 3);
        }
        {
            // The record is committed since there is a write record.
            std::optional<RegionDataReadInfoList> data_list_read = ReadRegionCommitCache(region, true);
            ASSERT_TRUE(data_list_read);
            ASSERT_EQ(1, data_list_read->size());
            RemoveRegionCommitCache(region, *data_list_read);
        }
        ASSERT_EQ(0, region->writeCFCount());
        {
            region->remove("lock", RecordKVFormat::genKey(table_id, 3));
            auto iter = region->createCommittedScanner(true, true);
            auto lock = iter.getLockInfo({100, nullptr});
            ASSERT_EQ(lock, nullptr);
        }
        region->clearAllData();
    }
    {
        // Test duplicate and tryCompactionFilter
        region->insert(
            "write",
            RecordKVFormat::genKey(table_id, 3, 8),
            RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 5));
        ASSERT_EQ(region->dataInfo(), "[write 1 ]");

        auto ori_size = region->dataSize();
        try
        {
            // insert duplicate records
            region->insert(
                "write",
                RecordKVFormat::genKey(table_id, 3, 8),
                RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 5));
            ASSERT_TRUE(false);
        }
        catch (Exception & e)
        {
            ASSERT_EQ(
                e.message(),
                "Found existing key in hex: 7480000000000000FF645F728000000000FF0000030000000000FAFFFFFFFFFFFFFFF7");
        }
        ASSERT_EQ(ori_size, region->dataSize());

        region->tryCompactionFilter(101);
        ASSERT_EQ(region->dataInfo(), "[]");
    }
    {
        // Test read and delete committed Del record.
        region->insert(
            "write",
            RecordKVFormat::genKey(table_id, 4, 8),
            RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::DelFlag, 5));
        ASSERT_EQ(1, region->writeCFCount());
        region->remove("write", RecordKVFormat::genKey(table_id, 4, 8));
        ASSERT_EQ(1, region->writeCFCount());
        {
            std::optional<RegionDataReadInfoList> data_list_read = ReadRegionCommitCache(region, true);
            ASSERT_TRUE(data_list_read);
            ASSERT_EQ(1, data_list_read->size());
            RemoveRegionCommitCache(region, *data_list_read);
        }
        ASSERT_EQ(0, region->writeCFCount());
    }
    {
        ASSERT_EQ(0, region->dataSize());
        region->insert("default", RecordKVFormat::genKey(table_id, 3, 5), TiKVValue("value1"));
        ASSERT_LT(0, region->dataSize());
        region->remove("default", RecordKVFormat::genKey(table_id, 3, 5));
        ASSERT_EQ(0, region->dataSize());
        // remove duplicate records
        region->remove("default", RecordKVFormat::genKey(table_id, 3, 5));
        ASSERT_EQ(0, region->dataSize());
    }
}

TEST_F(RegionKVStoreOldTest, Writes)
{
    createDefaultRegions();
    auto ctx = TiFlashTestEnv::getGlobalContext();

    KVStore & kvs = getKVS();
    {
        // Run without read-index workers
        kvs.initReadIndexWorkers([]() { return std::chrono::milliseconds(10); }, 0);
        ASSERT_EQ(kvs.read_index_worker_manager, nullptr);
        kvs.asyncRunReadIndexWorkers();
        kvs.stopReadIndexWorkers();
        kvs.releaseReadIndexWorkers();
    }
    {
        // Test set set id
        auto store = metapb::Store{};
        store.set_id(2345);
        kvs.setStore(store);
        ASSERT_EQ(kvs.getStoreID(), store.id());
    }
    {
        ASSERT_EQ(kvs.getRegion(0), nullptr);
        proxy_instance->debugAddRegions(
            kvs,
            ctx.getTMTContext(),
            {1, 2, 3},
            {{{RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 10)},
              {RecordKVFormat::genKey(1, 10), RecordKVFormat::genKey(1, 20)},
              {RecordKVFormat::genKey(1, 30), RecordKVFormat::genKey(1, 40)}}});
    }
    {
        // Test gc region persister
        tryPersistRegion(kvs, 1);
        kvs.gcPersistedRegion(Seconds{0});
    }
    {
        // Check region range
        ASSERT_EQ(kvs.regionSize(), 3);
        auto mmp = kvs.getRegionsByRangeOverlap(
            RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 15), TiKVKey("")));
        ASSERT_EQ(mmp.size(), 2);
        kvs.handleDestroy(3, ctx.getTMTContext());
        kvs.handleDestroy(3, ctx.getTMTContext());

        RegionMap mmp2 = kvs.getRegionsByRangeOverlap(
            RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 15), TiKVKey("")));
        ASSERT_EQ(mmp2.size(), 1);
        ASSERT_EQ(mmp2.at(2)->id(), 2);
    }
    {
        {
            raft_cmdpb::RaftCmdRequest request;
            {
                auto lock_key = RecordKVFormat::genKey(1, 2333);
                TiKVValue lock_value = RecordKVFormat::encodeLockCfValue(Region::DelFlag, "pk", 77, 0);
                RegionBench::setupPutRequest(request.add_requests(), ColumnFamilyName::Lock, lock_key, lock_value);
                auto write_key = RecordKVFormat::genKey(1, 2333, 1);
                TiKVValue write_value = RecordKVFormat::encodeWriteCfValue(Region::PutFlag, 2333);
                RegionBench::setupPutRequest(request.add_requests(), ColumnFamilyName::Write, write_key, write_value);
            }
            try
            {
                ASSERT_EQ(
                    RegionBench::applyWriteRaftCmd(kvs, std::move(request), 1, 6, 6, ctx.getTMTContext()),
                    EngineStoreApplyRes::None);
                ASSERT_TRUE(false);
            }
            catch (Exception & e)
            {
                ASSERT_EQ(
                    e.message(),
                    "Raw TiDB PK: 800000000000091D, Prewrite ts: 2333 can not found in default cf for key: "
                    "7480000000000000FF015F728000000000FF00091D0000000000FAFFFFFFFFFFFFFFFE, region_id: 1, "
                    "applied_index: 5: (applied_term: 5)");
                ASSERT_EQ(kvs.getRegion(1)->dataInfo(), "[write 1 lock 1 ]");
                kvs.getRegion(1)->tryCompactionFilter(1000);
            }
            try
            {
                raft_cmdpb::RaftCmdRequest request;
                {
                    auto key = RecordKVFormat::genKey(1, 2333, 1);
                    RegionBench::setupPutRequest(request.add_requests(), ColumnFamilyName::Default, key, "v1");
                }
                {
                    // duplicate
                    auto key = RecordKVFormat::genKey(1, 2333, 1);
                    RegionBench::setupPutRequest(request.add_requests(), ColumnFamilyName::Default, key, "v1");
                }
                ASSERT_EQ(
                    RegionBench::applyWriteRaftCmd(kvs, std::move(request), 1, 6, 6, ctx.getTMTContext()),
                    EngineStoreApplyRes::None);
                ASSERT_TRUE(false);
            }
            catch (Exception & e)
            {
                ASSERT_EQ(
                    e.message(),
                    "Found existing key in hex: "
                    "7480000000000000FF015F728000000000FF00091D0000000000FAFFFFFFFFFFFFFFFE");
            }
            ASSERT_EQ(kvs.getRegion(1)->dataInfo(), "[lock 1 default 1 ]");
            kvs.getRegion(1)->remove("default", RecordKVFormat::genKey(1, 2333, 1));
            try
            {
                raft_cmdpb::RaftCmdRequest request;
                {
                    RegionBench::setupPutRequest(
                        request.add_requests(),
                        ColumnFamilyName::Default,
                        std::string("k1"),
                        "v1");
                }
                ASSERT_EQ(
                    RegionBench::applyWriteRaftCmd(kvs, std::move(request), 1, 6, 6, ctx.getTMTContext()),
                    EngineStoreApplyRes::None);
                ASSERT_TRUE(false);
            }
            catch (Exception & e)
            {
                ASSERT_EQ(e.message(), "Unexpected eof");
            }
            try
            {
                raft_cmdpb::RaftCmdRequest request;
                request.add_requests()->set_cmd_type(::raft_cmdpb::CmdType::Invalid);
                ASSERT_EQ(
                    RegionBench::applyWriteRaftCmd(kvs, std::move(request), 1, 10, 6, ctx.getTMTContext()),
                    EngineStoreApplyRes::None);
                ASSERT_TRUE(false);
            }
            catch (Exception & e)
            {
                ASSERT_EQ(e.message(), "Unsupport raft cmd Invalid");
            }
        }
        ASSERT_EQ(kvs.getRegion(1)->dataInfo(), "[lock 1 ]");
        {
            raft_cmdpb::RaftCmdRequest request;
            {
                auto lock_key = RecordKVFormat::genKey(1, 2333);
                TiKVValue lock_value = RecordKVFormat::encodeLockCfValue(Region::DelFlag, "pk", 77, 0);
                RegionBench::setupDelRequest(request.add_requests(), ColumnFamilyName::Lock, lock_key);
            }
            raft_cmdpb::RaftCmdRequest first_request = request;
            ASSERT_EQ(
                RegionBench::applyWriteRaftCmd(kvs, std::move(first_request), 1, 7, 6, ctx.getTMTContext()),
                EngineStoreApplyRes::None);

            RegionBench::setupDelRequest(request.add_requests(), ColumnFamilyName::Write, TiKVKey("illegal key"));
            // index <= appliedIndex(), ignore
            raft_cmdpb::RaftCmdRequest second_request;
            ASSERT_EQ(
                RegionBench::applyWriteRaftCmd(kvs, std::move(second_request), 1, 7, 6, ctx.getTMTContext()),
                EngineStoreApplyRes::None);
            try
            {
                //
                request.clear_requests();
                RegionBench::setupDelRequest(request.add_requests(), ColumnFamilyName::Write, TiKVKey("illegal key"));
                ASSERT_EQ(
                    RegionBench::applyWriteRaftCmd(kvs, std::move(request), 1, 9, 6, ctx.getTMTContext()),
                    EngineStoreApplyRes::None);
                ASSERT_TRUE(false);
            }
            catch (Exception & e)
            {
                ASSERT_EQ(e.message(), "Key padding");
            }

            ASSERT_EQ(kvs.getRegion(1)->appliedIndex(), 7);
        }
        ASSERT_EQ(kvs.getRegion(1)->dataInfo(), "[]");

        ASSERT_EQ(
            RegionBench::applyWriteRaftCmd(kvs, raft_cmdpb::RaftCmdRequest{}, 8192, 7, 6, ctx.getTMTContext()),
            EngineStoreApplyRes::NotFound);
    }
    {
        kvs.handleDestroy(2, ctx.getTMTContext());
        ASSERT_EQ(kvs.regionSize(), 1);
    }
}


TEST_F(RegionKVStoreOldTest, AdminSplit)
{
    auto ctx = TiFlashTestEnv::getGlobalContext();
    KVStore & kvs = getKVS();
    proxy_instance->debugAddRegions(
        kvs,
        ctx.getTMTContext(),
        {1},
        {{RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 10)}});
    {
        testRaftSplit(kvs, ctx.getTMTContext(), proxy_instance);
        ASSERT_EQ(
            kvs.handleAdminRaftCmd(
                raft_cmdpb::AdminRequest{},
                raft_cmdpb::AdminResponse{},
                8192,
                5,
                6,
                ctx.getTMTContext()),
            EngineStoreApplyRes::NotFound);
    }
}

TEST_F(RegionKVStoreOldTest, AdminMergeRollback)
{
    auto ctx = TiFlashTestEnv::getGlobalContext();
    KVStore & kvs = getKVS();
    proxy_instance->debugAddRegions(
        kvs,
        ctx.getTMTContext(),
        {1, 7},
        {{RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 5)},
         {RecordKVFormat::genKey(1, 5), RecordKVFormat::genKey(1, 10)}});
    testRaftMergeRollback(kvs, ctx.getTMTContext());
}

TEST_F(RegionKVStoreOldTest, AdminMerge)
try
{
    auto ctx = TiFlashTestEnv::getGlobalContext();
    KVStore & kvs = getKVS();
    testRaftMerge(ctx, kvs, ctx.getTMTContext());
}
CATCH

TEST_F(RegionKVStoreOldTest, AdminChangePeer)
{
    UInt64 region_id = 88;
    auto ctx = TiFlashTestEnv::getGlobalContext();
    auto & kvs = getKVS();
    {
        proxy_instance->bootstrapWithRegion(
            kvs,
            ctx.getTMTContext(),
            region_id,
            std::make_optional(std::make_pair(RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 100))));
    }
    {
        auto meta = kvs.getRegion(region_id)->cloneMetaRegion();
        auto && [request, response] = MockRaftStoreProxy::composeChangePeer(std::move(meta), {2, 4}, false);
        kvs.handleAdminRaftCmd(std::move(request), std::move(response), region_id, 6, 5, ctx.getTMTContext());
        ASSERT_NE(kvs.getRegion(region_id), nullptr);
    }
    {
        auto meta = kvs.getRegion(region_id)->cloneMetaRegion();
        auto && [request, response] = MockRaftStoreProxy::composeChangePeer(std::move(meta), {3, 4});
        kvs.handleAdminRaftCmd(std::move(request), std::move(response), region_id, 7, 5, ctx.getTMTContext());
        ASSERT_EQ(kvs.getRegion(region_id), nullptr);
    }
}

// TODO Use test utils in new KVStore test for snapshot test.
// Otherwise data will not actually be inserted.
class ApplySnapshotTest
    : public RegionKVStoreOldTest
    , public testing::WithParamInterface<bool /* ingest_using_split */>
{
public:
    ApplySnapshotTest() { ingest_using_split = GetParam(); }

protected:
    bool ingest_using_split{};
};

INSTANTIATE_TEST_CASE_P(ByIngestUsingSplit, ApplySnapshotTest, testing::Bool());

TEST_P(ApplySnapshotTest, WithNewRegionRange)
try
{
    using DM::tests::DMTestEnv;

    createDefaultRegions();
    auto ctx = TiFlashTestEnv::getGlobalContext();
    auto & kvs = getKVS();
    auto table_id = 101;
    auto region_id = 19;
    auto region_id_str = std::to_string(region_id);
    ASSERT_NE(proxy_helper->sst_reader_interfaces.fn_key, nullptr);

    auto settings_backup = ctx.getGlobalContext().getSettings();
    ctx.getGlobalContext().getSettingsRef().dt_segment_limit_rows = 50;
    if (ingest_using_split)
    {
        ctx.getGlobalContext().getSettingsRef().dt_segment_delta_small_column_file_size = 50 * 8;
    }
    FailPointHelper::enableFailPoint(FailPoints::skip_check_segment_update);
    SCOPE_EXIT({
        FailPointHelper::disableFailPoint(FailPoints::skip_check_segment_update);
        ctx.getGlobalContext().setSettings(settings_backup);
    });

    StorageDeltaMergePtr storage;
    {
        auto columns = DMTestEnv::getDefaultTableColumns();
        auto table_info = DMTestEnv::getMinimalTableInfo(table_id);
        auto astptr = DMTestEnv::getPrimaryKeyExpr("test_table");
        storage = StorageDeltaMerge::create(
            "TiFlash",
            "default" /* db_name */,
            "test_table" /* table_name */,
            table_info,
            ColumnsDescription{columns},
            astptr,
            0,
            ctx);
        storage->startup();
    }
    SCOPE_EXIT({
        storage->drop();
        ctx.getTMTContext().getStorages().remove(NullspaceID, table_id);
    });
    // Initially region_19 range is [0, 10000)
    {
        auto region = makeRegion(
            region_id,
            RecordKVFormat::genKey(table_id, 0),
            RecordKVFormat::genKey(table_id, 10000),
            kvs.getProxyHelper());
        // Fill data from 20 to 100.
        GenMockSSTData(DMTestEnv::getMinimalTableInfo(table_id), table_id, region_id_str, 20, 100, 0);
        std::vector<SSTView> sst_views{
            SSTView{
                ColumnFamilyType::Write,
                BaseBuffView{region_id_str.data(), region_id_str.length()},
            },
            SSTView{
                ColumnFamilyType::Default,
                BaseBuffView{region_id_str.data(), region_id_str.length()},
            },
        };
        {
            RegionBench::handleApplySnapshot(
                kvs,
                region->cloneMetaRegion(),
                2,
                SSTViewVec{sst_views.data(), sst_views.size()},
                8,
                5,
                std::nullopt,
                ctx.getTMTContext());
            ASSERT_EQ(kvs.getRegion(region_id)->checkIndex(8), true);
        }
    }
    {
        if (ingest_using_split)
        {
            auto stats = storage->getStore()->getStoreStats();
            // Including 0..20, 20..100, 100..inf.
            ASSERT_EQ(3, stats.segment_count);
        }

        storage->mergeDelta(ctx);
    }
    // Later, its range is changed to [20000, 50000)
    {
        auto region = makeRegion(
            region_id,
            RecordKVFormat::genKey(table_id, 20000),
            RecordKVFormat::genKey(table_id, 50000),
            kvs.getProxyHelper());
        // Fill data from 20100 to 20200.
        GenMockSSTData(DMTestEnv::getMinimalTableInfo(table_id), table_id, region_id_str, 20100, 20200, 0);
        std::vector<SSTView> sst_views{
            SSTView{
                ColumnFamilyType::Write,
                BaseBuffView{region_id_str.data(), region_id_str.length()},
            },
            SSTView{
                ColumnFamilyType::Default,
                BaseBuffView{region_id_str.data(), region_id_str.length()},
            },
        };
        {
            RegionBench::handleApplySnapshot(
                kvs,
                region->cloneMetaRegion(),
                2,
                SSTViewVec{sst_views.data(), sst_views.size()},
                20,
                5,
                std::nullopt,
                ctx.getTMTContext());
            auto region_after_snapshot = kvs.getRegion(region_id);
            ASSERT_EQ(region_after_snapshot->appliedIndex(), 20);
            ASSERT_EQ(region_after_snapshot->appliedIndexTerm(), 5);
            ASSERT_EQ(region_after_snapshot->checkIndex(20), true);
        }
    }
    {
        auto stats = storage->getStore()->getStoreStats();
        ASSERT_NE(0, stats.total_stable_size_on_disk);
        ASSERT_NE(0, stats.total_rows);
        ASSERT_NE(0, stats.total_size);
    }
    // Finally, the region is migrated out
    {
        auto meta = kvs.getRegion(region_id)->cloneMetaRegion();
        auto && [request, response] = MockRaftStoreProxy::composeChangePeer(std::move(meta), {3});
        kvs.handleAdminRaftCmd(std::move(request), std::move(response), region_id, 21, 6, ctx.getTMTContext());
        ASSERT_EQ(kvs.getRegion(region_id), nullptr);
    }
    {
        // After several rounds of GC, everything should be reclaimed.
        for (size_t i = 0; i < 10; ++i)
        {
            storage->onSyncGc(100, DM::GCOptions::newAllForTest());
        }

        auto gc_n = storage->onSyncGc(100, DM::GCOptions::newAllForTest());
        ASSERT_EQ(0, gc_n);

        auto stats = storage->getStore()->getStoreStats();
        // The data of [20, 100), is not reclaimed during Apply Snapshot.
        if (ingest_using_split)
        {
            ASSERT_EQ(3, stats.segment_count);
            ASSERT_NE(0, stats.total_stable_size_on_disk);
            ASSERT_EQ(80, stats.total_rows);
            ASSERT_NE(0, stats.total_size);
        }
        else
        {
            // The only segment is not reclaimed.
            ASSERT_EQ(1, stats.segment_count);
            ASSERT_NE(0, stats.total_stable_size_on_disk);
            ASSERT_EQ(180, stats.total_rows);
            ASSERT_NE(0, stats.total_size);
        }
    }
}
CATCH

TEST_F(RegionKVStoreOldTest, ApplySnapshot)
try
{
    createDefaultRegions();
    auto ctx = TiFlashTestEnv::getGlobalContext();
    KVStore & kvs = getKVS();

    // In this test we only deal with meta though,
    ASSERT_NE(proxy_helper->sst_reader_interfaces.fn_key, nullptr);

    TableID table_id = 1;
    auto region_id = 19;
    auto region = makeRegion(
        region_id,
        RecordKVFormat::genKey(table_id, 50),
        RecordKVFormat::genKey(table_id, 60),
        kvs.getProxyHelper());

    {
        // Prepare a region with some kvs
        auto region_id_str = std::to_string(region_id);
        auto & mmp = MockSSTReader::getMockSSTData();
        MockSSTReader::getMockSSTData().clear();
        MockSSTReader::Data default_kv_list;
        {
            default_kv_list.emplace_back(
                RecordKVFormat::genKey(table_id, 55, 5).getStr(),
                TiKVValue("value1").getStr());
            default_kv_list.emplace_back(
                RecordKVFormat::genKey(table_id, 58, 5).getStr(),
                TiKVValue("value2").getStr());
        }
        mmp[MockSSTReader::Key{region_id_str, ColumnFamilyType::Default}] = std::move(default_kv_list);
        std::vector<SSTView> sst_views;
        sst_views.push_back(SSTView{
            ColumnFamilyType::Default,
            BaseBuffView{region_id_str.data(), region_id_str.length()},
        });
        // (DTFiles is ignored because the table is not created)
        RegionBench::handleApplySnapshot(
            kvs,
            region->cloneMetaRegion(),
            2,
            SSTViewVec{sst_views.data(), sst_views.size()},
            8,
            5,
            std::nullopt,
            ctx.getTMTContext());
        ASSERT_EQ(kvs.getRegion(region_id)->checkIndex(8), true);
    }

    {
        // Will reject a snapshot with smaller index.
        try
        {
            RegionBench::handleApplySnapshot(
                kvs,
                region->cloneMetaRegion(),
                2,
                {}, // empty snap files
                6, // smaller index
                5,
                std::nullopt,
                ctx.getTMTContext());
            ASSERT_TRUE(false);
        }
        catch (Exception & e)
        {
            ASSERT_EQ(
                e.message(),
                fmt::format(
                    "try to apply with older index, region_id={} applied_index={} new_index={}: (while "
                    "applyPreHandledSnapshot region_id={} keyspace_id=4294967295 table_id={})",
                    region_id,
                    8,
                    6,
                    region_id,
                    table_id));
        }
    }

    {
        // With larger index, accept and replace the existing region
        RegionBench::handleApplySnapshot(
            kvs,
            region->cloneMetaRegion(),
            2,
            {}, // empty snap files
            1024, // larger index
            5,
            std::nullopt,
            ctx.getTMTContext());
        ASSERT_EQ(kvs.getRegion(region_id)->checkIndex(1024), true);
    }
}
CATCH

TEST_F(RegionKVStoreOldTest, ApplySnapshotOverlap)
try
{
    createDefaultRegions();
    auto ctx = TiFlashTestEnv::getGlobalContext();
    KVStore & kvs = getKVS();

    const TableID table_id = 55;
    {
        // Snapshot will be rejected if region overlaps with existing Region.
        {
            // create an empty region 22, range=[50,100)
            auto region = makeRegion(
                22,
                RecordKVFormat::genKey(table_id, 50),
                RecordKVFormat::genKey(table_id, 100),
                kvs.getProxyHelper());
            auto prehandle_result = kvs.preHandleSnapshotToFiles(region, {}, 9, 5, std::nullopt, ctx.getTMTContext());
            kvs.checkAndApplyPreHandledSnapshot<RegionPtrWithSnapshotFiles>(
                RegionPtrWithSnapshotFiles{region, std::move(prehandle_result.ingest_ids)},
                ctx.getTMTContext());
            auto region_applied_22 = kvs.getRegion(22);
            ASSERT_NE(region_applied_22, nullptr);
            ASSERT_EQ(region->appliedIndex(), region_applied_22->appliedIndex());
            ASSERT_EQ(region->appliedIndexTerm(), region_applied_22->appliedIndexTerm());
        }
        try
        {
            // try apply snapshot to region 20, range=[50, 100) that is overlapped with region 22, should be rejected
            auto region = makeRegion(
                20,
                RecordKVFormat::genKey(table_id, 50),
                RecordKVFormat::genKey(table_id, 100),
                kvs.getProxyHelper());
            auto prehandle_result = kvs.preHandleSnapshotToFiles(region, {}, 9, 5, std::nullopt, ctx.getTMTContext());
            kvs.checkAndApplyPreHandledSnapshot<RegionPtrWithSnapshotFiles>(
                RegionPtrWithSnapshotFiles{region, std::move(prehandle_result.ingest_ids)},
                ctx.getTMTContext()); // overlap, but not tombstone
            ASSERT_TRUE(false);
        }
        catch (Exception & e)
        {
            ASSERT_TRUE(e.message().rfind("range of region_id=20 is overlapped with region_id=22", 0) == 0);
        }
    }

    {
        // Applying snapshot meets overlapped region, will throw if proxy is not inited.
        const auto * ori_ptr = proxy_helper->proxy_ptr.inner;
        SCOPE_EXIT({ proxy_helper->proxy_ptr.inner = ori_ptr; });

        try
        {
            auto region = makeRegion(
                20,
                RecordKVFormat::genKey(table_id, 50),
                RecordKVFormat::genKey(table_id, 100),
                kvs.getProxyHelper());
            // preHandleSnapshotToFiles will assert proxy_ptr is not null.
            auto prehandle_result = kvs.preHandleSnapshotToFiles(region, {}, 10, 5, std::nullopt, ctx.getTMTContext());
            proxy_helper->proxy_ptr.inner = nullptr;
            kvs.checkAndApplyPreHandledSnapshot<RegionPtrWithSnapshotFiles>(
                RegionPtrWithSnapshotFiles{region, std::move(prehandle_result.ingest_ids)},
                ctx.getTMTContext());
            ASSERT_TRUE(false);
        }
        catch (Exception & e)
        {
            ASSERT_EQ(e.message(), "getRegionLocalState meet internal error: RaftStoreProxyPtr is none");
        }
    }

    {
        // A snapshot can be applied when its overlapped region is already Tombstone.
        proxy_instance->getRegion(22)->setState(({
            raft_serverpb::RegionLocalState s;
            s.set_state(::raft_serverpb::PeerState::Tombstone);
            s;
        }));
        auto region = makeRegion(
            20,
            RecordKVFormat::genKey(table_id, 50),
            RecordKVFormat::genKey(table_id, 100),
            kvs.getProxyHelper());
        auto prehandle_result = kvs.preHandleSnapshotToFiles(region, {}, 10, 5, std::nullopt, ctx.getTMTContext());
        kvs.checkAndApplyPreHandledSnapshot<RegionPtrWithSnapshotFiles>(
            RegionPtrWithSnapshotFiles{region, std::move(prehandle_result.ingest_ids)},
            ctx.getTMTContext()); // overlap, tombstone, remove previous one

        auto state = proxy_helper->getRegionLocalState(22);
        ASSERT_EQ(state.state(), raft_serverpb::PeerState::Tombstone);
    }

    // Clear the region 20
    kvs.handleDestroy(20, ctx.getTMTContext());
}
CATCH

TEST_F(RegionKVStoreOldTest, IngestSST)
try
{
    createDefaultRegions();
    auto ctx = TiFlashTestEnv::getGlobalContext();
    KVStore & kvs = getKVS();

    {
        /// Ingest SST will be ignored if the region is not created
        auto new_region_id = 109;
        auto new_region_id_str = std::to_string(new_region_id);
        std::vector<SSTView> sst_views;
        kvs.handleIngestSST(new_region_id, SSTViewVec{sst_views.data(), sst_views.size()}, 100, 1, ctx.getTMTContext());
        auto region = kvs.getRegion(new_region_id);
        ASSERT_EQ(region, nullptr);
    }

    auto region_id = 19;
    auto region_id_str = std::to_string(region_id);
    // Prepare a region with some kvs
    {
        auto region
            = makeRegion(region_id, RecordKVFormat::genKey(1, 50), RecordKVFormat::genKey(1, 60), kvs.getProxyHelper());
        auto & mmp = MockSSTReader::getMockSSTData();
        MockSSTReader::getMockSSTData().clear();
        MockSSTReader::Data default_kv_list;
        {
            default_kv_list.emplace_back(RecordKVFormat::genKey(1, 55, 5).getStr(), TiKVValue("value1").getStr());
            default_kv_list.emplace_back(RecordKVFormat::genKey(1, 56, 5).getStr(), TiKVValue("value2").getStr());
        }
        mmp[MockSSTReader::Key{region_id_str, ColumnFamilyType::Default}] = std::move(default_kv_list);
        std::vector<SSTView> sst_views;
        sst_views.push_back(SSTView{
            ColumnFamilyType::Default,
            BaseBuffView{region_id_str.data(), region_id_str.length()},
        });

        RegionBench::handleApplySnapshot(
            kvs,
            region->cloneMetaRegion(),
            2,
            SSTViewVec{sst_views.data(), sst_views.size()},
            8,
            5,
            std::nullopt,
            ctx.getTMTContext());
        ASSERT_EQ(kvs.getRegion(region_id)->checkIndex(8), true);
    }

    {
        /// Ingest SST to the region
        // Mock ingest a SST data for column family "Default"
        auto & mmp = MockSSTReader::getMockSSTData();
        MockSSTReader::getMockSSTData().clear();
        MockSSTReader::Data default_kv_list;
        {
            default_kv_list.emplace_back(RecordKVFormat::genKey(1, 57, 5).getStr(), TiKVValue("value1").getStr());
            default_kv_list.emplace_back(RecordKVFormat::genKey(1, 58, 5).getStr(), TiKVValue("value2").getStr());
        }
        mmp[MockSSTReader::Key{region_id_str, ColumnFamilyType::Default}] = std::move(default_kv_list);

        {
            std::vector<SSTView> sst_views;
            sst_views.push_back(SSTView{
                ColumnFamilyType::Default,
                BaseBuffView{region_id_str.data(), region_id_str.length()},
            });
            kvs.handleIngestSST(region_id, SSTViewVec{sst_views.data(), sst_views.size()}, 100, 1, ctx.getTMTContext());
            auto region = kvs.getRegion(region_id);
            ASSERT_NE(region, nullptr);
            ASSERT_EQ(region->checkIndex(100), true);
        }
    }
}
CATCH

TEST_F(RegionKVStoreOldTest, Restore)
{
    auto ctx = TiFlashTestEnv::getGlobalContext();
    {
        KVStore & kvs = getKVS();
        {
            ASSERT_EQ(kvs.getRegion(0), nullptr);
            proxy_instance->debugAddRegions(
                kvs,
                ctx.getTMTContext(),
                {1, 2, 3},
                {{{RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 10)},
                  {RecordKVFormat::genKey(1, 10), RecordKVFormat::genKey(1, 20)},
                  {RecordKVFormat::genKey(1, 30), RecordKVFormat::genKey(1, 40)}}});
        }
        tryPersistRegion(kvs, 1);
        tryPersistRegion(kvs, 2);
        tryPersistRegion(kvs, 3);
    }
    {
        KVStore & kvs = reloadKVSFromDisk();
        kvs.getRegion(1);
        kvs.getRegion(2);
        kvs.getRegion(3);
    }
}

TEST_F(RegionKVStoreOldTest, RegionRange)
{
    {
        // Test util functions.
        RegionsRangeIndex region_index;
        region_index.split(TiKVRangeKey::makeTiKVRangeKey<true>(TiKVKey()));
        region_index.split(TiKVRangeKey::makeTiKVRangeKey<false>(TiKVKey()));
        region_index.tryMergeEmpty();
        const auto & root_map = region_index.getRoot();
        ASSERT_EQ(root_map.size(), 2);
    }
    {
        // Test findByRangeOverlap.
        RegionsRangeIndex region_index;
        const auto & root_map = region_index.getRoot();
        ASSERT_EQ(root_map.size(), 2); // start and end all equals empty

        region_index.add(makeRegion(1, RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 10)));

        ASSERT_EQ(root_map.begin()->second.region_map.size(), 0);

        region_index.add(makeRegion(2, RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 3)));
        region_index.add(makeRegion(3, RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 1)));

        auto res = region_index.findByRangeOverlap(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
        ASSERT_EQ(res.size(), 3);
        auto res2 = region_index.findByRangeChecked(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
        ASSERT_TRUE(std::holds_alternative<RegionsRangeIndex::OverlapInfo>(res2));

        region_index.add(makeRegion(4, RecordKVFormat::genKey(1, 1), RecordKVFormat::genKey(1, 4)));

        // -inf,0,1,3,4,10,inf
        ASSERT_EQ(root_map.size(), 7);

        // 1,2,3,4
        res = region_index.findByRangeOverlap(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
        ASSERT_EQ(res.size(), 4);

        res = region_index.findByRangeOverlap(
            RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 1), TiKVKey("")));
        ASSERT_EQ(res.size(), 3);
        ASSERT_TRUE(res.find(1) != res.end());
        ASSERT_TRUE(res.find(2) != res.end());
        ASSERT_TRUE(res.find(4) != res.end());

        res = region_index.findByRangeOverlap(
            RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 2), RecordKVFormat::genKey(1, 5)));
        ASSERT_EQ(res.size(), 3);
        ASSERT_TRUE(res.find(1) != res.end());
        ASSERT_TRUE(res.find(2) != res.end());
        ASSERT_TRUE(res.find(4) != res.end());

        region_index.remove(
            RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 1), RecordKVFormat::genKey(1, 4)),
            4);
        res = region_index.findByRangeOverlap(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
        ASSERT_EQ(res.size(), 3);

        region_index.remove(
            RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 1)),
            3);
        res = region_index.findByRangeOverlap(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
        ASSERT_EQ(res.size(), 2);

        region_index.remove(
            RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 3)),
            2);
        res = region_index.findByRangeOverlap(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
        ASSERT_EQ(res.size(), 1);

        region_index.remove(
            RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 10)),
            1);
        res = region_index.findByRangeOverlap(RegionRangeKeys::makeComparableKeys(TiKVKey(""), TiKVKey("")));
        ASSERT_TRUE(res.empty());

        ASSERT_EQ(root_map.size(), 2);
    }

    {
        // Test add and remove.
        RegionsRangeIndex region_index;
        const auto & root_map = region_index.getRoot();
        try
        {
            region_index.remove(RegionRangeKeys::makeComparableKeys(TiKVKey(), TiKVKey()), 1);
            assert(false);
        }
        catch (Exception & e)
        {
            const auto & res = e.message();
            std::regex msg_reg(".*: not found region_id=1");
            ASSERT_TRUE(std::regex_match(res, msg_reg));
        }

        region_index.add(makeRegion(2, RecordKVFormat::genKey(1, 3), RecordKVFormat::genKey(1, 5)));
        try
        {
            region_index.remove(
                RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 4), RecordKVFormat::genKey(1, 5)),
                2);
            assert(false);
        }
        catch (Exception & e)
        {
            const auto & res = e.message();
            std::regex msg_reg(".*: not found start key");
            ASSERT_TRUE(std::regex_match(res, msg_reg));
        }

        try
        {
            region_index.remove(
                RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 3), RecordKVFormat::genKey(1, 4)),
                2);
            assert(false);
        }
        catch (Exception & e)
        {
            const auto & res = e.message();
            std::regex msg_reg(".*: not found end key");
            ASSERT_TRUE(std::regex_match(res, msg_reg));
        }

        try
        {
            region_index.remove(
                RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 3), RecordKVFormat::genKey(1, 3)),
                2);
            assert(false);
        }
        catch (Exception & e)
        {
            const auto & res = e.message();
            std::regex msg_reg(".*: range of region_id=2 is empty");
            ASSERT_TRUE(std::regex_match(res, msg_reg));
        }

        try
        {
            region_index.remove(RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 3), TiKVKey()), 2);
            assert(false);
        }
        catch (Exception & e)
        {
            const auto & res = e.message();
            std::regex msg_reg(".*: not found region_id=2");
            ASSERT_TRUE(std::regex_match(res, msg_reg));
        }

        region_index.clear();

        try
        {
            region_index.add(makeRegion(6, RecordKVFormat::genKey(6, 6), RecordKVFormat::genKey(6, 6)));
            assert(false);
        }
        catch (Exception & e)
        {
            const auto & res = e.message();
            std::string tar = "Illegal region range, should not happen";
            ASSERT(res.size() > tar.size());
            ASSERT_EQ(res.substr(0, tar.size()), tar);
        }

        region_index.clear();

        region_index.add(makeRegion(1, RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 1)));
        region_index.add(makeRegion(2, RecordKVFormat::genKey(1, 1), RecordKVFormat::genKey(1, 2)));
        region_index.add(makeRegion(3, RecordKVFormat::genKey(1, 2), RecordKVFormat::genKey(1, 3)));

        ASSERT_EQ(root_map.size(), 6);
        region_index.remove(
            RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 2), RecordKVFormat::genKey(1, 3)),
            3);
        ASSERT_EQ(root_map.size(), 5);

        region_index.remove(
            RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 1)),
            1);
        ASSERT_EQ(root_map.size(), 4);

        region_index.remove(
            RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(1, 1), RecordKVFormat::genKey(1, 2)),
            2);
        ASSERT_EQ(root_map.size(), 2);
    }
    // Test region range with merge.
    {
        {
            // Compute `source_at_left` by region range.
            ASSERT_EQ(
                MetaRaftCommandDelegate::computeRegionMergeResult(
                    createRegionInfo(1, "x", ""),
                    createRegionInfo(1000, "", "x"))
                    .source_at_left,
                false);
            ASSERT_EQ(
                MetaRaftCommandDelegate::computeRegionMergeResult(
                    createRegionInfo(1, "", "x"),
                    createRegionInfo(1000, "x", ""))
                    .source_at_left,
                true);
            ASSERT_EQ(
                MetaRaftCommandDelegate::computeRegionMergeResult(
                    createRegionInfo(1, "x", "y"),
                    createRegionInfo(1000, "y", "z"))
                    .source_at_left,
                true);
            ASSERT_EQ(
                MetaRaftCommandDelegate::computeRegionMergeResult(
                    createRegionInfo(1, "y", "z"),
                    createRegionInfo(1000, "x", "y"))
                    .source_at_left,
                false);
        }
        {
            RegionState region_state;
            bool source_at_left;
            RegionState source_region_state;

            region_state.setStartKey(RecordKVFormat::genKey(1, 0));
            region_state.setEndKey(RecordKVFormat::genKey(1, 10));

            source_region_state.setStartKey(RecordKVFormat::genKey(1, 10));
            source_region_state.setEndKey(RecordKVFormat::genKey(1, 20));

            source_at_left = false;

            ChangeRegionStateRange(region_state, source_at_left, source_region_state);

            ASSERT_EQ(region_state.getRange()->comparableKeys().first.key, RecordKVFormat::genKey(1, 0));
            ASSERT_EQ(region_state.getRange()->comparableKeys().second.key, RecordKVFormat::genKey(1, 20));
        }
        {
            RegionState region_state;
            bool source_at_left;
            RegionState source_region_state;

            region_state.setStartKey(RecordKVFormat::genKey(2, 5));
            region_state.setEndKey(RecordKVFormat::genKey(2, 10));

            source_region_state.setStartKey(RecordKVFormat::genKey(2, 0));
            source_region_state.setEndKey(RecordKVFormat::genKey(2, 5));

            source_at_left = true;

            ChangeRegionStateRange(region_state, source_at_left, source_region_state);

            ASSERT_EQ(region_state.getRange()->comparableKeys().first.key, RecordKVFormat::genKey(2, 0));
            ASSERT_EQ(region_state.getRange()->comparableKeys().second.key, RecordKVFormat::genKey(2, 10));
        }
    }
}

TEST_F(RegionKVStoreOldTest, RegionRange2)
{
    auto mustOverlap = [](std::string s1, std::string e1, std::string s2, std::string e2) {
        auto r1 = RegionRangeKeys::makeComparableKeys(TiKVKey::copyFrom(s1), TiKVKey::copyFrom(e1));
        auto r2 = RegionRangeKeys::makeComparableKeys(TiKVKey::copyFrom(s2), TiKVKey::copyFrom(e2));
        ASSERT_TRUE(RegionRangeKeys::isRangeOverlapped(r1, r2));
        ASSERT_TRUE(RegionRangeKeys::isRangeOverlapped(r2, r1));
    };
    auto mustNotOverlap = [](std::string s1, std::string e1, std::string s2, std::string e2) {
        auto r1 = RegionRangeKeys::makeComparableKeys(TiKVKey::copyFrom(s1), TiKVKey::copyFrom(e1));
        auto r2 = RegionRangeKeys::makeComparableKeys(TiKVKey::copyFrom(s2), TiKVKey::copyFrom(e2));
        ASSERT_FALSE(RegionRangeKeys::isRangeOverlapped(r1, r2));
        ASSERT_FALSE(RegionRangeKeys::isRangeOverlapped(r2, r1));
    };
    mustOverlap("", "a", "", "b");
    mustOverlap("a", "", "b", "");
    mustOverlap("", "", "b", "");
    mustOverlap("", "", "", "a");
    mustOverlap("", "", "a", "b");
    mustOverlap("", "", "", "");

    mustOverlap("a", "e", "", "f");
    mustOverlap("a", "e", "", "");
    mustOverlap("b", "e", "a", "");

    mustOverlap("a", "e", "a", "e");
    mustOverlap("a", "f", "c", "d");
    mustOverlap("a", "f", "c", "f");

    mustNotOverlap("a", "e", "e", "f");
    mustNotOverlap("a", "e", "f", "g");
    mustNotOverlap("", "e", "e", "f");
    mustNotOverlap("a", "e", "e", "");
    mustNotOverlap("", "e", "f", "g");
    mustNotOverlap("a", "e", "f", "");
    mustNotOverlap("", "e", "f", "");
    mustNotOverlap("", "e", "e", "");
}

} // namespace DB::tests
