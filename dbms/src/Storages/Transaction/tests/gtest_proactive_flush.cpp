// Copyright 2023 PingCAP, Ltd.
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

#include "kvstore_helper.h"


namespace DB
{
namespace tests
{


TEST_F(RegionKVStoreTest, KVStorePassivePersistence)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    // CompactLog and passive persistence
    {
        KVStore & kvs = getKVS();
        UInt64 region_id = 1;
        {
            auto applied_index = 0;
            proxy_instance->bootstrapWithRegion(kvs, ctx.getTMTContext(), region_id, std::nullopt);
            MockRaftStoreProxy::FailCond cond;

            auto kvr1 = kvs.getRegion(region_id);
            auto r1 = proxy_instance->getRegion(region_id);
            ASSERT_NE(r1, nullptr);
            ASSERT_NE(kvr1, nullptr);
            applied_index = r1->getLatestAppliedIndex();
            ASSERT_EQ(r1->getLatestAppliedIndex(), kvr1->appliedIndex());
            auto [index, term] = proxy_instance->normalWrite(region_id, {33}, {"v1"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index + 1);
            ASSERT_EQ(kvr1->appliedIndex(), applied_index + 1);

            kvr1->markCompactLog();
            kvs.setRegionCompactLogConfig(0, 0, 0, 0);
            auto && [request, response] = MockRaftStoreProxy::composeCompactLog(r1, index);
            auto && [index2, term2] = proxy_instance->adminCommand(region_id, std::move(request), std::move(response));
            // In tryFlushRegionData we will call handleWriteRaftCmd, which will already cause an advance.
            // Notice kvs is not tmt->getKVStore(), so we can't use the ProxyFFI version.
            ASSERT_TRUE(kvs.tryFlushRegionData(region_id, false, true, ctx.getTMTContext(), index2, term, 0, 0));
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index2);
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index + 2);
            ASSERT_EQ(kvr1->appliedIndex(), applied_index + 2);
        }
        {
            proxy_instance->normalWrite(region_id, {34}, {"v2"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            // There shall be data to flush.
            ASSERT_EQ(kvs.needFlushRegionData(region_id, ctx.getTMTContext()), true);
            // If flush fails, and we don't insist a success.
            FailPointHelper::enableFailPoint(FailPoints::force_fail_in_flush_region_data);
            ASSERT_EQ(kvs.tryFlushRegionData(region_id, false, false, ctx.getTMTContext(), 0, 0, 0, 0), false);
            FailPointHelper::disableFailPoint(FailPoints::force_fail_in_flush_region_data);
            // Force flush until succeed only for testing.
            ASSERT_EQ(kvs.tryFlushRegionData(region_id, false, true, ctx.getTMTContext(), 0, 0, 0, 0), true);
            // Non existing region.
            // Flush and CompactLog will not panic.
            ASSERT_EQ(kvs.tryFlushRegionData(1999, false, true, ctx.getTMTContext(), 0, 0, 0, 0), true);
            raft_cmdpb::AdminRequest request;
            raft_cmdpb::AdminResponse response;
            request.mutable_compact_log();
            request.set_cmd_type(::raft_cmdpb::AdminCmdType::CompactLog);
            ASSERT_EQ(kvs.handleAdminRaftCmd(raft_cmdpb::AdminRequest{request}, std::move(response), 1999, 22, 6, ctx.getTMTContext()), EngineStoreApplyRes::NotFound);
        }
    }
}
CATCH

std::tuple<uint64_t, uint64_t, uint64_t> RegionKVStoreTest::prepareForProactiveFlushTest()
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    // Allow enough large segment size.
    ctx.getSettingsRef().dt_segment_limit_rows = 1000000;
    ctx.getSettingsRef().dt_segment_limit_size = 1000000;
    ctx.getSettingsRef().dt_segment_delta_cache_limit_rows = 0;
    ctx.getSettingsRef().dt_segment_delta_cache_limit_size = 0;
    UInt64 region_id = 1;
    UInt64 region_id2 = 7;
    TableID table_id;
    KVStore & kvs = getKVS();
    ctx.getTMTContext().debugSetKVStore(kvstore);
    MockRaftStoreProxy::FailCond cond;
    {
        initStorages();
        table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
        HandleID end_index = 100;
        HandleID mid_index = 50;
        auto start = RecordKVFormat::genKey(table_id, 0);
        auto end = RecordKVFormat::genKey(table_id, end_index);
        proxy_instance->bootstrapWithRegion(kvs, ctx.getTMTContext(), region_id, std::make_pair(start.toString(), end.toString()));

        auto source_region = kvs.getRegion(region_id);
        auto old_epoch = source_region->mutMeta().getMetaRegion().region_epoch();
        auto && [request, response] = MockRaftStoreProxy::composeBatchSplit(
            {region_id, region_id2},
            {{RecordKVFormat::genKey(table_id, mid_index), RecordKVFormat::genKey(table_id, end_index)},
             {RecordKVFormat::genKey(table_id, 0), RecordKVFormat::genKey(table_id, mid_index)}},
            old_epoch);
        auto && [index2, term2] = proxy_instance->adminCommand(region_id, std::move(request), std::move(response));
        proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index2);

        auto kvr1 = kvs.getRegion(region_id);
        auto kvr2 = kvs.getRegion(region_id2);
        ctx.getTMTContext().getRegionTable().updateRegion(*kvr1);
        ctx.getTMTContext().getRegionTable().updateRegion(*kvr2);
    }
    return std::make_tuple(table_id, region_id, region_id2);
}

TEST_F(RegionKVStoreTest, ProactiveFlushConsistency)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    auto tp = prepareForProactiveFlushTest();
    // auto table_id = std::get<0>(tp);
    auto region_id = std::get<1>(tp);
    // auto region_id2 = std::get<2>(tp);
    MockRaftStoreProxy::FailCond cond;
    KVStore & kvs = getKVS();

    std::shared_ptr<std::atomic<size_t>> ai = std::make_shared<std::atomic<size_t>>();
    DB::FailPointHelper::enableFailPoint(DB::FailPoints::proactive_flush_force_set_type, ai);
    ai->store(0b0000);

    {
        // Newer passive and older proactive.
        auto kvr1 = kvs.getRegion(region_id);
        auto r1 = proxy_instance->getRegion(region_id);
        uint64_t compact_index = 10;
        auto && [request, response] = MockRaftStoreProxy::composeCompactLog(r1, compact_index);
        auto && [index1, term] = proxy_instance->adminCommand(region_id, std::move(request), std::move(response), 11);
        kvs.setRegionCompactLogConfig(0, 0, 0, 500);
        proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index1);
        UNUSED(term);
        kvs.notifyCompactLog(region_id, 1, 5, false, false);
        ASSERT_EQ(r1->getApply().truncated_state().index(), compact_index);
    }

    DB::FailPointHelper::disableFailPoint(DB::FailPoints::proactive_flush_force_set_type);
}
CATCH

TEST_F(RegionKVStoreTest, ProactiveFlushLiveness)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    auto tp = prepareForProactiveFlushTest();
    auto table_id = std::get<0>(tp);
    auto region_id = std::get<1>(tp);
    auto region_id2 = std::get<2>(tp);
    MockRaftStoreProxy::FailCond cond;
    KVStore & kvs = getKVS();

    std::shared_ptr<std::atomic<size_t>> ai = std::make_shared<std::atomic<size_t>>();
    DB::FailPointHelper::enableFailPoint(DB::FailPoints::proactive_flush_force_set_type, ai);
    {
        // A fg flush and a bg flush will not deadlock.
        DB::FailPointHelper::enableFailPoint(DB::FailPoints::pause_proactive_flush_before_persist_region);
        DB::FailPointHelper::enableFailPoint(DB::FailPoints::pause_passive_flush_before_persist_region);
        ai->store(0b1011);
        auto f1 = [&]() {
            auto && [value_write, value_default] = proxy_instance->generateTiKVKeyValue(111, 999);
            auto k1 = RecordKVFormat::genKey(table_id, 60, 111);
            // Trigger a fg flush on region_id
            auto [index, term] = proxy_instance->rawWrite(region_id, {k1}, {value_default}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            auto [index2, term2] = proxy_instance->rawWrite(region_id, {k1}, {value_write}, {WriteCmdType::Put}, {ColumnFamilyType::Write});
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index2, std::make_optional(true));
        };
        std::thread t1(f1);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        ai->store(0b1110);
        // Force bg flush.
        auto f2 = [&]() {
            auto && [value_write, value_default] = proxy_instance->generateTiKVKeyValue(111, 999);
            auto k1 = RecordKVFormat::genKey(table_id, 5, 111);
            // Trigger a fg flush on region_id2
            auto [index, term] = proxy_instance->rawWrite(region_id2, {k1}, {value_default}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            auto [index2, term2] = proxy_instance->rawWrite(region_id2, {k1}, {value_write}, {WriteCmdType::Put}, {ColumnFamilyType::Write});
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id2, index);
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id2, index2, std::make_optional(false));
        };
        std::thread t2(f2);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        DB::FailPointHelper::disableFailPoint(DB::FailPoints::pause_proactive_flush_before_persist_region);
        DB::FailPointHelper::disableFailPoint(DB::FailPoints::pause_passive_flush_before_persist_region);
        t1.join();
        t2.join();
        ASSERT_EQ(proxy_instance->getRegion(region_id)->getApply().truncated_state().index(), proxy_instance->getRegion(region_id)->getLatestCommitIndex());
        // We can't assert for region_id2, since bg flush may be be finished.
    }
    kvs.setRegionCompactLogConfig(0, 0, 0, 500); // Every notify will take effect.
    {
        // Two fg flush will not deadlock.
        DB::FailPointHelper::enableFailPoint(DB::FailPoints::pause_proactive_flush_before_persist_region);
        ai->store(0b1011);
        auto f1 = [&]() {
            auto && [value_write, value_default] = proxy_instance->generateTiKVKeyValue(111, 999);
            auto k1 = RecordKVFormat::genKey(table_id, 60, 111);
            // Trigger a fg flush on region_id
            auto [index, term] = proxy_instance->rawWrite(region_id, {k1}, {value_default}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            auto [index2, term2] = proxy_instance->rawWrite(region_id, {k1}, {value_write}, {WriteCmdType::Put}, {ColumnFamilyType::Write});
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index2);
        };
        auto f2 = [&]() {
            auto && [value_write, value_default] = proxy_instance->generateTiKVKeyValue(111, 999);
            auto k1 = RecordKVFormat::genKey(table_id, 5, 111);
            // Trigger a fg flush on region_id2
            auto [index, term] = proxy_instance->rawWrite(region_id2, {k1}, {value_default}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            auto [index2, term2] = proxy_instance->rawWrite(region_id2, {k1}, {value_write}, {WriteCmdType::Put}, {ColumnFamilyType::Write});
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id2, index);
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id2, index2);
        };
        std::thread t1(f1);
        std::thread t2(f2);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        DB::FailPointHelper::disableFailPoint(DB::FailPoints::pause_proactive_flush_before_persist_region);
        t1.join();
        t2.join();
        ASSERT_EQ(proxy_instance->getRegion(region_id)->getApply().truncated_state().index(), proxy_instance->getRegion(region_id)->getLatestCommitIndex());
        ASSERT_EQ(proxy_instance->getRegion(region_id2)->getApply().truncated_state().index(), proxy_instance->getRegion(region_id2)->getLatestCommitIndex());
    }
    {
        // An obsolete notification triggered by another region's flush shall not override.
        kvs.notifyCompactLog(region_id, 1, 5, true, false);
        ASSERT_EQ(proxy_instance->getRegion(region_id)->getApply().truncated_state().index(), proxy_instance->getRegion(region_id)->getLatestCommitIndex());
    }
    {
        // Passive flush and fg proactive flush of the same region will not deadlock,
        // since they must be executed by order in one thread.
        // Passive flush and fg proactive flush will not deadlock.
        ai->store(0b1011); // Force fg
        DB::FailPointHelper::enableFailPoint(DB::FailPoints::pause_passive_flush_before_persist_region);
        auto f1 = [&]() {
            auto && [value_write, value_default] = proxy_instance->generateTiKVKeyValue(111, 999);
            auto k1 = RecordKVFormat::genKey(table_id, 60, 111);
            // Trigger a fg flush on region_id
            auto [index, term] = proxy_instance->rawWrite(region_id, {k1}, {value_default}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            auto [index2, term2] = proxy_instance->rawWrite(region_id, {k1}, {value_write}, {WriteCmdType::Put}, {ColumnFamilyType::Write});
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index2, std::make_optional(true));
        };
        auto f2 = [&]() {
            auto r2 = proxy_instance->getRegion(region_id2);
            auto && [request, response] = MockRaftStoreProxy::composeCompactLog(r2, 555);
            auto && [index2, term] = proxy_instance->adminCommand(region_id2, std::move(request), std::move(response), 600);
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id2, index2);
        };
        std::thread t1(f1);
        std::thread t2(f2);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        DB::FailPointHelper::disableFailPoint(DB::FailPoints::pause_passive_flush_before_persist_region);
        t2.join();
        ASSERT_EQ(proxy_instance->getRegion(region_id2)->getApply().truncated_state().index(), 555);
        DB::FailPointHelper::disableFailPoint(DB::FailPoints::pause_proactive_flush_before_persist_region);
        t1.join();
        ASSERT_EQ(proxy_instance->getRegion(region_id2)->getApply().truncated_state().index(), 555);
    }
    DB::FailPointHelper::disableFailPoint(DB::FailPoints::proactive_flush_force_set_type);
}
CATCH

TEST_F(RegionKVStoreTest, ProactiveFlushRecover1)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    std::shared_ptr<std::atomic<size_t>> ai = std::make_shared<std::atomic<size_t>>();
    // Safe to abort between flushCache and persistRegion.
    DB::FailPointHelper::enableFailPoint(DB::FailPoints::proactive_flush_force_set_type, ai);
    {
        auto tp = prepareForProactiveFlushTest();
        auto table_id = std::get<0>(tp);
        auto region_id = std::get<1>(tp);
        auto region_id2 = std::get<2>(tp);
        MockRaftStoreProxy::FailCond cond;

        DB::FailPointHelper::enableFailPoint(DB::FailPoints::proactive_flush_between_persist_cache_and_region);
        KVStore & kvs = getKVS();
        auto && [value_write, value_default] = proxy_instance->generateTiKVKeyValue(111, 999);
        auto k2 = RecordKVFormat::genKey(table_id, 5, 111);
        // Will not trigger a fg flush on region_id2
        auto [index2, term2] = proxy_instance->rawWrite(region_id2, {k2, k2}, {value_default, value_write}, {WriteCmdType::Put, WriteCmdType::Put}, {ColumnFamilyType::Default, ColumnFamilyType::Write});

        // Abort before persistRegion, but with DM flushed.
        cond.type = MockRaftStoreProxy::FailCond::Type::BEFORE_PROXY_ADVANCE;
        ai->store(0b1011);
        proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id2, index2);

        // If reload here, the data is lost since we don't persistRegion.
        // However, meta is not advanced either in KVStore or Proxy.
        auto & kvs2 = reloadKVSFromDisk();
        auto kvr2 = kvs2.getRegion(region_id2);
        auto r2 = proxy_instance->getRegion(region_id2);
        ASSERT_EQ(kvr2->appliedIndex() + 1, index2);
        ASSERT_EQ(r2->getLatestAppliedIndex() + 1, index2);

        cond.type = MockRaftStoreProxy::FailCond::Type::NORMAL;
        ai->store(0b1010);
        // No data lost.
        proxy_instance->doApply(kvs2, ctx.getTMTContext(), cond, region_id2, index2);
        auto [index22, term22] = proxy_instance->rawWrite(region_id2, {k2, k2}, {value_default, value_write}, {WriteCmdType::Put, WriteCmdType::Put}, {ColumnFamilyType::Default, ColumnFamilyType::Write});
        // There is no flush after write, so will throw when duplicate key.
        EXPECT_THROW(proxy_instance->doApply(kvs2, ctx.getTMTContext(), cond, region_id2, index22), Exception);

        ai->store(0b1011);
        DB::FailPointHelper::disableFailPoint(DB::FailPoints::proactive_flush_between_persist_cache_and_region);
        auto kvr1 = kvs2.getRegion(region_id);
        auto r1 = proxy_instance->getRegion(region_id);
        auto && [value_write1, value_default1] = proxy_instance->generateTiKVKeyValue(111, 999);
        auto k1 = RecordKVFormat::genKey(table_id, 60, 111);
        // Trigger a fg flush on region_id
        auto [index1, term1] = proxy_instance->rawWrite(region_id, {k1, k1}, {value_default1, value_write1}, {WriteCmdType::Put, WriteCmdType::Put}, {ColumnFamilyType::Default, ColumnFamilyType::Write});
        proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index1);

        auto & kvs3 = reloadKVSFromDisk();
        {
            auto kvr1 = kvs3.getRegion(region_id);
            auto r1 = proxy_instance->getRegion(region_id);
            ASSERT_EQ(kvr1->appliedIndex(), r1->getLatestAppliedIndex());
            auto kvr2 = kvs3.getRegion(region_id2);
            auto r2 = proxy_instance->getRegion(region_id2);
            ASSERT_EQ(kvr2->appliedIndex(), r2->getLatestAppliedIndex());
        }
    }

    DB::FailPointHelper::disableFailPoint(DB::FailPoints::proactive_flush_force_set_type);
}
CATCH

} // namespace tests
} // namespace DB