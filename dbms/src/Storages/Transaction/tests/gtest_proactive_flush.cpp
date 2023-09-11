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
            auto [index, term]
                = proxy_instance
                      ->normalWrite(region_id, {33}, {"v1"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index + 1);
            ASSERT_EQ(kvr1->appliedIndex(), applied_index + 1);

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
            ASSERT_EQ(
                kvs.handleAdminRaftCmd(
                    raft_cmdpb::AdminRequest{request},
                    std::move(response),
                    1999,
                    22,
                    6,
                    ctx.getTMTContext()),
                EngineStoreApplyRes::NotFound);
        }
    }
}
CATCH

std::tuple<uint64_t, uint64_t> mockTableWrite(std::unique_ptr<MockRaftStoreProxy> & proxy_instance, KVStore & kvs, TMTContext & tmt, TableID table_id, RegionID region_id, HandleID handle_id, Timestamp ts) {
    auto && [value_write, value_default] = proxy_instance->generateTiKVKeyValue(ts, 999);
    auto k3 = RecordKVFormat::genKey(table_id, handle_id, ts);
    auto && [index, term] = proxy_instance->rawWrite(
        region_id,
        {k3, k3},
        {value_default, value_write},
        {WriteCmdType::Put, WriteCmdType::Put},
        {ColumnFamilyType::Default, ColumnFamilyType::Write},
        std::nullopt);
    MockRaftStoreProxy::FailCond cond;
    proxy_instance->doApply(kvs, tmt, cond, region_id, index);
    return std::make_tuple(index, term);
}

void mockTableBatchSplit(std::unique_ptr<MockRaftStoreProxy> & proxy_instance, KVStore & kvs, TMTContext & tmt, TableID table_id, RegionID region_id, RegionID region_id2, uint64_t split) {
    auto source_region = kvs.getRegion(region_id);
    auto old_epoch = source_region->mutMeta().getMetaRegion().region_epoch();
    const auto & ori_source_range = source_region->getRange()->comparableKeys();
    auto start_key = TiKVKey::copyFrom(ori_source_range.first.key);
    auto end_key = TiKVKey::copyFrom(ori_source_range.second.key);
    RegionRangeKeys::RegionRange new_source_range
        = RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(table_id, split), std::move(end_key));
    RegionRangeKeys::RegionRange new_target_range
        = RegionRangeKeys::makeComparableKeys(std::move(start_key), RecordKVFormat::genKey(table_id, split));
    auto && [request, response] = MockRaftStoreProxy::composeBatchSplit(
        {region_id, region_id2},
        regionRangeToEncodeKeys(new_source_range, new_target_range),
        old_epoch);
    auto [indexc, termc]
        = proxy_instance->adminCommand(region_id, std::move(request), std::move(response), std::nullopt);
    MockRaftStoreProxy::FailCond cond;
    proxy_instance->doApply(kvs, tmt, cond, region_id, indexc);
}

TEST_F(RegionKVStoreTest, KVStoreStalePassiveFlush)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    UInt64 region_id = 1;
    TableID table_id;
    PersistRegionState persist_state;
    {
        initStorages();
        KVStore & kvs = getKVS();
        table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
        auto start = RecordKVFormat::genKey(table_id, 0);
        auto end = RecordKVFormat::genKey(table_id, 10);
        proxy_instance->bootstrapWithRegion(
            kvs,
            ctx.getTMTContext(),
            region_id,
            std::make_pair(start.toString(), end.toString()));

        auto kvr1 = kvs.getRegion(region_id);
        auto r1 = proxy_instance->getRegion(region_id);
        ASSERT_NE(r1, nullptr);
        ASSERT_NE(kvr1, nullptr);

        mockTableWrite(proxy_instance, kvs, ctx.getTMTContext(), table_id, region_id, 3, 100);

        persist_state = kvs.getPersistRegionState(region_id);
        mockTableWrite(proxy_instance, kvs, ctx.getTMTContext(), table_id, region_id, 3, 112);
        mockTableBatchSplit(proxy_instance, kvs, ctx.getTMTContext(), table_id, region_id, 7, 5);
    }
    {
        KVStore & kvs2 = reloadKVSFromDisk();
        // Failed due to a split command.
        ASSERT_FALSE(kvs2.doFlushRegionDataWithState(region_id, persist_state));
    }
}
CATCH

// TODO(proactive flush)

} // namespace tests
} // namespace DB
