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

#include <Storages/KVStore/tests/region_kvstore_test.h>

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
            ASSERT_EQ(r1->getPersistedAppliedIndex(), applied_index + 1);
            ASSERT_EQ(kvr1->appliedIndex(), applied_index + 1);

            kvs.setRegionCompactLogConfig(0, 0, 0, 0);
            auto && [request, response] = MockRaftStoreProxy::composeCompactLog(r1, index);
            auto && [index2, term2] = proxy_instance->adminCommand(region_id, std::move(request), std::move(response));
            // In tryFlushRegionData we will call handleWriteRaftCmd, which will already cause an advance.
            // Notice kvs is not tmt->getKVStore(), so we can't use the ProxyFFI version.
            ASSERT_TRUE(kvs.tryFlushRegionData(region_id, false, true, ctx.getTMTContext(), index2, term, 0, 0));
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index2);
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index + 2);
            ASSERT_EQ(r1->getPersistedAppliedIndex(), applied_index + 2);
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

// TODO(proactive flush)

} // namespace tests
} // namespace DB
