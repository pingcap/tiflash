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
TEST_F(RegionKVStoreTest, KVStoreFailRecovery)
{
    auto ctx = TiFlashTestEnv::getGlobalContext();
    {
        auto applied_index = 0;
        auto region_id = 1;
        {
            KVStore & kvs = getKVS();
            proxy_instance->bootstrap(kvs, ctx.getTMTContext(), region_id);
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
            kvs.tryPersist(region_id);
        }
        {
            const KVStore & kvs = reloadKVSFromDisk();
            auto kvr1 = kvs.getRegion(region_id);
            auto r1 = proxy_instance->getRegion(region_id);
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index + 1);
            ASSERT_EQ(kvr1->appliedIndex(), applied_index + 1);
            ASSERT_EQ(kvr1->appliedIndex(), r1->getLatestCommitIndex());
        }
    }

    {
        auto applied_index = 0;
        auto region_id = 2;
        {
            KVStore & kvs = getKVS();
            proxy_instance->bootstrap(kvs, ctx.getTMTContext(), region_id);
            MockRaftStoreProxy::FailCond cond;
            cond.type = MockRaftStoreProxy::FailCond::Type::BEFORE_KVSTORE_WRITE;

            auto kvr1 = kvs.getRegion(region_id);
            auto r1 = proxy_instance->getRegion(region_id);
            applied_index = r1->getLatestAppliedIndex();
            ASSERT_EQ(r1->getLatestAppliedIndex(), kvr1->appliedIndex());
            auto [index, term] = proxy_instance->normalWrite(region_id, {34}, {"v1"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            // KVStore failed before write and advance.
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index);
            ASSERT_EQ(kvr1->appliedIndex(), applied_index);
            kvs.tryPersist(region_id);
        }
        {
            KVStore & kvs = reloadKVSFromDisk();
            auto kvr1 = kvs.getRegion(region_id);
            auto r1 = proxy_instance->getRegion(region_id);
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index);
            ASSERT_EQ(kvr1->appliedIndex(), applied_index);
            ASSERT_EQ(kvr1->appliedIndex(), r1->getLatestCommitIndex() - 1);
            proxy_instance->replay(kvs, ctx.getTMTContext(), region_id, r1->getLatestCommitIndex());
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index + 1);
            ASSERT_EQ(kvr1->appliedIndex(), applied_index + 1);
        }
    }

    {
        auto applied_index = 0;
        auto region_id = 3;
        {
            KVStore & kvs = getKVS();
            proxy_instance->bootstrap(kvs, ctx.getTMTContext(), region_id);
            MockRaftStoreProxy::FailCond cond;
            cond.type = MockRaftStoreProxy::FailCond::Type::BEFORE_KVSTORE_ADVANCE;

            auto kvr1 = kvs.getRegion(region_id);
            auto r1 = proxy_instance->getRegion(region_id);
            applied_index = r1->getLatestAppliedIndex();
            ASSERT_EQ(r1->getLatestAppliedIndex(), kvr1->appliedIndex());
            auto [index, term] = proxy_instance->normalWrite(region_id, {34}, {"v1"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            // KVStore failed before advance.
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index);
            ASSERT_EQ(kvr1->appliedIndex(), applied_index);
            kvs.tryPersist(region_id);
        }
        {
            KVStore & kvs = reloadKVSFromDisk();
            auto kvr1 = kvs.getRegion(region_id);
            auto r1 = proxy_instance->getRegion(region_id);
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index);
            ASSERT_EQ(kvr1->appliedIndex(), applied_index);
            ASSERT_EQ(kvr1->appliedIndex(), r1->getLatestCommitIndex() - 1);
            EXPECT_THROW(proxy_instance->replay(kvs, ctx.getTMTContext(), region_id, r1->getLatestCommitIndex()), Exception);
        }
    }

    {
        auto applied_index = 0;
        auto region_id = 4;
        {
            KVStore & kvs = getKVS();
            proxy_instance->bootstrap(kvs, ctx.getTMTContext(), region_id);
            MockRaftStoreProxy::FailCond cond;
            cond.type = MockRaftStoreProxy::FailCond::Type::BEFORE_PROXY_ADVANCE;

            auto kvr1 = kvs.getRegion(region_id);
            auto r1 = proxy_instance->getRegion(region_id);
            applied_index = r1->getLatestAppliedIndex();
            ASSERT_EQ(r1->getLatestAppliedIndex(), kvr1->appliedIndex());
            LOG_INFO(&Poco::Logger::get("kvstore"), "applied_index {}", applied_index);
            auto [index, term] = proxy_instance->normalWrite(region_id, {35}, {"v1"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            // KVStore succeed. Proxy failed before advance.
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index);
            ASSERT_EQ(kvr1->appliedIndex(), applied_index + 1);
            kvs.tryPersist(region_id);
        }
        {
            MockRaftStoreProxy::FailCond cond;
            KVStore & kvs = reloadKVSFromDisk();
            auto kvr1 = kvs.getRegion(region_id);
            auto r1 = proxy_instance->getRegion(region_id);
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index);
            ASSERT_EQ(kvr1->appliedIndex(), applied_index + 1);
            ASSERT_EQ(kvr1->appliedIndex(), r1->getLatestCommitIndex());
            // Proxy shall replay from handle 35.
            proxy_instance->replay(kvs, ctx.getTMTContext(), region_id, r1->getLatestCommitIndex());
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index + 1);
            auto [index, term] = proxy_instance->normalWrite(region_id, {36}, {"v2"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index + 2);
        }
    }
}

TEST_F(RegionKVStoreTest, KVStoreAdminCommands)
{
    auto ctx = TiFlashTestEnv::getGlobalContext();
    {
        auto applied_index = 0;
        auto region_id = 1;
        {
            KVStore & kvs = getKVS();
            proxy_instance->bootstrap(kvs, ctx.getTMTContext(), region_id);
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
            kvs.setRegionCompactLogConfig(0, 0, 0);
            auto [index2, term2] = proxy_instance->compactLog(region_id, index);
            // In tryFlushRegionData we will call handleWriteRaftCmd, which will already cause an advance.
            // Notice kvs is not tmt->getKVStore(), so we can't use the ProxyFFI version.
            ASSERT_TRUE(kvs.tryFlushRegionData(region_id, false, true, ctx.getTMTContext(), index2, term));
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index2);
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index + 2);
            ASSERT_EQ(kvr1->appliedIndex(), applied_index + 2);
        }
    }
    {
        KVStore & kvs = getKVS();
        auto region_id = 1;
        proxy_instance->normalWrite(region_id, {34}, {"v2"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
        // There shall be data to flush.
        ASSERT_EQ(kvs.needFlushRegionData(region_id, ctx.getTMTContext()), true);
        // If flush fails, and we don't insist a success.
        FailPointHelper::enableFailPoint(FailPoints::force_fail_in_flush_region_data);
        ASSERT_EQ(kvs.tryFlushRegionData(region_id, false, false, ctx.getTMTContext(), 0, 0), false);
        FailPointHelper::disableFailPoint(FailPoints::force_fail_in_flush_region_data);
        // Force flush until succeed only for testing.
        ASSERT_EQ(kvs.tryFlushRegionData(region_id, false, true, ctx.getTMTContext(), 0, 0), true);
        // Non existing region.
        // Flush and CompactLog will not panic.
        ASSERT_EQ(kvs.tryFlushRegionData(1999, false, true, ctx.getTMTContext(), 0, 0), true);
        raft_cmdpb::AdminRequest request;
        raft_cmdpb::AdminResponse response;
        request.mutable_compact_log();
        request.set_cmd_type(::raft_cmdpb::AdminCmdType::CompactLog);
        ASSERT_EQ(kvs.handleAdminRaftCmd(raft_cmdpb::AdminRequest{request}, std::move(response), 1999, 22, 6, ctx.getTMTContext()), EngineStoreApplyRes::NotFound);
    }
}

TEST_F(RegionKVStoreTest, KVStoreSnapshot)
{
    auto ctx = TiFlashTestEnv::getGlobalContext();
    {
        UInt64 region_id = 1;
        TableID table_id;
        {
            initStorages();
            KVStore & kvs = getKVS();
            table_id = proxy_instance->bootstrap_table(ctx, kvs, ctx.getTMTContext());
            proxy_instance->bootstrap(kvs, ctx.getTMTContext(), region_id);
        }
        {
            KVStore & kvs = getKVS();
            auto kvr1 = kvs.getRegion(region_id);

            auto validate = [&](MockRaftStoreProxy::Cf & default_cf, int sst_size, int file_size) {
                auto proxy_helper = std::make_unique<TiFlashRaftProxyHelper>(MockRaftStoreProxy::SetRaftStoreProxyFFIHelper(
                    RaftStoreProxyPtr{proxy_instance.get()}));
                proxy_helper->sst_reader_interfaces = make_mock_sst_reader_interface();
                auto make_inner_func = [](const TiFlashRaftProxyHelper * proxy_helper, SSTView snap) {
                    return std::make_unique<MonoSSTReader>(proxy_helper, snap);
                };
                auto ssts = default_cf.ssts();
                ASSERT_EQ(ssts.size(), sst_size);
                MultiSSTReader<MonoSSTReader, SSTView> reader{proxy_helper.get(), ColumnFamilyType::Default, make_inner_func, ssts};
                size_t counter = 0;
                while (reader.remained())
                {
                    // repeatedly remained are called.
                    reader.remained();
                    reader.remained();
                    counter++;
                    auto v = std::string(reader.valueView().data);
                    ASSERT_EQ(v, "v" + std::to_string(counter));
                    reader.next();
                }
                ASSERT_EQ(counter, file_size);
            };

            {
                // Only one file
                MockSSTReader::getMockSSTData().clear();
                MockRaftStoreProxy::Cf default_cf{900, 800, ColumnFamilyType::Default};
                default_cf.insert(1, "v1");
                default_cf.insert(2, "v2");
                default_cf.finish_file();
                default_cf.freeze();
                validate(default_cf, 1, 2);
            }
            {
                // Empty
                MockSSTReader::getMockSSTData().clear();
                MockRaftStoreProxy::Cf default_cf{901, 800, ColumnFamilyType::Default};
                default_cf.finish_file();
                default_cf.freeze();
                validate(default_cf, 1, 0);
            }
            {
                // Multiple files
                MockSSTReader::getMockSSTData().clear();
                MockRaftStoreProxy::Cf default_cf{902, 800, ColumnFamilyType::Default};
                default_cf.insert(1, "v1");
                default_cf.finish_file();
                default_cf.insert(2, "v2");
                default_cf.finish_file();
                default_cf.insert(3, "v3");
                default_cf.insert(4, "v4");
                default_cf.finish_file();
                default_cf.insert(5, "v5");
                default_cf.insert(6, "v6");
                default_cf.finish_file();
                default_cf.insert(7, "v7");
                default_cf.finish_file();
                default_cf.freeze();
                validate(default_cf, 5, 7);
            }

            {
                // Test of ingesting multiple files with MultiSSTReader.
                MockSSTReader::getMockSSTData().clear();
                MockRaftStoreProxy::Cf default_cf{region_id, table_id, ColumnFamilyType::Default};
                default_cf.insert(1, "v1");
                default_cf.insert(2, "v2");
                default_cf.finish_file();
                default_cf.insert(3, "v3");
                default_cf.insert(4, "v4");
                default_cf.insert(5, "v5");
                default_cf.finish_file();
                default_cf.insert(6, "v6");
                default_cf.finish_file();
                default_cf.freeze();
                validate(default_cf, 3, 6);

                kvs.mutProxyHelperUnsafe()->sst_reader_interfaces = make_mock_sst_reader_interface();
                proxy_instance->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf}, 6, 6);

                MockRaftStoreProxy::FailCond cond;
                {
                    auto [index, term] = proxy_instance->normalWrite(region_id, {9}, {"v9"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
                    proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
                }
                {
                    // Test if write succeed.
                    auto [index, term] = proxy_instance->normalWrite(region_id, {1}, {"fv1"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
                    EXPECT_THROW(proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index), Exception);
                }
            }
            {
                // Test of ingesting single files with MultiSSTReader.
                MockSSTReader::getMockSSTData().clear();
                MockRaftStoreProxy::Cf default_cf{region_id, table_id, ColumnFamilyType::Default};
                default_cf.insert(10, "v10");
                default_cf.insert(11, "v11");
                default_cf.finish_file();
                default_cf.freeze();

                kvs.mutProxyHelperUnsafe()->sst_reader_interfaces = make_mock_sst_reader_interface();
                proxy_instance->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf}, 6, 6);

                MockRaftStoreProxy::FailCond cond;
                {
                    auto [index, term] = proxy_instance->normalWrite(region_id, {19}, {"v19"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
                    proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
                }
                {
                    // Test if write succeed.
                    auto [index, term] = proxy_instance->normalWrite(region_id, {10}, {"v10"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
                    EXPECT_THROW(proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index), Exception);
                }
            }
            {
                // Test of ingesting multiple cfs with MultiSSTReader.
                MockSSTReader::getMockSSTData().clear();
                MockRaftStoreProxy::Cf default_cf{region_id, table_id, ColumnFamilyType::Default};
                default_cf.insert(20, "v20");
                default_cf.insert(21, "v21");
                default_cf.finish_file();
                default_cf.freeze();
                MockRaftStoreProxy::Cf write_cf{region_id, table_id, ColumnFamilyType::Write};
                write_cf.insert(20, "v20");
                write_cf.insert(21, "v21");
                write_cf.finish_file();
                write_cf.freeze();

                kvs.mutProxyHelperUnsafe()->sst_reader_interfaces = make_mock_sst_reader_interface();
                proxy_instance->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 6, 6);

                MockRaftStoreProxy::FailCond cond;
                {
                    // Test if write succeed.
                    auto [index, term] = proxy_instance->normalWrite(region_id, {20}, {"v20"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
                    EXPECT_THROW(proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index), Exception);
                }
            }
        }
    }
}

} // namespace tests
} // namespace DB