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

#include <Common/MemoryTracker.h>
#include <Debug/MockKVStore/MockSSTGenerator.h>
#include <Debug/MockTiDB.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/KVStore/Read/LearnerRead.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/Utils/AsyncTasks.h>
#include <Storages/KVStore/tests/region_kvstore_test.h>
#include <Storages/RegionQueryInfo.h>
#include <TiDB/Schema/SchemaSyncService.h>
#include <TiDB/Schema/TiDBSchemaManager.h>

#include <limits>

extern std::shared_ptr<MemoryTracker> root_of_kvstore_mem_trackers;

namespace DB::tests
{

TEST_F(RegionKVStoreTest, RegionStruct)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    initStorages();
    MockRaftStoreProxy::FailCond cond;
    KVStore & kvs = getKVS();
    auto table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
    auto start = RecordKVFormat::genKey(table_id, 0);
    auto end = RecordKVFormat::genKey(table_id, 100);
    auto str_key = RecordKVFormat::genKey(table_id, 1, 111);
    auto [str_val_write, str_val_default] = proxy_instance->generateTiKVKeyValue(111, 999);
    auto str_lock_value
        = RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 111, 999).toString();
    proxy_instance->bootstrapWithRegion(kvs, ctx.getTMTContext(), 1, std::nullopt);
    {
        RegionID region_id = 1;
        auto kvr1 = kvs.getRegion(region_id);
        ASSERT_NE(kvr1, nullptr);
        auto [index, term] = proxy_instance->rawWrite(
            region_id,
            {str_key, str_key},
            {str_lock_value, str_val_default},
            {WriteCmdType::Put, WriteCmdType::Put},
            {ColumnFamilyType::Lock, ColumnFamilyType::Default});
        UNUSED(term);
        proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
        ASSERT_EQ(kvr1->getLockByKey(str_key)->dataSize(), str_lock_value.size());
        ASSERT_EQ(kvr1->getLockByKey(RecordKVFormat::genKey(table_id, 1, 112)), nullptr);
    }
}
CATCH


TEST_F(RegionKVStoreTest, MemoryTracker)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    initStorages();
    KVStore & kvs = getKVS();
    auto table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
    auto start = RecordKVFormat::genKey(table_id, 0);
    auto end = RecordKVFormat::genKey(table_id, 100);
    auto str_key = RecordKVFormat::genKey(table_id, 1, 111);
    auto [str_val_write, str_val_default] = proxy_instance->generateTiKVKeyValue(111, 999);
    auto str_lock_value
        = RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 111, 999).toString();
    MockRaftStoreProxy::FailCond cond;
    proxy_instance->debugAddRegions(
        kvs,
        ctx.getTMTContext(),
        {1, 2},
        {{RecordKVFormat::genKey(table_id, 0), RecordKVFormat::genKey(table_id, 10)},
         {RecordKVFormat::genKey(table_id, 11), RecordKVFormat::genKey(table_id, 20)}});

    {
        auto region_id = 1;
        auto kvr1 = kvs.getRegion(region_id);
        auto [index, term]
            = proxy_instance
                  ->rawWrite(region_id, {str_key}, {str_val_default}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
        UNUSED(term);
        proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), str_key.dataSize() + str_val_default.size());
    }

    {
        root_of_kvstore_mem_trackers->reset();
        RegionPtr region = tests::makeRegion(700, start, end, proxy_helper.get());
        region->insert("default", TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_val_default));
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), str_key.dataSize() + str_val_default.size());
        region->remove("default", TiKVKey::copyFrom(str_key));
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), 0);
    }
    ASSERT_EQ(root_of_kvstore_mem_trackers->get(), 0);
    {
        root_of_kvstore_mem_trackers->reset();
        RegionPtr region = tests::makeRegion(701, start, end, proxy_helper.get());
        region->insert("default", TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_val_default));
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), str_key.dataSize() + str_val_default.size());
    }
    ASSERT_EQ(root_of_kvstore_mem_trackers->get(), 0);
    {
        root_of_kvstore_mem_trackers->reset();
        RegionPtr region = tests::makeRegion(702, start, end, proxy_helper.get());
        region->insert("default", TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_val_default));
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), str_key.dataSize() + str_val_default.size());
        tryPersistRegion(kvs, 1);
        reloadKVSFromDisk();
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), str_key.dataSize() + str_val_default.size());
    }
    ASSERT_EQ(root_of_kvstore_mem_trackers->get(), 0);
    {
        root_of_kvstore_mem_trackers->reset();
        RegionPtr region = tests::makeRegion(800, start, end, proxy_helper.get());
        region->insert("default", TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_val_default));
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), str_key.dataSize() + str_val_default.size());
        region->insert("write", TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_val_write));
        std::optional<RegionDataReadInfoList> data_list_read = ReadRegionCommitCache(region, true);
        ASSERT_TRUE(data_list_read);
        ASSERT_EQ(1, data_list_read->size());
        RemoveRegionCommitCache(region, *data_list_read);
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), 0);
    }
    ASSERT_EQ(root_of_kvstore_mem_trackers->get(), 0);
    {
        root_of_kvstore_mem_trackers->reset();
        RegionPtr region = tests::makeRegion(900, start, end, proxy_helper.get());
        region->insert("default", TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_val_default));
        auto str_key2 = RecordKVFormat::genKey(table_id, 20, 111);
        auto [str_val_write2, str_val_default2] = proxy_instance->generateTiKVKeyValue(111, 999);
        region->insert("default", TiKVKey::copyFrom(str_key2), TiKVValue::copyFrom(str_val_default2));
        auto expected = str_key.dataSize() + str_val_default.size() + str_key2.dataSize() + str_val_default2.size();
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), expected);
        auto new_region = splitRegion(
            region,
            RegionMeta(
                createPeer(901, true),
                createRegionInfo(902, RecordKVFormat::genKey(table_id, 50), end),
                initialApplyState()));
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), expected);
        region->mergeDataFrom(*new_region);
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), expected);
    }
    {
        root_of_kvstore_mem_trackers->reset();
        RegionPtr region = tests::makeRegion(1000, start, end, proxy_helper.get());
        region->insert("lock", TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_lock_value));
        auto expected = str_key.dataSize() + str_lock_value.size();
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), expected);
        auto str_key2 = RecordKVFormat::genKey(table_id, 20, 111);
        std::string short_value(97, 'a');
        auto str_lock_value2
            = RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 20, 111, &short_value)
                  .toString();
        region->insert("lock", TiKVKey::copyFrom(str_key2), TiKVValue::copyFrom(str_lock_value2));
        expected += str_key2.dataSize() + str_lock_value2.size();
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), expected);
        auto new_region = splitRegion(
            region,
            RegionMeta(
                createPeer(1001, true),
                createRegionInfo(1002, RecordKVFormat::genKey(table_id, 50), end),
                initialApplyState()));
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), expected);
        region->mergeDataFrom(*new_region);
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), expected);
        region->insert("lock", TiKVKey::copyFrom(str_key2), TiKVValue::copyFrom(str_lock_value2));
        auto str_lock_value2_2
            = RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 20, 111).toString();
        region->insert("lock", TiKVKey::copyFrom(str_key2), TiKVValue::copyFrom(str_lock_value2_2));
        expected -= short_value.size();
        expected -= 2; // Short value prefix and length
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), expected);
    }
    ASSERT_EQ(root_of_kvstore_mem_trackers->get(), 0);
}
CATCH

TEST_F(RegionKVStoreTest, KVStoreFailRecovery)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    KVStore & kvs = getKVS();
    {
        auto applied_index = 0;
        auto region_id = 1;
        {
            MockRaftStoreProxy::FailCond cond;

            proxy_instance->debugAddRegions(
                kvs,
                ctx.getTMTContext(),
                {1},
                {{{RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 10)}}});
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
            tryPersistRegion(kvs, region_id);
        }
        {
            const KVStore & kvs = reloadKVSFromDisk();
            auto kvr1 = kvs.getRegion(region_id);
            auto r1 = proxy_instance->getRegion(region_id);
            ASSERT_EQ(r1->getPersistedAppliedIndex(), applied_index + 1);
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index + 1);
            ASSERT_EQ(kvr1->appliedIndex(), applied_index + 1);
            ASSERT_EQ(kvr1->appliedIndex(), r1->getLatestCommitIndex());
        }
    }

    {
        auto applied_index = 0;
        auto region_id = 2;
        {
            proxy_instance->debugAddRegions(
                kvs,
                ctx.getTMTContext(),
                {2},
                {{{RecordKVFormat::genKey(1, 10), RecordKVFormat::genKey(1, 20)}}});
            MockRaftStoreProxy::FailCond cond;
            cond.type = MockRaftStoreProxy::FailCond::Type::BEFORE_KVSTORE_WRITE;

            auto kvr1 = kvs.getRegion(region_id);
            auto r1 = proxy_instance->getRegion(region_id);
            applied_index = r1->getLatestAppliedIndex();
            ASSERT_EQ(r1->getLatestAppliedIndex(), kvr1->appliedIndex());
            auto [index, term]
                = proxy_instance
                      ->normalWrite(region_id, {34}, {"v1"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            // KVStore failed before write and advance.
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index + 1);
            ASSERT_EQ(r1->getPersistedAppliedIndex(), applied_index);
            ASSERT_EQ(kvr1->appliedIndex(), applied_index);
            tryPersistRegion(kvs, region_id);
        }
        {
            MockRaftStoreProxy::FailCond cond;
            KVStore & kvs = reloadKVSFromDisk();
            auto kvr1 = kvs.getRegion(region_id);
            auto r1 = proxy_instance->getRegion(region_id);
            ASSERT_EQ(kvr1->lastCompactLogApplied(), 5);
            ASSERT_EQ(r1->getPersistedAppliedIndex(), applied_index);
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index);
            ASSERT_EQ(kvr1->appliedIndex(), applied_index);
            ASSERT_EQ(kvr1->appliedIndex(), r1->getLatestCommitIndex() - 1);
            proxy_instance->replay(kvs, ctx.getTMTContext(), region_id, r1->getLatestCommitIndex());
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index + 1);
            ASSERT_EQ(kvr1->appliedIndex(), applied_index + 1);

            auto && [req, res] = MockRaftStoreProxy::composeCompactLog(r1, kvr1->appliedIndex());
            auto [indexc, termc]
                = proxy_instance->adminCommand(region_id, std::move(req), std::move(res), std::nullopt);
            // Reject compact log.
            kvs.setRegionCompactLogConfig(10000000, 10000000, 10000000, 0);
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, indexc);
            ASSERT_EQ(kvr1->lastCompactLogApplied(), 5);
        }
    }

    {
        auto applied_index = 0;
        auto region_id = 3;
        {
            proxy_instance->debugAddRegions(
                kvs,
                ctx.getTMTContext(),
                {3},
                {{{RecordKVFormat::genKey(1, 30), RecordKVFormat::genKey(1, 40)}}});
            MockRaftStoreProxy::FailCond cond;
            cond.type = MockRaftStoreProxy::FailCond::Type::BEFORE_KVSTORE_ADVANCE;

            auto kvr1 = kvs.getRegion(region_id);
            auto r1 = proxy_instance->getRegion(region_id);
            applied_index = r1->getLatestAppliedIndex();
            ASSERT_EQ(r1->getLatestAppliedIndex(), kvr1->appliedIndex());
            auto [index, term]
                = proxy_instance
                      ->normalWrite(region_id, {34}, {"v1"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            // KVStore failed before advance.
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index + 1);
            ASSERT_EQ(kvr1->appliedIndex(), applied_index);
            ASSERT_NE(kvr1->appliedIndex(), index);
            // The persisted applied_index is `applied_index`.
            tryPersistRegion(kvs, region_id);
        }
        {
            KVStore & kvs = reloadKVSFromDisk();
            auto kvr1 = kvs.getRegion(region_id);
            auto r1 = proxy_instance->getRegion(region_id);
            ASSERT_EQ(r1->getPersistedAppliedIndex(), applied_index);
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index);
            ASSERT_EQ(kvr1->appliedIndex(), applied_index);
            ASSERT_EQ(kvr1->appliedIndex(), r1->getLatestCommitIndex() - 1);
            EXPECT_THROW(
                proxy_instance->replay(kvs, ctx.getTMTContext(), region_id, r1->getLatestCommitIndex()),
                Exception);
        }
    }

    {
        auto applied_index = 0;
        auto region_id = 4;
        {
            proxy_instance->debugAddRegions(
                kvs,
                ctx.getTMTContext(),
                {4},
                {{{RecordKVFormat::genKey(1, 50), RecordKVFormat::genKey(1, 60)}}});
            MockRaftStoreProxy::FailCond cond;
            cond.type = MockRaftStoreProxy::FailCond::Type::BEFORE_PROXY_PERSIST_ADVANCE;

            auto kvr1 = kvs.getRegion(region_id);
            auto r1 = proxy_instance->getRegion(region_id);
            applied_index = r1->getLatestAppliedIndex();
            ASSERT_EQ(r1->getLatestAppliedIndex(), kvr1->appliedIndex());
            LOG_INFO(Logger::get(), "applied_index {}", applied_index);
            auto [index, term]
                = proxy_instance
                      ->normalWrite(region_id, {35}, {"v1"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            // KVStore succeed. Proxy failed before advance.
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            ASSERT_EQ(r1->getPersistedAppliedIndex(), applied_index);
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index + 1);
            ASSERT_EQ(kvr1->appliedIndex(), applied_index + 1);
            tryPersistRegion(kvs, region_id);
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
            auto [index, term]
                = proxy_instance
                      ->normalWrite(region_id, {36}, {"v2"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            ASSERT_EQ(r1->getLatestAppliedIndex(), applied_index + 2);
        }
    }
}
CATCH

TEST_F(RegionKVStoreTest, KVStoreInvalidWrites)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    {
        auto region_id = 1;
        {
            initStorages();
            KVStore & kvs = getKVS();
            proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
            proxy_instance->bootstrapWithRegion(kvs, ctx.getTMTContext(), region_id, std::nullopt);

            MockRaftStoreProxy::FailCond cond;

            auto kvr1 = kvs.getRegion(region_id);
            auto r1 = proxy_instance->getRegion(region_id);
            ASSERT_NE(r1, nullptr);
            ASSERT_NE(kvr1, nullptr);
            ASSERT_EQ(r1->getLatestAppliedIndex(), kvr1->appliedIndex());
            {
                r1->getLatestAppliedIndex();
                // This key has empty PK which is actually truncated.
                std::string k = "7480000000000001FFBD5F720000000000FAF9ECEFDC3207FFFC";
                std::string v = "4486809092ACFEC38906";
                auto str_key = Redact::hexStringToKey(k.data(), k.size());
                auto str_val = Redact::hexStringToKey(v.data(), v.size());

                auto [index, term]
                    = proxy_instance
                          ->rawWrite(region_id, {str_key}, {str_val}, {WriteCmdType::Put}, {ColumnFamilyType::Write});
                EXPECT_THROW(proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index), Exception);
                UNUSED(term);
                EXPECT_THROW(ReadRegionCommitCache(kvr1, true), Exception);
            }
        }
    }
}
CATCH

TEST_F(RegionKVStoreTest, KVStoreAdminCommands)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    {
        KVStore & kvs = getKVS();
        UInt64 region_id = 2;
        proxy_instance->debugAddRegions(
            kvs,
            ctx.getTMTContext(),
            {region_id},
            {{{RecordKVFormat::genKey(1, 10), RecordKVFormat::genKey(1, 20)}}});

        // InvalidAdmin
        raft_cmdpb::AdminRequest request;
        raft_cmdpb::AdminResponse response;

        request.set_cmd_type(::raft_cmdpb::AdminCmdType::InvalidAdmin);
        try
        {
            kvs.handleAdminRaftCmd(std::move(request), std::move(response), region_id, 110, 6, ctx.getTMTContext());
            ASSERT_TRUE(false);
        }
        catch (Exception & e)
        {
            ASSERT_EQ(e.message(), "unsupported admin command type InvalidAdmin");
        }
    }
    {
        // All "useless" commands.
        KVStore & kvs = getKVS();
        UInt64 region_id = 3;
        proxy_instance->debugAddRegions(
            kvs,
            ctx.getTMTContext(),
            {region_id},
            {{{RecordKVFormat::genKey(1, 20), RecordKVFormat::genKey(1, 30)}}});
        raft_cmdpb::AdminRequest request;
        raft_cmdpb::AdminResponse response2;
        raft_cmdpb::AdminResponse response;

        request.mutable_compact_log();
        request.set_cmd_type(::raft_cmdpb::AdminCmdType::CompactLog);
        response = response2;
        ASSERT_EQ(
            kvs.handleAdminRaftCmd(
                raft_cmdpb::AdminRequest{request},
                std::move(response),
                region_id,
                22,
                6,
                ctx.getTMTContext()),
            EngineStoreApplyRes::Persist);

        response = response2;
        ASSERT_EQ(
            kvs.handleAdminRaftCmd(
                raft_cmdpb::AdminRequest{request},
                std::move(response),
                region_id,
                23,
                6,
                ctx.getTMTContext()),
            EngineStoreApplyRes::Persist);

        response = response2;
        ASSERT_EQ(
            kvs.handleAdminRaftCmd(
                raft_cmdpb::AdminRequest{request},
                std::move(response),
                8192,
                5,
                6,
                ctx.getTMTContext()),
            EngineStoreApplyRes::NotFound);

        request.set_cmd_type(::raft_cmdpb::AdminCmdType::ComputeHash);
        response = response2;
        ASSERT_EQ(
            kvs.handleAdminRaftCmd(
                raft_cmdpb::AdminRequest{request},
                std::move(response),
                region_id,
                24,
                6,
                ctx.getTMTContext()),
            EngineStoreApplyRes::None);

        request.set_cmd_type(::raft_cmdpb::AdminCmdType::VerifyHash);
        response = response2;
        ASSERT_EQ(
            kvs.handleAdminRaftCmd(
                raft_cmdpb::AdminRequest{request},
                std::move(response),
                region_id,
                25,
                6,
                ctx.getTMTContext()),
            EngineStoreApplyRes::None);

        {
            kvs.setRegionCompactLogConfig(0, 0, 0, 0);
            request.set_cmd_type(::raft_cmdpb::AdminCmdType::CompactLog);
            ASSERT_EQ(
                kvs.handleAdminRaftCmd(std::move(request), std::move(response2), region_id, 26, 6, ctx.getTMTContext()),
                EngineStoreApplyRes::Persist);
        }
    }
}
CATCH

TEST_F(RegionKVStoreTest, KVStoreSnapshotV1)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    ASSERT_NE(proxy_helper->sst_reader_interfaces.fn_key, nullptr);
    {
        UInt64 region_id = 1;
        TableID table_id;
        {
            initStorages();
            KVStore & kvs = getKVS();
            table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
            LOG_INFO(&Poco::Logger::get("Test"), "generated table_id {}", table_id);
            proxy_instance->bootstrapWithRegion(kvs, ctx.getTMTContext(), region_id, std::nullopt);
            auto kvr1 = kvs.getRegion(region_id);
            auto r1 = proxy_instance->getRegion(region_id);
            {
                // Only one file
                MockSSTReader::getMockSSTData().clear();
                MockSSTGenerator default_cf{900, 800, ColumnFamilyType::Default};
                default_cf.insert(1, "v1");
                default_cf.insert(2, "v2");
                default_cf.finish_file();
                default_cf.freeze();
                validateSSTGeneration(kvs, proxy_instance, region_id, default_cf, ColumnFamilyType::Default, 1, 2);
            }
            {
                // Empty
                MockSSTReader::getMockSSTData().clear();
                MockSSTGenerator default_cf{901, 800, ColumnFamilyType::Default};
                default_cf.finish_file();
                default_cf.freeze();
                validateSSTGeneration(kvs, proxy_instance, region_id, default_cf, ColumnFamilyType::Default, 1, 0);
            }
            {
                // Multiple files
                MockSSTReader::getMockSSTData().clear();
                MockSSTGenerator default_cf{902, 800, ColumnFamilyType::Default};
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
                validateSSTGeneration(kvs, proxy_instance, region_id, default_cf, ColumnFamilyType::Default, 5, 7);
            }

            {
                // Test of ingesting multiple files with MultiSSTReader.
                MockSSTReader::getMockSSTData().clear();
                MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
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
                validateSSTGeneration(kvs, proxy_instance, region_id, default_cf, ColumnFamilyType::Default, 3, 6);

                kvs.mutProxyHelperUnsafe()->sst_reader_interfaces = make_mock_sst_reader_interface();
                proxy_instance->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf}, 0, 0, std::nullopt);

                MockRaftStoreProxy::FailCond cond;
                {
                    auto [index, term]
                        = proxy_instance
                              ->normalWrite(region_id, {9}, {"v9"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
                    proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
                }
                {
                    // Test if write succeed.
                    auto [index, term]
                        = proxy_instance
                              ->normalWrite(region_id, {1}, {"fv1"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
                    EXPECT_THROW(proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index), Exception);
                }
            }
            {
                // Test of ingesting single file with MultiSSTReader.
                MockSSTReader::getMockSSTData().clear();
                MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
                default_cf.insert(10, "v10");
                default_cf.insert(11, "v11");
                default_cf.finish_file();
                default_cf.freeze();

                kvs.mutProxyHelperUnsafe()->sst_reader_interfaces = make_mock_sst_reader_interface();
                proxy_instance->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf}, 0, 0, std::nullopt);

                MockRaftStoreProxy::FailCond cond;
                {
                    auto [index, term]
                        = proxy_instance
                              ->normalWrite(region_id, {19}, {"v19"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
                    proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
                }
                {
                    // Test if write succeed.
                    auto [index, term]
                        = proxy_instance
                              ->normalWrite(region_id, {10}, {"v10"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
                    EXPECT_THROW(proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index), Exception);
                }
            }
            {
                // Test of ingesting multiple cfs with MultiSSTReader.
                MockSSTReader::getMockSSTData().clear();
                MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
                default_cf.insert(20, "v20");
                default_cf.insert(21, "v21");
                default_cf.finish_file();
                default_cf.freeze();
                MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
                write_cf.insert(20, "v20");
                write_cf.insert(21, "v21");
                write_cf.finish_file();
                write_cf.freeze();

                kvs.mutProxyHelperUnsafe()->sst_reader_interfaces = make_mock_sst_reader_interface();
                proxy_instance
                    ->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 0, 0, std::nullopt);

                MockRaftStoreProxy::FailCond cond;
                {
                    // Test if write succeed.
                    auto [index, term]
                        = proxy_instance
                              ->normalWrite(region_id, {20}, {"v20"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
                    // Found existing key ...
                    EXPECT_THROW(proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index), Exception);
                }
            }
            {
                // Test of ingesting duplicated key with the same value.
                MockSSTReader::getMockSSTData().clear();
                MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
                default_cf.insert(21, "v21");
                default_cf.insert(21, "v21");
                default_cf.finish_file();
                default_cf.freeze();
                MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
                write_cf.insert(21, "v21");
                write_cf.insert(21, "v21");
                write_cf.finish_file();
                write_cf.freeze();

                kvs.mutProxyHelperUnsafe()->sst_reader_interfaces = make_mock_sst_reader_interface();
                // Shall not panic.
                proxy_instance
                    ->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 0, 0, std::nullopt);
            }
            {
                // Test of ingesting duplicated key with different values.
                MockSSTReader::getMockSSTData().clear();
                MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
                default_cf.insert(21, "v21");
                default_cf.insert(21, "v22");
                default_cf.finish_file();
                default_cf.freeze();
                MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
                write_cf.insert(21, "v21");
                write_cf.insert(21, "v21");
                write_cf.finish_file();
                write_cf.freeze();

                kvs.mutProxyHelperUnsafe()->sst_reader_interfaces = make_mock_sst_reader_interface();
                // Found existing key ...
                EXPECT_THROW(
                    proxy_instance
                        ->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 0, 0, std::nullopt),
                    Exception);
            }
            {
                // Test of cancel prehandled.
                MockSSTReader::getMockSSTData().clear();
                auto k1 = RecordKVFormat::genKey(table_id, 1, 111);
                auto && [value_write1, value_default1] = proxy_instance->generateTiKVKeyValue(111, 999);
                MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
                default_cf.insert_raw(k1, value_default1);
                default_cf.finish_file();
                default_cf.freeze();
                MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
                write_cf.insert_raw(k1, value_write1);
                write_cf.finish_file();
                write_cf.freeze();

                kvs.mutProxyHelperUnsafe()->sst_reader_interfaces = make_mock_sst_reader_interface();
                auto [r, res] = proxy_instance->snapshot(
                    kvs,
                    ctx.getTMTContext(),
                    region_id,
                    {default_cf, write_cf},
                    0,
                    0,
                    std::nullopt,
                    /*cancel_after_prehandle=*/true);
            }
        }
    }
}
CATCH

TEST_F(RegionKVStoreTest, LearnerRead)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    const RegionID region_id = 1;
    KVStore & kvs = getKVS();
    ctx.getTMTContext().debugSetKVStore(kvstore);
    initStorages();

    ctx.getTMTContext().debugSetWaitIndexTimeout(1);

    startReadIndexUtils(ctx);
    SCOPE_EXIT({ stopReadIndexUtils(); });

    const auto table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
    proxy_instance->bootstrapWithRegion(kvs, ctx.getTMTContext(), region_id, std::nullopt);
    const auto region_1 = kvs.getRegion(region_id);
    ASSERT_EQ(region_1->appliedIndex(), 5);
    ASSERT_EQ(region_1->appliedIndexTerm(), 5);

    // prepare 3 kv for region_1
    Strings keys{
        RecordKVFormat::genKey(table_id, 3).toString(),
        RecordKVFormat::genKey(table_id, 3, 5).toString(),
        RecordKVFormat::genKey(table_id, 3, 8).toString(),
    };
    Strings vals{
        RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 3, 20).toString(),
        TiKVValue("value1").toString(),
        RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 5).toString(),
    };
    auto ops = std::vector<ColumnFamilyType>{
        ColumnFamilyType::Lock,
        ColumnFamilyType::Default,
        ColumnFamilyType::Write,
    };
    auto [index, term] = proxy_instance->rawWrite(
        region_id,
        std::move(keys),
        std::move(vals),
        {WriteCmdType::Put, WriteCmdType::Put, WriteCmdType::Put},
        std::move(ops));
    // the applied index of region is not advanced
    ASSERT_EQ(region_1->appliedIndex(), 5);
    ASSERT_EQ(region_1->appliedIndexTerm(), 5);
    ASSERT_EQ(index, 6);
    ASSERT_EQ(term, 5);

    auto mvcc_query_info = MvccQueryInfo(false, 10);
    mvcc_query_info.regions_query_info.emplace_back(RegionQueryInfo{
        region_id,
        region_1->version(),
        region_1->confVer(),
        table_id,
        region_1->getRange()->rawKeys(),
        {},
    });
    EXPECT_THROW(
        {
            // The region applied index does not get advanced and will
            // get wait index timeout
            auto discard = doLearnerRead(table_id, mvcc_query_info, false, ctx, log);
            UNUSED(discard);
        },
        RegionException);

    {
        /// Advance the index in `MockRaftStoreProxy`
        // We can't `doApply`, since the TiKVValue is not valid.
        auto r1 = proxy_instance->getRegion(region_id);
        r1->updateAppliedIndex(index, true);
    }
    region_1->setApplied(index, term);
    auto regions_snapshot = doLearnerRead(table_id, mvcc_query_info, false, ctx, log);
    // 0 unavailable regions
    ASSERT_EQ(regions_snapshot.size(), 1);

    // Check the regions_snapshot after learner read done and snapshot on the storage is acquired.
    // Should not throw
    validateQueryInfo(mvcc_query_info, regions_snapshot, ctx.getTMTContext(), log);
}
CATCH

TEST_F(RegionKVStoreTest, KVStoreApplyEmptySnapshot)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    proxy_instance->cluster_ver = RaftstoreVer::V2;
    UInt64 region_id = 1;
    TableID table_id;
    {
        region_id = 2;
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
        auto r1 = proxy_instance->getRegion(region_id);

        {
            MockSSTReader::getMockSSTData().clear();
            MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
            write_cf.finish_file(SSTFormatKind::KIND_TABLET);
            write_cf.freeze();

            proxy_instance->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 0, 0, std::nullopt);
        }
    }
}
CATCH

void RegionKVStoreTest::dropTable(Context & ctx, TableID table_id)
{
    MockTiDB::instance().dropTableById(ctx, table_id, /*drop_regions*/ false);
    auto & tmt = ctx.getTMTContext();
    auto schema_syncer = tmt.getSchemaSyncerManager();
    schema_syncer->syncSchemas(ctx, NullspaceID);
    auto sync_service = std::make_shared<SchemaSyncService>(ctx);
    sync_service->gcImpl(std::numeric_limits<Timestamp>::max(), NullspaceID, /*ignore_remain_regions*/ true);
    sync_service->shutdown();
}

TEST_F(RegionKVStoreTest, KVStoreApplyWriteToNonExistStorage)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    proxy_instance->cluster_ver = RaftstoreVer::V2;
    RegionID region_id = 2;
    initStorages();
    KVStore & kvs = getKVS();
    TableID table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
    auto start = RecordKVFormat::genKey(table_id, 0);
    auto end = RecordKVFormat::genKey(table_id, 100);
    proxy_instance
        ->bootstrapWithRegion(kvs, ctx.getTMTContext(), region_id, std::make_pair(start.toString(), end.toString()));

    {
        auto str_key = RecordKVFormat::genKey(table_id, 1, 111);
        auto [str_val_write, str_val_default] = proxy_instance->generateTiKVKeyValue(111, 999);
        auto str_lock_value
            = RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 111, 999).toString();
        auto kvr1 = kvs.getRegion(region_id);
        ASSERT_NE(kvr1, nullptr);
        auto [index, term] = proxy_instance->rawWrite(
            region_id,
            {str_key, str_key, str_key},
            {str_lock_value, str_val_default, str_val_write},
            {WriteCmdType::Put, WriteCmdType::Put, WriteCmdType::Put},
            {ColumnFamilyType::Lock, ColumnFamilyType::Default, ColumnFamilyType::Write});
        UNUSED(term);

        dropTable(ctx, table_id);

        // No exception thrown, the rows are just throw away
        MockRaftStoreProxy::FailCond cond;
        proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
    }
}
CATCH

TEST_F(RegionKVStoreTest, MemoryTrace)
try
{
    KVStore & kvs = getKVS();
    std::string name = "test1-1";
    kvs.reportThreadAllocInfo(std::string_view(name.data(), name.size()), ReportThreadAllocateInfoType::Reset, 0);
    kvs.reportThreadAllocBatch(
        std::string_view(name.data(), name.size()),
        ReportThreadAllocateInfoBatch{.alloc = 1, .dealloc = 2});
    auto & tiflash_metrics = TiFlashMetrics::instance();
    ASSERT_EQ(tiflash_metrics.getProxyThreadMemory(TiFlashMetrics::MemoryAllocType::Alloc, "test1"), 1);
    ASSERT_EQ(tiflash_metrics.getProxyThreadMemory(TiFlashMetrics::MemoryAllocType::Dealloc, "test1"), 2);
    std::string namee;
    kvs.reportThreadAllocBatch(
        std::string_view(namee.data(), namee.size()),
        ReportThreadAllocateInfoBatch{.alloc = 1, .dealloc = 2});
    EXPECT_ANY_THROW(tiflash_metrics.getProxyThreadMemory(TiFlashMetrics::MemoryAllocType::Alloc, ""));
    // bg pool and blockable-bg pool
    kvs.getJointThreadInfoJeallocMap()->accessStorageMap([](const JointThreadInfoJeallocMap::AllocMap & m) {
        ASSERT_EQ(m.size(), TiFlashTestEnv::DEFAULT_BG_POOL_SIZE * 2);
    });
    kvs.getJointThreadInfoJeallocMap()->accessProxyMap(
        [](const JointThreadInfoJeallocMap::AllocMap & m) { ASSERT_EQ(m.size(), 1); });
    std::thread t([&]() {
        // For names not in `PROXY_RECORD_WHITE_LIST_THREAD_PREFIX`, the record operation will not update.
        std::string name1 = "test11-1";
        kvs.reportThreadAllocInfo(std::string_view(name1.data(), name1.size()), ReportThreadAllocateInfoType::Reset, 0);
        uint64_t mock = 999;
        auto alloc_ptr = reinterpret_cast<uint64_t>(&mock);
        kvs.reportThreadAllocInfo(
            std::string_view(name1.data(), name1.size()),
            ReportThreadAllocateInfoType::AllocPtr,
            alloc_ptr);
        recordThreadAllocInfoForProxy(kvs.getJointThreadInfoJeallocMap());
        ASSERT_EQ(tiflash_metrics.getProxyThreadMemory(TiFlashMetrics::MemoryAllocType::Alloc, "test11"), 0);

        // recordThreadAllocInfoForProxy can't override if not all alloc/dealloc are provided.
        std::string name2 = "ReadIndexWkr-1";
        kvs.reportThreadAllocInfo(std::string_view(name2.data(), name2.size()), ReportThreadAllocateInfoType::Reset, 0);
        kvs.reportThreadAllocBatch(
            std::string_view(name2.data(), name2.size()),
            ReportThreadAllocateInfoBatch{.alloc = 111, .dealloc = 222});
        kvs.reportThreadAllocInfo(
            std::string_view(name2.data(), name2.size()),
            ReportThreadAllocateInfoType::AllocPtr,
            alloc_ptr);
        recordThreadAllocInfoForProxy(kvs.getJointThreadInfoJeallocMap());
        ASSERT_EQ(tiflash_metrics.getProxyThreadMemory(TiFlashMetrics::MemoryAllocType::Alloc, "ReadIndexWkr"), 111);
        ASSERT_EQ(tiflash_metrics.getProxyThreadMemory(TiFlashMetrics::MemoryAllocType::Dealloc, "ReadIndexWkr"), 222);

        // recordThreadAllocInfoForProxy will override if all alloc/dealloc are both provided,
        // because the infomation from pointer is always the newest.
        uint64_t mock2 = 998;
        auto dealloc_ptr = reinterpret_cast<uint64_t>(&mock2);
        kvs.reportThreadAllocInfo(
            std::string_view(name2.data(), name2.size()),
            ReportThreadAllocateInfoType::DeallocPtr,
            dealloc_ptr);
        recordThreadAllocInfoForProxy(kvs.getJointThreadInfoJeallocMap());
        ASSERT_EQ(tiflash_metrics.getProxyThreadMemory(TiFlashMetrics::MemoryAllocType::Alloc, "ReadIndexWkr"), 999);
        ASSERT_EQ(tiflash_metrics.getProxyThreadMemory(TiFlashMetrics::MemoryAllocType::Dealloc, "ReadIndexWkr"), 998);
        kvs.reportThreadAllocInfo(
            std::string_view(name2.data(), name2.size()),
            ReportThreadAllocateInfoType::Remove,
            0);


        kvs.reportThreadAllocInfo(std::string_view(name2.data(), name2.size()), ReportThreadAllocateInfoType::Reset, 0);
        kvs.reportThreadAllocBatch(
            std::string_view(name2.data(), name2.size()),
            ReportThreadAllocateInfoBatch{.alloc = 111, .dealloc = 222});
        recordThreadAllocInfoForProxy(kvs.getJointThreadInfoJeallocMap());
        ASSERT_EQ(tiflash_metrics.getProxyThreadMemory(TiFlashMetrics::MemoryAllocType::Alloc, "ReadIndexWkr"), 111);
        ASSERT_EQ(tiflash_metrics.getProxyThreadMemory(TiFlashMetrics::MemoryAllocType::Dealloc, "ReadIndexWkr"), 222);
    });
    t.join();
}
CATCH


TEST_F(RegionKVStoreTest, MemoryTraceAgg)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    uint64_t al1 = 1;
    uint64_t al2 = 2;
    uint64_t dl = 3;
    auto & tiflash_metrics = TiFlashMetrics::instance();
    std::string name1 = "non-agg-1";
    ctx.getJointThreadInfoJeallocMap()
        ->reportThreadAllocInfoForStorage(name1, ReportThreadAllocateInfoType::Reset, 0, '\0');
    ctx.getJointThreadInfoJeallocMap()->reportThreadAllocInfoForStorage(
        name1,
        ReportThreadAllocateInfoType::AllocPtr,
        reinterpret_cast<uint64_t>(&al1),
        '\0');
    ctx.getJointThreadInfoJeallocMap()->reportThreadAllocInfoForStorage(
        name1,
        ReportThreadAllocateInfoType::DeallocPtr,
        reinterpret_cast<uint64_t>(&dl),
        '\0');
    std::string name2 = "non-agg-2";
    ctx.getJointThreadInfoJeallocMap()
        ->reportThreadAllocInfoForStorage(name2, ReportThreadAllocateInfoType::Reset, 0, '\0');
    ctx.getJointThreadInfoJeallocMap()->reportThreadAllocInfoForStorage(
        name2,
        ReportThreadAllocateInfoType::AllocPtr,
        reinterpret_cast<uint64_t>(&al2),
        '\0');
    ctx.getJointThreadInfoJeallocMap()->reportThreadAllocInfoForStorage(
        name2,
        ReportThreadAllocateInfoType::DeallocPtr,
        reinterpret_cast<uint64_t>(&dl),
        '\0');
    std::string name3 = "agg+1";
    ctx.getJointThreadInfoJeallocMap()
        ->reportThreadAllocInfoForStorage(name3, ReportThreadAllocateInfoType::Reset, 0, '+');
    ctx.getJointThreadInfoJeallocMap()->reportThreadAllocInfoForStorage(
        name3,
        ReportThreadAllocateInfoType::AllocPtr,
        reinterpret_cast<uint64_t>(&al1),
        '\0');
    ctx.getJointThreadInfoJeallocMap()->reportThreadAllocInfoForStorage(
        name3,
        ReportThreadAllocateInfoType::DeallocPtr,
        reinterpret_cast<uint64_t>(&dl),
        '\0');
    std::string name4 = "agg+2";
    ctx.getJointThreadInfoJeallocMap()
        ->reportThreadAllocInfoForStorage(name4, ReportThreadAllocateInfoType::Reset, 0, '+');
    ctx.getJointThreadInfoJeallocMap()->reportThreadAllocInfoForStorage(
        name4,
        ReportThreadAllocateInfoType::AllocPtr,
        reinterpret_cast<uint64_t>(&al2),
        '\0');
    ctx.getJointThreadInfoJeallocMap()->reportThreadAllocInfoForStorage(
        name4,
        ReportThreadAllocateInfoType::DeallocPtr,
        reinterpret_cast<uint64_t>(&dl),
        '\0');
    ctx.getJointThreadInfoJeallocMap()->recordThreadAllocInfo();
    ASSERT_EQ(al1 + al2, tiflash_metrics.getStorageThreadMemory(TiFlashMetrics::MemoryAllocType::Alloc, "agg"));
    ASSERT_EQ(al1, tiflash_metrics.getStorageThreadMemory(TiFlashMetrics::MemoryAllocType::Alloc, "non-agg-1"));
    ctx.getJointThreadInfoJeallocMap()
        ->reportThreadAllocInfoForStorage(name1, ReportThreadAllocateInfoType::Remove, 0, '\0');
    ctx.getJointThreadInfoJeallocMap()
        ->reportThreadAllocInfoForStorage(name2, ReportThreadAllocateInfoType::Remove, 0, '\0');
    ctx.getJointThreadInfoJeallocMap()
        ->reportThreadAllocInfoForStorage(name3, ReportThreadAllocateInfoType::Remove, 0, '+');
    ctx.getJointThreadInfoJeallocMap()
        ->reportThreadAllocInfoForStorage(name4, ReportThreadAllocateInfoType::Remove, 0, '+');
}
CATCH

TEST(FFIJemallocTest, JemallocThread)
try
{
    std::thread t2([&]() {
        char * a = new char[888888];
        std::thread t1([&]() {
            auto [allocated, deallocated] = JointThreadInfoJeallocMap::getPtrs();
            ASSERT(allocated != 0);
            ASSERT_EQ(*allocated, 0);
            ASSERT(deallocated != 0);
            ASSERT_EQ(*deallocated, 0);
        });
        t1.join();
        auto [allocated, deallocated] = JointThreadInfoJeallocMap::getPtrs();
        ASSERT(allocated != 0);
        ASSERT_GE(*allocated, 888888);
        ASSERT(deallocated != 0);
        delete[] a;
    });
    t2.join();
}
CATCH

TEST_F(RegionKVStoreTest, StorageBgPool)
try
{
    using namespace std::chrono_literals;
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    auto size = TiFlashTestEnv::DEFAULT_BG_POOL_SIZE;
    std::atomic_bool b;
    auto & pool = ctx.getBackgroundPool();
    auto t = pool.addTask(
        [&]() {
            auto * x = new int[1000];
            while (!b.load())
            {
                std::this_thread::sleep_for(1500ms);
            }
            delete[] x;
            return false;
        },
        false,
        5 * 60 * 1000);
    std::this_thread::sleep_for(500ms);
    JointThreadInfoJeallocMap & jm = *ctx.getJointThreadInfoJeallocMap();
    jm.recordThreadAllocInfo();

    LOG_INFO(DB::Logger::get(), "size {}", size);
    UInt64 r = TiFlashMetrics::instance().getStorageThreadMemory(TiFlashMetrics::MemoryAllocType::Alloc, "bg");
    ASSERT_GE(r, sizeof(int) * 1000);
    jm.accessStorageMap([size](const JointThreadInfoJeallocMap::AllocMap & m) {
        // There are some other bg thread pools
        ASSERT_GE(m.size(), size);
    });
    jm.accessProxyMap([](const JointThreadInfoJeallocMap::AllocMap & m) { ASSERT_EQ(m.size(), 0); });
    b.store(true);
    ctx.getBackgroundPool().removeTask(t);
}
CATCH

TEST(ProxyMode, Normal)
try
{
    ASSERT_EQ(SERVERLESS_PROXY, 0);
}
CATCH

TEST_F(RegionKVStoreTest, ParseUniPage)
try
{
    String origin = "0101020000000000000835010000000000021AE8";
    auto decode = Redact::hexStringToKey(origin.data(), origin.size());
    const char * data = decode.data();
    size_t len = decode.size();
    ASSERT_EQ(RecordKVFormat::readUInt8(data, len), UniversalPageIdFormat::RAFT_PREFIX);
    ASSERT_EQ(RecordKVFormat::readUInt8(data, len), 0x01);
    ASSERT_EQ(RecordKVFormat::readUInt8(data, len), 0x02);
    // RAFT_PREFIX LOCAL_PREFIX REGION_RAFT_PREFIX region_id RAFT_LOG_SUFFIX
    LOG_INFO(DB::Logger::get(), "region_id={}", RecordKVFormat::readUInt64(data, len));
    ASSERT_EQ(RecordKVFormat::readUInt8(data, len), 0x01);
    LOG_INFO(DB::Logger::get(), "index={}", RecordKVFormat::readUInt64(data, len));
}
CATCH

} // namespace DB::tests
