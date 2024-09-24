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

#include <Storages/KVStore/Read/LearnerRead.h>
#include <Storages/KVStore/tests/region_kvstore_test.h>

namespace DB
{
namespace FailPoints
{
extern const char force_raise_prehandle_exception[];
extern const char pause_before_prehandle_subtask[];
extern const char force_set_sst_to_dtfile_block_size[];
} // namespace FailPoints

namespace tests
{
class RegionKVStoreV2Test : public KVStoreTestBase
{
public:
    RegionKVStoreV2Test()
    {
        log = DB::Logger::get("RegionKVStoreV2Test");
        test_path = TiFlashTestEnv::getTemporaryPath("/region_kvs_v2_test");
    }
};

TEST_F(RegionKVStoreV2Test, KVStoreSnapshotV2Extra)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    ASSERT_NE(proxy_helper->sst_reader_interfaces.fn_key, nullptr);
    UInt64 region_id = 2;
    TableID table_id;
    {
        std::string start_str = "7480000000000000FF795F720000000000FA";
        std::string end_str = "7480000000000000FF795F720380000000FF0000004003800000FF0000017FCC000000FC";
        auto start = Redact::hexStringToKey(start_str.data(), start_str.size());
        auto end = Redact::hexStringToKey(end_str.data(), end_str.size());

        initStorages();
        KVStore & kvs = getKVS();
        table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
        proxy_instance->bootstrapWithRegion(kvs, ctx.getTMTContext(), region_id, std::make_pair(start, end));
        auto kvr1 = kvs.getRegion(region_id);
        auto r1 = proxy_instance->getRegion(region_id);
    }
    KVStore & kvs = getKVS();
    {
        std::string k = "7480000000000000FF795F720380000000FF0000026303800000FF0000017801000000FCF9DE534E2797FB83";
        MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
        write_cf.insert_raw(Redact::hexStringToKey(k.data(), k.size()), "v1");
        write_cf.finish_file(SSTFormatKind::KIND_TABLET);
        write_cf.freeze();
        validateSSTGeneration(kvs, proxy_instance, region_id, write_cf, ColumnFamilyType::Write, 1, 0);
    }
}
CATCH

TEST_F(RegionKVStoreV2Test, KVStoreSnapshotV2Basic)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    ASSERT_NE(proxy_helper->sst_reader_interfaces.fn_key, nullptr);
    UInt64 region_id = 1;
    TableID table_id;
    {
        region_id = 2;
        initStorages();
        KVStore & kvs = getKVS();
        table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
        proxy_instance->bootstrapWithRegion(kvs, ctx.getTMTContext(), region_id, std::nullopt);
        auto kvr1 = kvs.getRegion(region_id);
        auto r1 = proxy_instance->getRegion(region_id);
        {
            // Shall filter out of range kvs.
            // RegionDefaultCFDataTrait will "reconstruct" TiKVKey, without table_id, it is correct since different tables ares in different regions.
            // so we may find conflict if we set the same handle_id here.
            // Shall we remove this constraint?
            auto klo = RecordKVFormat::genKey(table_id - 1, 1, 111);
            auto klo2 = RecordKVFormat::genKey(table_id - 1, 9999, 222);
            auto kro = RecordKVFormat::genKey(table_id + 1, 0, 333);
            auto kro2 = RecordKVFormat::genKey(table_id + 1, 2, 444);
            auto kin1 = RecordKVFormat::genKey(table_id, 0, 0);
            auto kin2 = RecordKVFormat::genKey(table_id, 5, 0);
            MockSSTReader::getMockSSTData().clear();
            MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
            default_cf.insert_raw(klo, "v1");
            default_cf.insert_raw(klo2, "v1");
            default_cf.insert_raw(kin1, "v1");
            default_cf.insert_raw(kin2, "v2");
            default_cf.insert_raw(kro, "v1");
            default_cf.insert_raw(kro2, "v1");
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
            write_cf.insert_raw(klo, "v1");
            write_cf.insert_raw(klo2, "v1");
            write_cf.insert_raw(kro, "v1");
            write_cf.insert_raw(kro2, "v1");
            write_cf.finish_file(SSTFormatKind::KIND_TABLET);
            write_cf.freeze();
            validateSSTGeneration(kvs, proxy_instance, region_id, default_cf, ColumnFamilyType::Default, 1, 2);
            validateSSTGeneration(kvs, proxy_instance, region_id, write_cf, ColumnFamilyType::Write, 1, 0);

            proxy_helper->sst_reader_interfaces = make_mock_sst_reader_interface();
            proxy_instance->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 0, 0, std::nullopt);
            MockRaftStoreProxy::FailCond cond;
            {
                auto [index, term]
                    = proxy_instance
                          ->rawWrite(region_id, {klo}, {"v1"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
                proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            }
            {
                auto [index, term]
                    = proxy_instance
                          ->rawWrite(region_id, {klo2}, {"v1"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
                proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            }
            {
                auto [index, term]
                    = proxy_instance
                          ->rawWrite(region_id, {kro}, {"v1"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
                proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            }
            {
                auto [index, term]
                    = proxy_instance
                          ->rawWrite(region_id, {kro2}, {"v1"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
                proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            }
        }
    }
}
CATCH

TEST_F(RegionKVStoreV2Test, KVStoreExtraDataSnapshot1)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    proxy_instance->cluster_ver = RaftstoreVer::V2;
    ASSERT_NE(proxy_helper->sst_reader_interfaces.fn_key, nullptr);
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

        // See `decodeWriteCfValue`.
        auto [value_write, value_default] = proxy_instance->generateTiKVKeyValue(111, 999);

        {
            auto k1 = RecordKVFormat::genKey(table_id, 1, 111);
            auto k2 = RecordKVFormat::genKey(table_id, 2, 111);
            MockSSTReader::getMockSSTData().clear();
            MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
            default_cf.insert_raw(k1, value_default);
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
            write_cf.insert_raw(k1, value_write);
            write_cf.insert_raw(k2, value_write);
            write_cf.finish_file(SSTFormatKind::KIND_TABLET);
            write_cf.freeze();

            auto [kvr1, res]
                = proxy_instance
                      ->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 0, 0, std::nullopt);
            ASSERT_EQ(kvr1->orphanKeysInfo().remainedKeyCount(), 1);
            ASSERT_EQ(kvr1->writeCFCount(), 1); // k2
        }
        MockRaftStoreProxy::FailCond cond;
        {
            // Add a new key to trigger another row2col transform.
            auto kvr1 = kvs.getRegion(region_id);
            auto k = RecordKVFormat::genKey(table_id, 3, 111);
            auto [index, term]
                = proxy_instance
                      ->rawWrite(region_id, {k}, {value_default}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            auto [index2, term2]
                = proxy_instance
                      ->rawWrite(region_id, {k}, {value_write}, {WriteCmdType::Put}, {ColumnFamilyType::Write});
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index2);
            ASSERT_EQ(kvr1->orphanKeysInfo().remainedKeyCount(), 1);
            ASSERT_EQ(kvr1->writeCFCount(), 1); // k2
            UNUSED(term);
            UNUSED(term2);
        }
        {
            // A normal write to "save" the orphan key.
            auto k2 = RecordKVFormat::genKey(table_id, 2, 111);
            auto kvr1 = kvs.getRegion(region_id);
            auto [index, term]
                = proxy_instance
                      ->rawWrite(region_id, {k2}, {value_default}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            // After applied this log, the write record is not orphan any more.
            ASSERT_EQ(kvr1->writeCFCount(), 0);
            auto [index2, term2]
                = proxy_instance
                      ->rawWrite(region_id, {k2}, {value_write}, {WriteCmdType::Put}, {ColumnFamilyType::Write});
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index2);
            ASSERT_EQ(kvr1->writeCFCount(), 0);
            ASSERT_EQ(kvr1->orphanKeysInfo().remainedKeyCount(), 0);
            UNUSED(term);
            UNUSED(term2);
        }
        {
            // An orphan key in normal write will still trigger a hard error.
            // TODO Enable this test again when we fix `RegionData::readDataByWriteIt`.

            // auto k = RecordKVFormat::genKey(table_id, 4, 111);
            // auto [index, term2] = proxy_instance->rawWrite(region_id, {k}, {value_write}, {WriteCmdType::Put}, {ColumnFamilyType::Write});
            // UNUSED(term2);
            // EXPECT_THROW(proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index), Exception);
        }
    }
}
CATCH

TEST_F(RegionKVStoreV2Test, KVStoreExtraDataSnapshot2)
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

        // See `decodeWriteCfValue`.
        auto && [value_write, value_default] = proxy_instance->generateTiKVKeyValue(111, 999);

        {
            auto k1 = RecordKVFormat::genKey(table_id, 1, 111);
            MockSSTReader::getMockSSTData().clear();
            MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
            write_cf.insert_raw(k1, value_write);
            write_cf.finish_file(SSTFormatKind::KIND_TABLET);
            write_cf.freeze();

            auto [kvr1, res]
                = proxy_instance
                      ->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 0, 0, std::nullopt);
            ASSERT_FALSE(kvs.prehandling_trace.hasTask(region_id));
            ASSERT_EQ(kvr1->orphanKeysInfo().remainedKeyCount(), 1);
        }
        {
            auto k2 = RecordKVFormat::genKey(table_id, 2, 111);
            MockSSTReader::getMockSSTData().clear();
            MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
            write_cf.insert_raw(k2, value_write);
            write_cf.finish_file(SSTFormatKind::KIND_TABLET);
            write_cf.freeze();

            auto [kvr1, res]
                = proxy_instance->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 0, 0, 10);
            // Every snapshot contains a full copy of this region. So we will drop all orphan keys in the previous region.
            ASSERT_EQ(kvr1->orphanKeysInfo().remainedKeyCount(), 1);
        }
        MockRaftStoreProxy::FailCond cond;
        {
            auto k3 = RecordKVFormat::genKey(table_id, 3, 111);
            auto kvr1 = kvs.getRegion(region_id);
            proxy_instance->rawWrite(
                region_id,
                {k3, k3},
                {value_default, value_write},
                {WriteCmdType::Put, WriteCmdType::Put},
                {ColumnFamilyType::Default, ColumnFamilyType::Write},
                8);
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, 8);

            auto k4 = RecordKVFormat::genKey(table_id, 4, 111);
            proxy_instance->rawWrite(
                region_id,
                {k4, k4},
                {value_default, value_write},
                {WriteCmdType::Put, WriteCmdType::Put},
                {ColumnFamilyType::Default, ColumnFamilyType::Write},
                10);
            // Remaining orphan keys of k2.
            EXPECT_THROW(proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, 10), Exception);
        }
        {
            auto k5 = RecordKVFormat::genKey(table_id, 5, 111);
            MockSSTReader::getMockSSTData().clear();
            MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
            write_cf.insert_raw(k5, value_write);
            write_cf.finish_file(SSTFormatKind::KIND_TABLET);
            write_cf.freeze();

            auto [kvr1, res]
                = proxy_instance->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 15, 0, 20);
            ASSERT_EQ(kvr1->orphanKeysInfo().remainedKeyCount(), 1);
        }
        {
            auto k6 = RecordKVFormat::genKey(table_id, 6, 111);
            auto kvr1 = kvs.getRegion(region_id);
            auto r1 = proxy_instance->getRegion(region_id);

            // Trigger a row2col.
            auto && [req, res] = MockRaftStoreProxy::composeCompactLog(r1, 10);
            proxy_instance->adminCommand(region_id, std::move(req), std::move(res), 20);
            EXPECT_THROW(proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, 20), Exception);
        }
    }
}
CATCH

TEST_F(RegionKVStoreV2Test, KVStoreExtraDataSnapshot)
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

        // See `decodeWriteCfValue`.
        auto && [value_write, value_default] = proxy_instance->generateTiKVKeyValue(111, 999);

        {
            auto k1 = RecordKVFormat::genKey(table_id, 1, 111);
            MockSSTReader::getMockSSTData().clear();
            MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
            write_cf.insert_raw(k1, value_write);
            write_cf.finish_file(SSTFormatKind::KIND_TABLET);
            write_cf.freeze();

            auto [kvr1, res]
                = proxy_instance
                      ->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 0, 0, std::nullopt);
            ASSERT_FALSE(kvs.prehandling_trace.hasTask(region_id));
            ASSERT_EQ(kvr1->orphanKeysInfo().remainedKeyCount(), 1);
        }
        {
            auto k2 = RecordKVFormat::genKey(table_id, 2, 111);
            MockSSTReader::getMockSSTData().clear();
            MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
            write_cf.insert_raw(k2, value_write);
            write_cf.finish_file(SSTFormatKind::KIND_TABLET);
            write_cf.freeze();

            auto [kvr1, res]
                = proxy_instance->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 0, 0, 10);
            // Every snapshot contains a full copy of this region. So we will drop all orphan keys in the previous region.
            ASSERT_EQ(kvr1->orphanKeysInfo().remainedKeyCount(), 1);
        }
        MockRaftStoreProxy::FailCond cond;
        {
            auto k3 = RecordKVFormat::genKey(table_id, 3, 111);
            auto kvr1 = kvs.getRegion(region_id);
            proxy_instance->rawWrite(
                region_id,
                {k3, k3},
                {value_default, value_write},
                {WriteCmdType::Put, WriteCmdType::Put},
                {ColumnFamilyType::Default, ColumnFamilyType::Write},
                8);
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, 8);

            auto k4 = RecordKVFormat::genKey(table_id, 4, 111);
            proxy_instance->rawWrite(
                region_id,
                {k4, k4},
                {value_default, value_write},
                {WriteCmdType::Put, WriteCmdType::Put},
                {ColumnFamilyType::Default, ColumnFamilyType::Write},
                10);
            // Remaining orphan keys of k2.
            EXPECT_THROW(proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, 10), Exception);
        }
        {
            auto k5 = RecordKVFormat::genKey(table_id, 5, 111);
            MockSSTReader::getMockSSTData().clear();
            MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
            write_cf.insert_raw(k5, value_write);
            write_cf.finish_file(SSTFormatKind::KIND_TABLET);
            write_cf.freeze();

            auto [kvr1, res]
                = proxy_instance->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 15, 0, 20);
            ASSERT_EQ(kvr1->orphanKeysInfo().remainedKeyCount(), 1);
        }
        {
            auto k6 = RecordKVFormat::genKey(table_id, 6, 111);
            auto kvr1 = kvs.getRegion(region_id);
            auto r1 = proxy_instance->getRegion(region_id);

            // Trigger a row2col.
            auto && [req, res] = MockRaftStoreProxy::composeCompactLog(r1, 10);
            proxy_instance->adminCommand(region_id, std::move(req), std::move(res), 20);
            EXPECT_THROW(proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, 20), Exception);
        }
    }
}
CATCH

// Test if active cancel from proxy.
TEST_F(RegionKVStoreV2Test, KVStoreSingleSnapCancel)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    proxy_instance->cluster_ver = RaftstoreVer::V2;
    ASSERT_NE(proxy_helper->sst_reader_interfaces.fn_key, nullptr);
    ASSERT_NE(proxy_helper->fn_get_config_json, nullptr);
    UInt64 region_id = 1;
    TableID table_id;
    FailPointHelper::enableFailPoint(FailPoints::force_set_parallel_prehandle_threshold, static_cast<size_t>(0));
    FailPointHelper::enableFailPoint(FailPoints::force_set_sst_to_dtfile_block_size, static_cast<size_t>(1));
    SCOPE_EXIT({ FailPointHelper::disableFailPoint("force_set_sst_to_dtfile_block_size"); });
    SCOPE_EXIT({ FailPointHelper::disableFailPoint("force_set_parallel_prehandle_threshold"); });
    {
        region_id = 2;
        initStorages();
        KVStore & kvs = getKVS();
        HandleID table_limit = 40;
        HandleID sst_limit = 40;
        table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
        auto start = RecordKVFormat::genKey(table_id, 0);
        auto end = RecordKVFormat::genKey(table_id, table_limit);
        proxy_instance->bootstrapWithRegion(
            kvs,
            ctx.getTMTContext(),
            region_id,
            std::make_pair(start.toString(), end.toString()));
        auto r1 = proxy_instance->getRegion(region_id);

        auto [value_write, value_default] = proxy_instance->generateTiKVKeyValue(111, 999);
        auto kkk = RecordKVFormat::decodeWriteCfValue(TiKVValue::copyFrom(value_write));
        {
            MockSSTReader::getMockSSTData().clear();
            MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
            for (HandleID h = 1; h < sst_limit; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                default_cf.insert_raw(k, value_default);
            }
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
            for (HandleID h = 1; h < sst_limit; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                write_cf.insert_raw(k, value_write);
            }
            write_cf.finish_file(SSTFormatKind::KIND_TABLET);
            write_cf.freeze();

            auto sp = SyncPointCtl::enableInScope("before_SSTFilesToDTFilesOutputStream::handle_one");
            std::thread t([&]() {
                auto [kvr1, res]
                    = proxy_instance
                          ->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 0, 0, std::nullopt);
            });
            sp.waitAndPause();
            kvs.abortPreHandleSnapshot(region_id, ctx.getTMTContext());
            sp.next();
            sp.disable();
            t.join();
        }
    }
}
CATCH

// Test several uncommitted keys with only one version.
TEST_F(RegionKVStoreV2Test, KVStoreSingleSnap1)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    proxy_instance->cluster_ver = RaftstoreVer::V2;
    ASSERT_NE(proxy_helper->sst_reader_interfaces.fn_key, nullptr);
    ASSERT_NE(proxy_helper->fn_get_config_json, nullptr);
    UInt64 region_id = 1;
    TableID table_id;
    FailPointHelper::enableFailPoint(FailPoints::force_set_parallel_prehandle_threshold, static_cast<size_t>(0));
    SCOPE_EXIT({ FailPointHelper::disableFailPoint("force_set_parallel_prehandle_threshold"); });
    {
        region_id = 2;
        initStorages();
        KVStore & kvs = getKVS();
        HandleID table_limit = 20;
        HandleID sst_limit = 30;
        HandleID uncommitted = 15;
        table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
        auto start = RecordKVFormat::genKey(table_id, 0);
        auto end = RecordKVFormat::genKey(table_id, table_limit);
        proxy_instance->bootstrapWithRegion(
            kvs,
            ctx.getTMTContext(),
            region_id,
            std::make_pair(start.toString(), end.toString()));
        auto r1 = proxy_instance->getRegion(region_id);

        auto [value_write, value_default] = proxy_instance->generateTiKVKeyValue(111, 999);
        {
            MockSSTReader::getMockSSTData().clear();
            MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
            for (HandleID h = 1; h < sst_limit; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                default_cf.insert_raw(k, value_default);
            }
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
            for (HandleID h = 1; h < sst_limit; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                if (h == uncommitted)
                    continue;
                write_cf.insert_raw(k, value_write);
            }
            write_cf.finish_file(SSTFormatKind::KIND_TABLET);
            write_cf.freeze();

            auto [kvr1, res]
                = proxy_instance
                      ->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 0, 0, std::nullopt);
            ASSERT_EQ(res.stats.write_cf_keys, 18); // table_limit - 1 - 1(uncommitted)
            // No extra read, otherwise mergeDataFrom will also fire.
            ASSERT_EQ(res.stats.write_cf_keys + 1, res.stats.default_cf_keys);
            ASSERT_EQ(res.stats.parallels, 4);
        }
        // Switch V1 to test if there are duplicated keys.
        proxy_instance->cluster_ver = RaftstoreVer::V1;
        {
            auto k = RecordKVFormat::genKey(table_id, uncommitted, 111);
            auto [index, term]
                = proxy_instance
                      ->rawWrite(region_id, {k}, {value_default}, {WriteCmdType::Put}, {ColumnFamilyType::Write});
            MockRaftStoreProxy::FailCond cond;
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
        }
        {
            auto k = RecordKVFormat::genKey(table_id, uncommitted, 111);
            auto [index, term] = proxy_instance->rawWrite(
                region_id,
                {k, k},
                {value_default, value_default},
                {WriteCmdType::Put, WriteCmdType::Put},
                {ColumnFamilyType::Default, ColumnFamilyType::Write});
            MockRaftStoreProxy::FailCond cond;
            EXPECT_THROW(proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index), Exception);
        }
    }
}
CATCH

// Test if there is only one pk with may versions.
TEST_F(RegionKVStoreV2Test, KVStoreSingleSnap2)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    proxy_instance->cluster_ver = RaftstoreVer::V2;
    ASSERT_NE(proxy_helper->sst_reader_interfaces.fn_key, nullptr);
    ASSERT_NE(proxy_helper->fn_get_config_json, nullptr);
    UInt64 region_id = 1;
    TableID table_id;
    FailPointHelper::enableFailPoint(FailPoints::force_set_parallel_prehandle_threshold, static_cast<size_t>(0));
    SCOPE_EXIT({ FailPointHelper::disableFailPoint("force_set_parallel_prehandle_threshold"); });
    {
        region_id = 2;
        initStorages();
        KVStore & kvs = getKVS();
        table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
        auto start = RecordKVFormat::genKey(table_id, 0);
        auto end = RecordKVFormat::genKey(table_id, 20);
        proxy_instance->bootstrapWithRegion(
            kvs,
            ctx.getTMTContext(),
            region_id,
            std::make_pair(start.toString(), end.toString()));
        auto r1 = proxy_instance->getRegion(region_id);

        {
            MockSSTReader::getMockSSTData().clear();
            MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
            for (uint64_t tso = 1; tso < 50; tso++)
            {
                auto [value_write, value_default] = proxy_instance->generateTiKVKeyValue(tso, 999);
                auto k = RecordKVFormat::genKey(table_id, 10, tso);
                default_cf.insert_raw(k, value_default);
            }
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
            for (uint64_t tso = 1; tso < 50; tso++)
            {
                auto [value_write, value_default] = proxy_instance->generateTiKVKeyValue(tso, 999);
                auto k = RecordKVFormat::genKey(table_id, 10, tso);
                write_cf.insert_raw(k, value_write);
            }
            write_cf.finish_file(SSTFormatKind::KIND_TABLET);
            write_cf.freeze();

            auto [kvr1, res]
                = proxy_instance
                      ->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 0, 0, std::nullopt);
            ASSERT_EQ(res.stats.write_cf_keys, 49); // There are 49 versions.
            ASSERT_EQ(res.stats.parallels, 4);
            ASSERT_EQ(
                res.stats.max_split_write_cf_keys,
                res.stats.write_cf_keys); // Only one split can handle all write keys.
        }
    }
}
CATCH

// Test if there are too many untrimmed data.
TEST_F(RegionKVStoreV2Test, KVStoreSingleSnap3)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    proxy_instance->cluster_ver = RaftstoreVer::V2;
    ASSERT_NE(proxy_helper->sst_reader_interfaces.fn_key, nullptr);
    ASSERT_NE(proxy_helper->fn_get_config_json, nullptr);
    UInt64 region_id = 1;
    TableID table_id;
    FailPointHelper::enableFailPoint(FailPoints::force_set_parallel_prehandle_threshold, static_cast<size_t>(0));
    SCOPE_EXIT({ FailPointHelper::disableFailPoint("force_set_parallel_prehandle_threshold"); });
    {
        region_id = 2;
        initStorages();
        KVStore & kvs = getKVS();
        HandleID table_limit_start = 30;
        HandleID table_limit_end = 32;
        HandleID sst_limit = 100;
        table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
        auto start = RecordKVFormat::genKey(table_id, table_limit_start);
        auto end = RecordKVFormat::genKey(table_id, table_limit_end);
        proxy_instance->bootstrapWithRegion(
            kvs,
            ctx.getTMTContext(),
            region_id,
            std::make_pair(start.toString(), end.toString()));
        auto r1 = proxy_instance->getRegion(region_id);

        auto [value_write, value_default] = proxy_instance->generateTiKVKeyValue(111, 999);
        {
            MockSSTReader::getMockSSTData().clear();
            MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
            for (HandleID h = 1; h < sst_limit; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                default_cf.insert_raw(k, value_default);
            }
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
            for (HandleID h = 1; h < sst_limit; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                write_cf.insert_raw(k, value_write);
            }
            write_cf.finish_file(SSTFormatKind::KIND_TABLET);
            write_cf.freeze();

            auto [kvr1, res]
                = proxy_instance
                      ->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 0, 0, std::nullopt);
            // There must be some parallel which actually reads no write cf.
            ASSERT_EQ(res.stats.write_cf_keys, 2); // table_limit_end - table_limit_start
            ASSERT_EQ(res.stats.parallels, 4);
        }
    }
}
CATCH

// Test if one subtask throws.
// TEST_F(RegionKVStoreV2Test, KVStoreSingleSnap4)
// try
// {
//     auto & ctx = TiFlashTestEnv::getGlobalContext();
//     proxy_instance->cluster_ver = RaftstoreVer::V2;
//     ASSERT_NE(proxy_helper->sst_reader_interfaces.fn_key, nullptr);
//     ASSERT_NE(proxy_helper->fn_get_config_json, nullptr);
//     UInt64 region_id = 1;
//     TableID table_id;
//     FailPointHelper::enableFailPoint(FailPoints::force_set_parallel_prehandle_threshold, static_cast<size_t>(0));
//     SCOPE_EXIT({ FailPointHelper::disableFailPoint("force_set_parallel_prehandle_threshold"); });
//     {
//         region_id = 2;
//         initStorages();
//         KVStore & kvs = getKVS();
//         HandleID table_limit = 90;
//         HandleID sst_limit = 100;
//         table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
//         auto start = RecordKVFormat::genKey(table_id, 0);
//         auto end = RecordKVFormat::genKey(table_id, table_limit);
//         proxy_instance->bootstrapWithRegion(
//             kvs,
//             ctx.getTMTContext(),
//             region_id,
//             std::make_pair(start.toString(), end.toString()));
//         auto r1 = proxy_instance->getRegion(region_id);

//         auto [value_write, value_default] = proxy_instance->generateTiKVKeyValue(111, 999);
//         {
//             MockSSTReader::getMockSSTData().clear();
//             MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
//             for (HandleID h = 1; h < sst_limit; h++)
//             {
//                 auto k = RecordKVFormat::genKey(table_id, h, 111);
//                 default_cf.insert_raw(k, value_default);
//             }
//             default_cf.finish_file(SSTFormatKind::KIND_TABLET);
//             default_cf.freeze();
//             MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
//             for (HandleID h = 1; h < sst_limit; h++)
//             {
//                 auto k = RecordKVFormat::genKey(table_id, h, 111);
//                 write_cf.insert_raw(k, value_write);
//             }
//             write_cf.finish_file(SSTFormatKind::KIND_TABLET);
//             write_cf.freeze();

//             auto fpv = std::make_shared<std::atomic_uint64_t>(0);
//             FailPointHelper::enableFailPoint(FailPoints::force_raise_prehandle_exception, fpv);
//             SCOPE_EXIT({ FailPointHelper::disableFailPoint("force_raise_prehandle_exception"); });
//             {
//                 LOG_INFO(log, "Try decode when meet the first ErrUpdateSchema");
//                 fpv->store(1);
//                 auto [kvr1, res]
//                     = proxy_instance
//                           ->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 0, 0, std::nullopt);
//                 // After retried.
//                 ASSERT_EQ(res.stats.parallels, 4);
//             }
//             {
//                 LOG_INFO(log, "Try decode when always meet ErrUpdateSchema");
//                 fpv->store(2);
//                 EXPECT_THROW(
//                     proxy_instance
//                         ->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 0, 0, std::nullopt),
//                     Exception);
//             }
//         }
//     }
// }
// CATCH

// Test if default has significantly more kvs than write cf.
TEST_F(RegionKVStoreV2Test, KVStoreSingleSnap5)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    proxy_instance->cluster_ver = RaftstoreVer::V2;
    ASSERT_NE(proxy_helper->sst_reader_interfaces.fn_key, nullptr);
    ASSERT_NE(proxy_helper->fn_get_config_json, nullptr);
    UInt64 region_id = 1;
    TableID table_id;
    FailPointHelper::enableFailPoint(FailPoints::force_set_parallel_prehandle_threshold, static_cast<size_t>(0));
    FailPointHelper::enableFailPoint(FailPoints::force_set_sst_to_dtfile_block_size, static_cast<size_t>(20));
    SCOPE_EXIT({ FailPointHelper::disableFailPoint("force_set_parallel_prehandle_threshold"); });
    SCOPE_EXIT({ FailPointHelper::disableFailPoint("force_set_sst_to_dtfile_block_size"); });
    initStorages();
    KVStore & kvs = getKVS();
    {
        region_id = 2;
        HandleID table_limit_start = 100;
        HandleID table_limit_end = 1900;
        HandleID sst_limit = 2000;
        table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
        auto start = RecordKVFormat::genKey(table_id, table_limit_start);
        auto end = RecordKVFormat::genKey(table_id, table_limit_end);
        proxy_instance->bootstrapWithRegion(
            kvs,
            ctx.getTMTContext(),
            region_id,
            std::make_pair(start.toString(), end.toString()));
        auto r1 = proxy_instance->getRegion(region_id);

        auto [value_write, value_default] = proxy_instance->generateTiKVKeyValue(111, 999);
        {
            MockSSTReader::getMockSSTData().clear();
            MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
            for (HandleID h = 1; h < sst_limit; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                default_cf.insert_raw(k, value_default);
            }
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
            for (HandleID h = table_limit_start + 10; h < table_limit_end - 10; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                write_cf.insert_raw(k, value_write);
            }
            write_cf.finish_file(SSTFormatKind::KIND_TABLET);
            write_cf.freeze();

            // It could throw "got duplicate key"
            ASSERT_NO_THROW(
                proxy_instance
                    ->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 0, 0, std::nullopt));
        }
    }
}
CATCH

// Test if parallel limit is reached.
TEST_F(RegionKVStoreV2Test, KVStoreSingleSnap6)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    proxy_instance->cluster_ver = RaftstoreVer::V2;
    proxy_instance->proxy_config_string = R"({"raftstore":{"snap-handle-pool-size":3},"server":{"engine-addr":"123"}})";
    KVStore & kvs = getKVS();
    kvs.fetchProxyConfig(proxy_helper.get());
    ASSERT_NE(proxy_helper->sst_reader_interfaces.fn_key, nullptr);
    ASSERT_NE(proxy_helper->fn_get_config_json, nullptr);
    UInt64 region_id = 1;
    TableID table_id;
    FailPointHelper::enableFailPoint(FailPoints::force_set_parallel_prehandle_threshold, static_cast<size_t>(0));
    SCOPE_EXIT({ FailPointHelper::disableFailPoint("force_set_parallel_prehandle_threshold"); });
    {
        initStorages();
        std::vector<UInt64> region_ids = {2, 3, 4};
        region_id = region_ids[0];
        std::vector<HandleID> table_limits = {0, 90, 180, 270};
        table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());

        proxy_instance->bootstrapWithRegion(
            kvs,
            ctx.getTMTContext(),
            region_id,
            std::make_pair(
                RecordKVFormat::genKey(table_id, table_limits[0]).toString(),
                RecordKVFormat::genKey(table_id, table_limits[1]).toString()));

        auto ranges = std::vector<std::pair<std::string, std::string>>();
        for (size_t i = 1; i + 1 < table_limits.size(); i++)
        {
            ranges.push_back(std::make_pair(
                RecordKVFormat::genKey(table_id, table_limits[i]).toString(),
                RecordKVFormat::genKey(table_id, table_limits[i + 1]).toString()));
        }
        proxy_instance->debugAddRegions(
            kvs,
            ctx.getTMTContext(),
            std::vector(region_ids.begin() + 1, region_ids.end()),
            std::move(ranges));
        auto r1 = proxy_instance->getRegion(region_id);

        MockSSTReader::getMockSSTData().clear();
        DB::FailPointHelper::enablePauseFailPoint(DB::FailPoints::pause_before_prehandle_subtask, 100);
        std::vector<std::thread> ths;
        auto runId = [&](size_t ths_id) {
            auto [value_write, value_default] = proxy_instance->generateTiKVKeyValue(111, 999);
            MockSSTGenerator default_cf{region_ids[ths_id], table_id, ColumnFamilyType::Default};
            for (HandleID h = table_limits[ths_id]; h < table_limits[ths_id + 1]; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                default_cf.insert_raw(k, value_default);
            }
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockSSTGenerator write_cf{region_ids[ths_id], table_id, ColumnFamilyType::Write};
            for (HandleID h = table_limits[ths_id]; h < table_limits[ths_id + 1]; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                write_cf.insert_raw(k, value_write);
            }
            write_cf.finish_file(SSTFormatKind::KIND_TABLET);
            write_cf.freeze();

            {
                auto [kvr1, res] = proxy_instance->snapshot(
                    kvs,
                    ctx.getTMTContext(),
                    region_ids[ths_id],
                    {default_cf, write_cf},
                    0,
                    0,
                    std::nullopt);
            }
        };
        ths.push_back(std::thread(runId, 0));
        std::this_thread::sleep_for(std::chrono::milliseconds(300));

        ASSERT_EQ(kvs.getOngoingPrehandleTaskCount(), 1);
        for (size_t ths_id = 1; ths_id < region_ids.size(); ths_id++)
        {
            ths.push_back(std::thread(runId, ths_id));
        }

        auto loop = 0;
        // All threads can be prehandled.
        while (kvs.getOngoingPrehandleTaskCount() != 3)
        {
            loop += 1;
            ASSERT_TRUE(loop < 30);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        ASSERT_EQ(kvs.prehandling_trace.ongoing_prehandle_subtask_count.load(), 3);
        DB::FailPointHelper::disableFailPoint(DB::FailPoints::pause_before_prehandle_subtask);
        for (auto && t : ths)
        {
            t.join();
        }
        ASSERT_EQ(kvs.getOngoingPrehandleTaskCount(), 0);
    }
}
CATCH

TEST_F(RegionKVStoreV2Test, KVStoreSingleSnap7)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    proxy_instance->cluster_ver = RaftstoreVer::V2;
    ASSERT_NE(proxy_helper->sst_reader_interfaces.fn_key, nullptr);
    ASSERT_NE(proxy_helper->fn_get_config_json, nullptr);
    UInt64 region_id = 1;
    TableID table_id;
    FailPointHelper::enableFailPoint(FailPoints::force_set_parallel_prehandle_threshold, static_cast<size_t>(0));
    SCOPE_EXIT({ FailPointHelper::disableFailPoint("force_set_parallel_prehandle_threshold"); });
    initStorages();
    KVStore & kvs = getKVS();
    {
        region_id = 2;
        RegionID region_id2 = 3;
        HandleID table_limit_start = 100;
        HandleID table_limit_end = 1900;
        HandleID sst_limit = 2000;
        table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
        auto start = RecordKVFormat::genKey(table_id, table_limit_start);
        auto end = RecordKVFormat::genKey(table_id, table_limit_end);
        auto start2 = RecordKVFormat::genKey(table_id, table_limit_start + sst_limit);
        auto end2 = RecordKVFormat::genKey(table_id, table_limit_end + 2 * sst_limit);
        proxy_instance->bootstrapWithRegion(
            kvs,
            ctx.getTMTContext(),
            region_id,
            std::make_pair(start.toString(), end.toString()));
        auto r1 = proxy_instance->getRegion(region_id);
        proxy_instance->debugAddRegions(
            kvs,
            ctx.getTMTContext(),
            {region_id2},
            {std::make_pair(start2.toString(), end2.toString())});

        auto [value_write, value_default] = proxy_instance->generateTiKVKeyValue(111, 999);
        {
            MockSSTReader::getMockSSTData().clear();
            MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
            for (HandleID h = 1; h < sst_limit; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                default_cf.insert_raw(k, value_default);
            }
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockSSTGenerator write_cf{region_id, table_id, ColumnFamilyType::Write};
            for (HandleID h = table_limit_start + 10; h < table_limit_end - 10; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                write_cf.insert_raw(k, value_write);
            }
            write_cf.finish_file(SSTFormatKind::KIND_TABLET);
            write_cf.freeze();

            auto sp = SyncPointCtl::enableInScope("before_MockRaftStoreProxy::snapshot_prehandle");
            std::thread t1([&]() {
                ASSERT_NO_THROW(
                    proxy_instance
                        ->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 0, 0, std::nullopt));
            });
            sp.waitAndPause();
            std::thread t2([&]() {
                ASSERT_NO_THROW(
                    proxy_instance
                        ->snapshot(kvs, ctx.getTMTContext(), region_id2, {default_cf, write_cf}, 0, 0, std::nullopt));
            });
            sp.next();
            sp.disable();
            t1.join();
            t2.join();
        }
    }
}
CATCH

} // namespace tests
} // namespace DB
