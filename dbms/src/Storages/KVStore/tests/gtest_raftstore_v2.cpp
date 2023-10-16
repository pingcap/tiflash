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

#include "kvstore_helper.h"


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

// Test several uncommitted keys with only one version.
TEST_F(RegionKVStoreTest, KVStoreSingleSnap1)
try
{
    auto ctx = TiFlashTestEnv::getGlobalContext();
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
            MockRaftStoreProxy::Cf default_cf{region_id, table_id, ColumnFamilyType::Default};
            for (HandleID h = 1; h < sst_limit; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                default_cf.insert_raw(k, value_default);
            }
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockRaftStoreProxy::Cf write_cf{region_id, table_id, ColumnFamilyType::Write};
            for (HandleID h = 1; h < sst_limit; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                if (h == uncommitted)
                    continue;
                write_cf.insert_raw(k, value_default);
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
TEST_F(RegionKVStoreTest, KVStoreSingleSnap2)
try
{
    auto ctx = TiFlashTestEnv::getGlobalContext();
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
            MockRaftStoreProxy::Cf default_cf{region_id, table_id, ColumnFamilyType::Default};
            for (uint64_t tso = 1; tso < 50; tso++)
            {
                auto [value_write, value_default] = proxy_instance->generateTiKVKeyValue(tso, 999);
                auto k = RecordKVFormat::genKey(table_id, 10, tso);
                default_cf.insert_raw(k, value_default);
            }
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockRaftStoreProxy::Cf write_cf{region_id, table_id, ColumnFamilyType::Write};
            for (uint64_t tso = 1; tso < 50; tso++)
            {
                auto [value_write, value_default] = proxy_instance->generateTiKVKeyValue(tso, 999);
                auto k = RecordKVFormat::genKey(table_id, 10, tso);
                write_cf.insert_raw(k, value_default);
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
TEST_F(RegionKVStoreTest, KVStoreSingleSnap3)
try
{
    auto ctx = TiFlashTestEnv::getGlobalContext();
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
            MockRaftStoreProxy::Cf default_cf{region_id, table_id, ColumnFamilyType::Default};
            for (HandleID h = 1; h < sst_limit; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                default_cf.insert_raw(k, value_default);
            }
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockRaftStoreProxy::Cf write_cf{region_id, table_id, ColumnFamilyType::Write};
            for (HandleID h = 1; h < sst_limit; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                write_cf.insert_raw(k, value_default);
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
TEST_F(RegionKVStoreTest, KVStoreSingleSnap4)
try
{
    auto ctx = TiFlashTestEnv::getGlobalContext();
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
        HandleID table_limit = 90;
        HandleID sst_limit = 100;
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
            MockRaftStoreProxy::Cf default_cf{region_id, table_id, ColumnFamilyType::Default};
            for (HandleID h = 1; h < sst_limit; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                default_cf.insert_raw(k, value_default);
            }
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockRaftStoreProxy::Cf write_cf{region_id, table_id, ColumnFamilyType::Write};
            for (HandleID h = 1; h < sst_limit; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                write_cf.insert_raw(k, value_default);
            }
            write_cf.finish_file(SSTFormatKind::KIND_TABLET);
            write_cf.freeze();

            std::shared_ptr<std::atomic_uint64_t> fpv = std::make_shared<std::atomic_uint64_t>(0);
            {
                fpv->store(1);
                FailPointHelper::enableFailPoint(FailPoints::force_raise_prehandle_exception, fpv);
                auto [kvr1, res]
                    = proxy_instance
                          ->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 0, 0, std::nullopt);
                // After retried.
                ASSERT_EQ(res.stats.parallels, 4);
            }
            {
                fpv->store(2);
                EXPECT_THROW(
                    proxy_instance
                        ->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf, write_cf}, 0, 0, std::nullopt),
                    Exception);
            }
        }
    }
}
CATCH

// Test if default has significantly more kvs than write cf.
TEST_F(RegionKVStoreTest, KVStoreSingleSnap5)
try
{
    auto ctx = TiFlashTestEnv::getGlobalContext();
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
            MockRaftStoreProxy::Cf default_cf{region_id, table_id, ColumnFamilyType::Default};
            for (HandleID h = 1; h < sst_limit; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                default_cf.insert_raw(k, value_default);
            }
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockRaftStoreProxy::Cf write_cf{region_id, table_id, ColumnFamilyType::Write};
            for (HandleID h = table_limit_start + 10; h < table_limit_end - 10; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                write_cf.insert_raw(k, value_default);
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
TEST_F(RegionKVStoreTest, KVStoreSingleSnap6)
try
{
    auto ctx = TiFlashTestEnv::getGlobalContext();
    proxy_instance->cluster_ver = RaftstoreVer::V2;
    proxy_instance->proxy_config_string = R"({"raftstore":{"snap-handle-pool-size":3}})";
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
            MockRaftStoreProxy::Cf default_cf{region_ids[ths_id], table_id, ColumnFamilyType::Default};
            for (HandleID h = table_limits[ths_id]; h < table_limits[ths_id + 1]; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                default_cf.insert_raw(k, value_default);
            }
            default_cf.finish_file(SSTFormatKind::KIND_TABLET);
            default_cf.freeze();
            MockRaftStoreProxy::Cf write_cf{region_ids[ths_id], table_id, ColumnFamilyType::Write};
            for (HandleID h = table_limits[ths_id]; h < table_limits[ths_id + 1]; h++)
            {
                auto k = RecordKVFormat::genKey(table_id, h, 111);
                write_cf.insert_raw(k, value_default);
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
        while (kvs.getOngoingPrehandleTaskCount() != 3)
        {
            loop += 1;
            ASSERT(loop < 30);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        DB::FailPointHelper::disableFailPoint(DB::FailPoints::pause_before_prehandle_subtask);
        for (auto && t : ths)
        {
            t.join();
        }
        ASSERT_EQ(kvs.getOngoingPrehandleTaskCount(), 0);
    }
}
CATCH

} // namespace tests
} // namespace DB
