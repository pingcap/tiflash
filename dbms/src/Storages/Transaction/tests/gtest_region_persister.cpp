// Copyright 2022 PingCAP, Ltd.
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

#include <Common/FailPoint.h>
#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Context.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/PathPool.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionManager.h>
#include <Storages/Transaction/RegionPersister.h>
#include <Storages/Transaction/TiKVRecordFormat.h>
#include <Storages/Transaction/tests/region_helper.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <common/logger_useful.h>
#include <common/types.h>

#include <ext/scope_guard.h>

namespace DB
{

namespace tests
{

static ::testing::AssertionResult PeerCompare(
    const char * lhs_expr,
    const char * rhs_expr,
    const metapb::Peer & lhs,
    const metapb::Peer & rhs)
{
    if (lhs.id() == rhs.id() && lhs.role() == rhs.role())
        return ::testing::AssertionSuccess();
    return ::testing::internal::EqFailure(lhs_expr, rhs_expr, lhs.ShortDebugString(), rhs.ShortDebugString(), false);
}
#define ASSERT_PEER_EQ(val1, val2) ASSERT_PRED_FORMAT2(::DB::tests::PeerCompare, val1, val2)

static ::testing::AssertionResult RegionCompare(
    const char * lhs_expr,
    const char * rhs_expr,
    const DB::Region & lhs,
    const DB::Region & rhs)
{
    if (lhs == rhs)
        return ::testing::AssertionSuccess();
    return ::testing::internal::EqFailure(lhs_expr, rhs_expr, lhs.toString(), rhs.toString(), false);
}
#define ASSERT_REGION_EQ(val1, val2) ASSERT_PRED_FORMAT2(::DB::tests::RegionCompare, val1, val2)

class RegionSeriTest : public ::testing::Test
{
public:
    RegionSeriTest()
        : dir_path(TiFlashTestEnv::getTemporaryPath("RegionSeriTest"))
    {
    }

    void SetUp() override
    {
        TiFlashTestEnv::tryRemovePath(dir_path, /*recreate=*/true);
    }

    std::string dir_path;
};

TEST_F(RegionSeriTest, peer)
try
{
    auto peer = createPeer(100, true);
    const auto path = dir_path + "/peer.test";
    WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
    auto size = writeBinary2(peer, write_buf);
    write_buf.next();
    write_buf.sync();
    ASSERT_EQ(size, Poco::File(path).getSize());

    ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
    auto new_peer = readPeer(read_buf);
    ASSERT_PEER_EQ(new_peer, peer);
}
CATCH

TEST_F(RegionSeriTest, RegionInfo)
try
{
    auto region_info = createRegionInfo(233, "", "");
    const auto path = dir_path + "/region_info.test";
    WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
    auto size = writeBinary2(region_info, write_buf);
    write_buf.next();
    write_buf.sync();
    ASSERT_EQ(size, (size_t)Poco::File(path).getSize());

    ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
    auto new_region_info = readRegion(read_buf);
    ASSERT_EQ(new_region_info.id(), region_info.id());
    ASSERT_EQ(new_region_info.start_key(), region_info.start_key());
    ASSERT_EQ(new_region_info.end_key(), region_info.end_key());
    ASSERT_EQ(new_region_info.peers_size(), region_info.peers_size());
    for (int i = 0; i < new_region_info.peers_size(); ++i)
        ASSERT_PEER_EQ(new_region_info.peers(i), region_info.peers(i));
}
CATCH

TEST_F(RegionSeriTest, RegionMeta)
try
{
    RegionMeta meta = createRegionMeta(888, 66);
    const auto path = dir_path + "/meta.test";
    WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
    auto size = std::get<0>(meta.serialize(write_buf));
    write_buf.next();
    write_buf.sync();
    ASSERT_EQ(size, (size_t)Poco::File(path).getSize());

    ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
    auto new_meta = RegionMeta::deserialize(read_buf);
    ASSERT_EQ(new_meta, meta);
}
CATCH

TEST_F(RegionSeriTest, Region)
try
{
    TableID table_id = 100;
    auto region = std::make_shared<Region>(createRegionMeta(1001, table_id));
    TiKVKey key = RecordKVFormat::genKey(table_id, 323, 9983);
    region->insert("default", TiKVKey::copyFrom(key), TiKVValue("value1"));
    region->insert("write", TiKVKey::copyFrom(key), RecordKVFormat::encodeWriteCfValue('P', 0));
    region->insert("lock", TiKVKey::copyFrom(key), RecordKVFormat::encodeLockCfValue('P', "", 0, 0));

    const auto path = dir_path + "/region.test";
    WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
    size_t region_ser_size = std::get<0>(region->serialize(write_buf));
    write_buf.next();
    write_buf.sync();
    ASSERT_EQ(region_ser_size, (size_t)Poco::File(path).getSize());

    ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
    auto new_region = Region::deserialize(read_buf);
    ASSERT_REGION_EQ(*new_region, *region);
}
CATCH

TEST_F(RegionSeriTest, RegionStat)
try
{
    RegionPtr region = nullptr;
    TableID table_id = 100;
    {
        raft_serverpb::RaftApplyState apply_state;
        raft_serverpb::RegionLocalState region_state;
        {
            apply_state.set_applied_index(6671);
            apply_state.mutable_truncated_state()->set_index(6672);
            apply_state.mutable_truncated_state()->set_term(6673);

            *region_state.mutable_region() = createRegionInfo(1001, RecordKVFormat::genKey(table_id, 0), RecordKVFormat::genKey(table_id, 300));
            region_state.mutable_merge_state()->set_commit(888);
            region_state.mutable_merge_state()->set_min_index(777);
            *region_state.mutable_merge_state()->mutable_target() = createRegionInfo(1111, RecordKVFormat::genKey(table_id, 300), RecordKVFormat::genKey(table_id, 400));
        };
        region = std::make_shared<Region>(RegionMeta(
            createPeer(31, true),
            apply_state,
            5,
            region_state));
    }

    TiKVKey key = RecordKVFormat::genKey(table_id, 323, 9983);
    region->insert("default", TiKVKey::copyFrom(key), TiKVValue("value1"));
    region->insert("write", TiKVKey::copyFrom(key), RecordKVFormat::encodeWriteCfValue('P', 0));
    region->insert("lock", TiKVKey::copyFrom(key), RecordKVFormat::encodeLockCfValue('P', "", 0, 0));

    const auto path = dir_path + "/region_state.test";
    WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
    size_t region_ser_size = std::get<0>(region->serialize(write_buf));
    write_buf.next();

    ASSERT_EQ(region_ser_size, (size_t)Poco::File(path).getSize());
    ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
    auto new_region = Region::deserialize(read_buf);
    ASSERT_EQ(*new_region, *region);
}
CATCH

class RegionPersisterTest
    : public ::testing::Test
    , public testing::WithParamInterface<PageStorageRunMode>
{
public:
    RegionPersisterTest()
        : dir_path(TiFlashTestEnv::getTemporaryPath("/region_persister_test"))
        , log(Logger::get())
    {
        test_run_mode = GetParam();
        old_run_mode = test_run_mode;
    }

    static void SetUpTestCase() {}

    void SetUp() override
    {
        TiFlashTestEnv::tryRemovePath(dir_path);
        auto & global_ctx = DB::tests::TiFlashTestEnv::getGlobalContext();
        old_run_mode = global_ctx.getPageStorageRunMode();
        global_ctx.setPageStorageRunMode(test_run_mode);

        auto path_capacity = global_ctx.getPathCapacity();
        auto provider = global_ctx.getFileProvider();
        Strings main_data_paths{dir_path};
        mocked_path_pool = std::make_unique<PathPool>(
            main_data_paths,
            main_data_paths,
            /*kvstore_paths=*/Strings{},
            path_capacity,
            provider);
        global_ctx.tryReleaseWriteNodePageStorageForTest();
        global_ctx.initializeWriteNodePageStorageIfNeed(*mocked_path_pool);
    }

    void reload()
    {
        auto & global_ctx = DB::tests::TiFlashTestEnv::getGlobalContext();
        global_ctx.tryReleaseWriteNodePageStorageForTest();
        global_ctx.initializeWriteNodePageStorageIfNeed(*mocked_path_pool);
    }

    void TearDown() override
    {
        auto & global_ctx = TiFlashTestEnv::getGlobalContext();
        global_ctx.setPageStorageRunMode(old_run_mode);
    }

protected:
    PageStorageRunMode test_run_mode;
    String dir_path;
    PageStorageRunMode old_run_mode;

    std::unique_ptr<PathPool> mocked_path_pool;
    LoggerPtr log;
};

TEST_P(RegionPersisterTest, persister)
try
{
    RegionManager region_manager;

    auto ctx = TiFlashTestEnv::getGlobalContext();

    size_t region_num = 100;
    RegionMap regions;
    const TableID table_id = 100;

    PageStorageConfig config;
    config.file_roll_size = 128 * MB;
    {
        UInt64 diff = 0;
        RegionPersister persister(ctx, region_manager);
        persister.restore(*mocked_path_pool, nullptr, config);

        // Persist region by region
        for (size_t i = 0; i < region_num; ++i)
        {
            auto region = std::make_shared<Region>(createRegionMeta(i, table_id));
            TiKVKey key = RecordKVFormat::genKey(table_id, i, diff++);
            region->insert(ColumnFamilyType::Default, TiKVKey::copyFrom(key), TiKVValue("value1"));
            region->insert(ColumnFamilyType::Write, TiKVKey::copyFrom(key), RecordKVFormat::encodeWriteCfValue('P', 0));
            region->insert(ColumnFamilyType::Lock, TiKVKey::copyFrom(key), RecordKVFormat::encodeLockCfValue('P', "", 0, 0));

            persister.persist(*region);

            regions.emplace(region->id(), region);
        }
    }

    {
        // Truncate the last byte of the meta to mock that the last region persist is not completed
        String meta_path;
        switch (test_run_mode)
        {
        case PageStorageRunMode::ONLY_V3:
            meta_path = dir_path + "/page/kvstore/wal/log_1_0"; // First page
            break;
        case PageStorageRunMode::UNI_PS:
            meta_path = dir_path + "/page/write/wal/log_1_0"; // First page
            break;
        default:
            throw Exception("", ErrorCodes::NOT_IMPLEMENTED);
        }
        Poco::File meta_file(meta_path);
        size_t size = meta_file.getSize();
        int ret = ::truncate(meta_path.c_str(), size - 1); // Remove last one byte
        ASSERT_EQ(ret, 0);
    }

    reload();

    RegionMap new_regions;
    {
        RegionPersister persister(ctx, region_manager);
        new_regions = persister.restore(*mocked_path_pool, nullptr, config);

        // check that only the last region (which write is not completed) is thrown away
        size_t num_regions_missed = 0;
        for (size_t i = 0; i < region_num; ++i)
        {
            auto new_iter = new_regions.find(i);
            if (new_iter == new_regions.end())
            {
                LOG_ERROR(Logger::get("RegionPersisterTest"), "Region missed, id={}", i);
                ++num_regions_missed;
            }
            else
            {
                auto old_region = regions[i];
                auto new_region = new_regions[i];
                ASSERT_EQ(*new_region, *old_region);
            }
        }
        ASSERT_EQ(num_regions_missed, 1);
    }
}
CATCH

TEST_P(RegionPersisterTest, LargeRegion)
try
{
    RegionManager region_manager;

    auto ctx = TiFlashTestEnv::getGlobalContext();

    const TableID table_id = 100;
    const RegionID region_id_base = 20;
    const String large_value(1024 * 512, 'v');

    PageStorageConfig config;
    config.blob_file_limit_size = 32 * MB;
    RegionMap regions;
    {
        UInt64 tso = 0;
        RegionPersister persister(ctx, region_manager);
        persister.restore(*mocked_path_pool, nullptr, config);

        // Persist region
        auto gen_region_data = [&](RegionID region_id, UInt64 expect_size) {
            auto region = std::make_shared<Region>(createRegionMeta(region_id, table_id));
            UInt64 handle_id = 0;
            while (true)
            {
                if (auto data_size = region->dataSize(); data_size > expect_size)
                {
                    LOG_INFO(log, "will persist region_id={} size={}", region_id, data_size);
                    break;
                }
                TiKVKey key = RecordKVFormat::genKey(table_id, handle_id, tso++);
                region->insert(ColumnFamilyType::Default, TiKVKey::copyFrom(key), TiKVValue(large_value.data()));
                region->insert(ColumnFamilyType::Write, TiKVKey::copyFrom(key), RecordKVFormat::encodeWriteCfValue('P', 0));
                region->insert(ColumnFamilyType::Lock, TiKVKey::copyFrom(key), RecordKVFormat::encodeLockCfValue('P', "", 0, 0));
                handle_id += 1;
            }
            return region;
        };

        std::vector<double> test_scales{0.5, 1.0, 1.5, 2.5};
        for (size_t idx = 0; idx < test_scales.size(); ++idx)
        {
            auto scale = test_scales[idx];
            auto region = gen_region_data(region_id_base + idx, config.blob_file_limit_size * scale);
            persister.persist(*region);
            regions.emplace(region->id(), region);
        }
        ASSERT_EQ(regions.size(), test_scales.size());
    }

    RegionMap restored_regions;
    {
        RegionPersister persister(ctx, region_manager);
        restored_regions = persister.restore(*mocked_path_pool, nullptr, config);
    }
    ASSERT_EQ(restored_regions.size(), regions.size());
    for (const auto & [region_id, region] : regions)
    {
        ASSERT_NE(restored_regions.find(region_id), restored_regions.end()) << region_id;
        auto & new_region = restored_regions.at(region_id);
        ASSERT_EQ(new_region->id(), region_id);
        ASSERT_EQ(new_region->confVer(), region->confVer()) << region_id;
        ASSERT_EQ(new_region->dataSize(), region->dataSize()) << region_id;
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(
    TestMode,
    RegionPersisterTest,
    testing::Values(PageStorageRunMode::ONLY_V3, PageStorageRunMode::UNI_PS));
} // namespace tests
} // namespace DB
