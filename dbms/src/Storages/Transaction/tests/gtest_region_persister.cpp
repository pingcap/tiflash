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
#include <Common/Stopwatch.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
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

#include <ext/scope_guard.h>

namespace DB
{
namespace FailPoints
{
extern const char force_enable_region_persister_compatible_mode[];
extern const char force_disable_region_persister_compatible_mode[];
} // namespace FailPoints

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


class RegionPersisterTest : public ::testing::Test
{
public:
    RegionPersisterTest()
        : dir_path(TiFlashTestEnv::getTemporaryPath("/region_persister_test"))
    {
    }

    static void SetUpTestCase() {}

    void SetUp() override
    {
        TiFlashTestEnv::tryRemovePath(dir_path);

        auto & global_ctx = TiFlashTestEnv::getGlobalContext();
        auto path_capacity = global_ctx.getPathCapacity();
        auto provider = global_ctx.getFileProvider();

        Strings main_data_paths{dir_path};
        mocked_path_pool = std::make_unique<PathPool>(
            main_data_paths,
            main_data_paths,
            /*kvstore_paths=*/Strings{},
            path_capacity,
            provider,
            /*enable_raft_compatible_mode_=*/true);
    }

protected:
    String dir_path;

    std::unique_ptr<PathPool> mocked_path_pool;
};

TEST_F(RegionPersisterTest, persister)
try
{
    RegionManager region_manager;

    auto ctx = TiFlashTestEnv::getGlobalContext();

    size_t region_num = 100;
    RegionMap regions;
    const TableID table_id = 100;

    PageStorage::Config config;
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
            region->insert("default", TiKVKey::copyFrom(key), TiKVValue("value1"));
            region->insert("write", TiKVKey::copyFrom(key), RecordKVFormat::encodeWriteCfValue('P', 0));
            region->insert("lock", TiKVKey::copyFrom(key), RecordKVFormat::encodeLockCfValue('P', "", 0, 0));

            persister.persist(*region);

            regions.emplace(region->id(), region);
        }
    }

    {
        // Truncate the last byte of the meta to mock that the last region persist is not completed
        auto meta_path = dir_path + "/page/kvstore/wal/log_1_0"; // First page
        Poco::File meta_file(meta_path);
        size_t size = meta_file.getSize();
        int ret = ::truncate(meta_path.c_str(), size - 1); // Remove last one byte
        ASSERT_EQ(ret, 0);
    }

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
                LOG_FMT_ERROR(&Poco::Logger::get("RegionPersisterTest"), "Region missed, id={}", i);
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

TEST_F(RegionPersisterTest, persisterPSVersionUpgrade)
try
{
    auto & global_ctx = TiFlashTestEnv::getGlobalContext();
    auto saved_storage_run_mode = global_ctx.getPageStorageRunMode();
    global_ctx.setPageStorageRunMode(PageStorageRunMode::ONLY_V2);
    // Force to run in ps v1 mode for the default region persister
    SCOPE_EXIT({
        FailPointHelper::disableFailPoint(FailPoints::force_enable_region_persister_compatible_mode);
        global_ctx.setPageStorageRunMode(saved_storage_run_mode);
    });

    size_t region_num = 500;
    RegionMap regions;
    TableID table_id = 100;

    PageStorage::Config config;
    config.file_roll_size = 16 * 1024;
    RegionManager region_manager;
    DB::Timestamp tso = 0;
    {
        RegionPersister persister(global_ctx, region_manager);
        // Force to run in ps v1 mode
        FailPointHelper::enableFailPoint(FailPoints::force_enable_region_persister_compatible_mode);
        persister.restore(*mocked_path_pool, nullptr, config);
        ASSERT_EQ(persister.page_writer, nullptr);
        ASSERT_EQ(persister.page_reader, nullptr);
        ASSERT_NE(persister.stable_page_storage, nullptr); // ps v1

        for (size_t i = 0; i < region_num; ++i)
        {
            auto region = std::make_shared<Region>(createRegionMeta(i, table_id));
            TiKVKey key = RecordKVFormat::genKey(table_id, i, tso++);
            region->insert("default", TiKVKey::copyFrom(key), TiKVValue("value1"));
            region->insert("write", TiKVKey::copyFrom(key), RecordKVFormat::encodeWriteCfValue('P', 0));
            region->insert("lock", TiKVKey::copyFrom(key), RecordKVFormat::encodeLockCfValue('P', "", 0, 0));

            persister.persist(*region);

            regions.emplace(region->id(), region);
        }
        LOG_DEBUG(&Poco::Logger::get("fff"), "v1 write done");
    }

    {
        RegionPersister persister(global_ctx, region_manager);
        // restore normally, should run in ps v1 mode.
        RegionMap new_regions = persister.restore(*mocked_path_pool, nullptr, config);
        ASSERT_EQ(persister.page_writer, nullptr);
        ASSERT_EQ(persister.page_reader, nullptr);
        ASSERT_NE(persister.stable_page_storage, nullptr); // ps v1
        // Try to read
        for (size_t i = 0; i < region_num; ++i)
        {
            auto new_iter = new_regions.find(i);
            ASSERT_NE(new_iter, new_regions.end());
            auto old_region = regions[i];
            auto new_region = new_regions[i];
            ASSERT_EQ(*new_region, *old_region);
        }
    }

    size_t region_num_under_nromal_mode = 200;
    {
        RegionPersister persister(global_ctx, region_manager);
        // Force to run in ps v2 mode
        FailPointHelper::enableFailPoint(FailPoints::force_disable_region_persister_compatible_mode);
        RegionMap new_regions = persister.restore(*mocked_path_pool, nullptr, config);
        ASSERT_NE(persister.page_writer, nullptr);
        ASSERT_NE(persister.page_reader, nullptr);
        ASSERT_EQ(persister.stable_page_storage, nullptr);
        // Try to read
        for (size_t i = 0; i < region_num; ++i)
        {
            auto new_iter = new_regions.find(i);
            ASSERT_NE(new_iter, new_regions.end());
            auto old_region = regions[i];
            auto new_region = new_regions[i];
            ASSERT_EQ(*new_region, *old_region);
        }
        // Try to write more regions under ps v2 mode
        for (size_t i = region_num; i < region_num + region_num_under_nromal_mode; ++i)
        {
            auto region = std::make_shared<Region>(createRegionMeta(i, table_id));
            TiKVKey key = RecordKVFormat::genKey(table_id, i, tso++);
            region->insert("default", TiKVKey::copyFrom(key), TiKVValue("value1"));
            region->insert("write", TiKVKey::copyFrom(key), RecordKVFormat::encodeWriteCfValue('P', 0));
            region->insert("lock", TiKVKey::copyFrom(key), RecordKVFormat::encodeLockCfValue('P', "", 0, 0));

            persister.persist(*region);

            regions.emplace(region->id(), region);
        }
    }

    {
        RegionPersister persister(global_ctx, region_manager);
        // Restore normally, should run in ps v2 mode.
        RegionMap new_regions = persister.restore(*mocked_path_pool, nullptr, config);
        ASSERT_NE(persister.page_writer, nullptr);
        ASSERT_NE(persister.page_reader, nullptr);
        ASSERT_EQ(persister.stable_page_storage, nullptr);
        // Try to read
        for (size_t i = 0; i < region_num + region_num_under_nromal_mode; ++i)
        {
            auto new_iter = new_regions.find(i);
            ASSERT_NE(new_iter, new_regions.end()) << " region:" << i;
            auto old_region = regions[i];
            auto new_region = new_regions[i];
            ASSERT_EQ(*new_region, *old_region) << " region:" << i;
        }
    }
}
CATCH

} // namespace tests
} // namespace DB
