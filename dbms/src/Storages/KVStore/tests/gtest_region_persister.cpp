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

#include <Common/FailPoint.h>
#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Common/SyncPoint/Ctl.h>
#include <IO/Buffer/ReadBufferFromFile.h>
#include <IO/Buffer/WriteBufferFromFile.h>
#include <Interpreters/Context.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/KVStore/MultiRaft/RegionManager.h>
#include <Storages/KVStore/MultiRaft/RegionPersister.h>
#include <Storages/KVStore/MultiRaft/RegionSerde.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>
#include <Storages/KVStore/tests/region_helper.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/PathPool.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <common/logger_useful.h>
#include <common/types.h>

#include <ext/scope_guard.h>
#include <future>

namespace DB
{
namespace FailPoints
{
extern const char pause_when_persist_region[];
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

static RegionPtr makeTmpRegion()
{
    return makeRegion(createRegionMeta(1001, 1));
}

static std::function<size_t(UInt32 &, WriteBuffer &)> mockSerFactory(int value)
{
    return [value](UInt32 & actual_extension_count, WriteBuffer & buf) -> size_t {
        auto total_size = 0;
        if (value & 1)
        {
            std::string s = "abcd";
            total_size += Region::writePersistExtension(
                actual_extension_count,
                buf,
                magic_enum::enum_underlying(RegionPersistExtension::ReservedForTest),
                s.data(),
                s.size());
        }
        if (value & 2)
        {
            std::string s = "kkk";
            total_size += Region::writePersistExtension(
                actual_extension_count,
                buf,
                UNUSED_EXTENSION_NUMBER_FOR_TEST,
                s.data(),
                s.size());
        }
        return total_size;
    };
}

static std::function<bool(UInt32, ReadBuffer &, UInt32)> mockDeserFactory(int value, std::shared_ptr<int> counter)
{
    return [value, counter](UInt32 extension_type, ReadBuffer & buf, UInt32 length) -> bool {
        if (value & 1)
        {
            if (extension_type == magic_enum::enum_underlying(RegionPersistExtension::ReservedForTest))
            {
                RUNTIME_CHECK(length == 4);
                RUNTIME_CHECK(readStringWithLength(buf, 4) == "abcd");
                *counter |= 1;
                return true;
            }
        }
        if (value & 2)
        {
            // Can't parse UNUSED_EXTENSION_NUMBER_FOR_TEST.
            if (extension_type == UNUSED_EXTENSION_NUMBER_FOR_TEST)
            {
                RUNTIME_CHECK(length == 3);
                *counter |= 2;
            }
        }
        return false;
    };
}

class RegionSeriTest : public ::testing::Test
{
public:
    RegionSeriTest()
        : dir_path(TiFlashTestEnv::getTemporaryPath("RegionSeriTest"))
    {}

    void SetUp() override { clearFileOnDisk(); }

    void clearFileOnDisk() { TiFlashTestEnv::tryRemovePath(dir_path, /*recreate=*/true); }

    const std::string dir_path;
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
    const auto path = dir_path + "/meta.test";
    WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
    RegionMeta meta = createRegionMeta(888, 66);
    auto size = std::get<0>(meta.serialize(write_buf));
    write_buf.next();
    write_buf.sync();
    ASSERT_EQ(size, (size_t)Poco::File(path).getSize());

    ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
    auto restored_meta = RegionMeta::deserialize(read_buf);
    ASSERT_EQ(restored_meta, meta);
}
CATCH

TEST_F(RegionSeriTest, RegionOldFormatVersion)
try
{
    TableID table_id = 100;
    auto region = makeTmpRegion();
    TiKVKey key = RecordKVFormat::genKey(table_id, 323, 9983);
    region->insert("default", TiKVKey::copyFrom(key), TiKVValue("value1"));
    region->insert("write", TiKVKey::copyFrom(key), RecordKVFormat::encodeWriteCfValue('P', 0));
    region->insert("lock", TiKVKey::copyFrom(key), RecordKVFormat::encodeLockCfValue('P', "", 0, 0));

    region->updateRaftLogEagerIndex(1024);

    const auto path = dir_path + "/region.test";
    WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
    size_t region_ser_size = std::get<0>(region->serializeImpl(1, 0, mockSerFactory(0), write_buf));
    write_buf.next();
    write_buf.sync();
    ASSERT_EQ(region_ser_size, (size_t)Poco::File(path).getSize());

    ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
    auto new_region = Region::deserialize(read_buf);
    ASSERT_REGION_EQ(*new_region, *region);
    {
        // For the region restored with binary_version == 1, the eager_truncated_index is equals to
        // truncated_index
        const auto & [eager_truncated_index, applied_index] = new_region->getRaftLogEagerGCRange();
        ASSERT_EQ(new_region->mutMeta().truncateIndex(), 5);
        ASSERT_EQ(eager_truncated_index, 5);
    }
}
CATCH

TEST_F(RegionSeriTest, Region)
try
{
    TableID table_id = 100;
    auto region = makeTmpRegion();
    TiKVKey key = RecordKVFormat::genKey(table_id, 323, 9983);
    region->insert("default", TiKVKey::copyFrom(key), TiKVValue("value1"));
    region->insert("write", TiKVKey::copyFrom(key), RecordKVFormat::encodeWriteCfValue('P', 0));
    region->insert("lock", TiKVKey::copyFrom(key), RecordKVFormat::encodeLockCfValue('P', "", 0, 0));

    region->updateRaftLogEagerIndex(1024);

    const auto path = dir_path + "/region.test";
    WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
    size_t region_ser_size = std::get<0>(region->serialize(write_buf));
    write_buf.next();
    write_buf.sync();
    ASSERT_EQ(region_ser_size, (size_t)Poco::File(path).getSize());

    ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
    auto new_region = Region::deserialize(read_buf);
    ASSERT_REGION_EQ(*new_region, *region);
    {
        const auto & [eager_truncated_index, applied_index] = new_region->getRaftLogEagerGCRange();
        ASSERT_EQ(eager_truncated_index, 1024);
    }
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

            *region_state.mutable_region()
                = createRegionInfo(1001, RecordKVFormat::genKey(table_id, 0), RecordKVFormat::genKey(table_id, 300));
            region_state.mutable_merge_state()->set_commit(888);
            region_state.mutable_merge_state()->set_min_index(777);
            *region_state.mutable_merge_state()->mutable_target()
                = createRegionInfo(1111, RecordKVFormat::genKey(table_id, 300), RecordKVFormat::genKey(table_id, 400));
        }
        region = makeRegion(RegionMeta(createPeer(31, true), apply_state, 5, region_state));
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

TEST_F(RegionSeriTest, FlexibleRestore)
try
{
    auto ext_cnt_2 = 0; // Suppose has no ext.
    auto ext_cnt_3 = 1; // Suppose has ReservedForTest.
    auto ext_cnt_4 = 2; // Suppose has UNUSED_EXTENSION_NUMBER_FOR_TEST.
    {
        auto counter = std::make_shared<int>(0);
        // V2 store, V2 load, no unrecognized fields
        auto region = makeTmpRegion();
        region->updateRaftLogEagerIndex(5678);
        const auto path = dir_path + "/region0.test";
        WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
        size_t region_ser_size = std::get<0>(region->serializeImpl(2, ext_cnt_2, mockSerFactory(0), write_buf));
        write_buf.next();
        write_buf.sync();
        ASSERT_EQ(region_ser_size, (size_t)Poco::File(path).getSize());

        ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
        auto new_region = Region::deserializeImpl(2, mockDeserFactory(0, counter), read_buf);
        ASSERT_EQ(new_region->getRaftLogEagerGCRange().first, 5678);
        ASSERT_REGION_EQ(*new_region, *region);
        ASSERT_EQ(*counter, 0);
    }
    {
        auto counter = std::make_shared<int>(0);
        // V3 store, V3 load, no unrecognized fields
        auto region = makeTmpRegion();
        region->updateRaftLogEagerIndex(5678);
        const auto path = dir_path + "/region.test";
        WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
        size_t region_ser_size = std::get<0>(region->serializeImpl(3, ext_cnt_3, mockSerFactory(1), write_buf));
        write_buf.next();
        write_buf.sync();
        ASSERT_EQ(region_ser_size, (size_t)Poco::File(path).getSize());

        ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
        auto new_region = Region::deserializeImpl(3, mockDeserFactory(1, counter), read_buf);
        ASSERT_EQ(new_region->getRaftLogEagerGCRange().first, 5678);
        ASSERT_REGION_EQ(*new_region, *region);
        ASSERT_EQ(*counter, 1);
    }
    {
        auto counter = std::make_shared<int>(0);
        // Downgrade. V4(whatever) store, V3 load, UNUSED_EXTENSION_NUMBER_FOR_TEST unrecognized.
        auto region = makeTmpRegion();
        region->updateRaftLogEagerIndex(5678);
        // In V2, will also write UNUSED_EXTENSION_NUMBER_FOR_TEST.
        const auto path = dir_path + "/region2.test";
        WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
        size_t region_ser_size = std::get<0>(region->serializeImpl(4, ext_cnt_4, mockSerFactory(1 | 2), write_buf));
        write_buf.next();
        write_buf.sync();
        ASSERT_EQ(region_ser_size, (size_t)Poco::File(path).getSize());

        ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
        auto new_region = Region::deserializeImpl(3, mockDeserFactory(1, counter), read_buf);
        ASSERT_EQ(new_region->getRaftLogEagerGCRange().first, 5678);
        ASSERT_REGION_EQ(*new_region, *region);
        ASSERT_EQ(*counter, 1);
    }
    {
        auto counter = std::make_shared<int>(0);
        // Downgrade. V4(whatever) store. V2 load. UNUSED_EXTENSION_NUMBER_FOR_TEST unrecognized.
        auto region = makeTmpRegion();
        region->updateRaftLogEagerIndex(5678);
        const auto path = dir_path + "/region3.test";
        WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
        size_t region_ser_size = std::get<0>(region->serializeImpl(4, ext_cnt_4, mockSerFactory(1 | 2), write_buf));
        write_buf.next();
        write_buf.sync();
        ASSERT_EQ(region_ser_size, (size_t)Poco::File(path).getSize());

        {
            ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
            auto new_region = Region::deserializeImpl(2, mockDeserFactory(1, counter), read_buf);
            ASSERT_EQ(new_region->getRaftLogEagerGCRange().first, 5678);
            ASSERT_REGION_EQ(*new_region, *region);
            ASSERT_EQ(*counter, 1); // Only parsed ReservedForTest.
        }
        {
            // Also test V4 load.
            ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
            auto new_region = Region::deserializeImpl(4, mockDeserFactory(1 | 2, counter), read_buf);
            ASSERT_EQ(new_region->getRaftLogEagerGCRange().first, 5678);
            ASSERT_REGION_EQ(*new_region, *region);
            ASSERT_EQ(*counter, 1 | 2);
        }
    }
    {
        auto counter = std::make_shared<int>(0);
        // Upgrade. V2 to V3.
        auto region = makeTmpRegion();
        region->updateRaftLogEagerIndex(5678);
        const auto path = dir_path + "/region4.test";
        WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
        size_t region_ser_size = std::get<0>(region->serializeImpl(2, ext_cnt_2, mockSerFactory(0), write_buf));
        write_buf.next();
        write_buf.sync();
        ASSERT_EQ(region_ser_size, (size_t)Poco::File(path).getSize());

        ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
        auto new_region = Region::deserializeImpl(3, mockDeserFactory(1, counter), read_buf);
        ASSERT_EQ(new_region->getRaftLogEagerGCRange().first, 5678);
        ASSERT_REGION_EQ(*new_region, *region);
        ASSERT_EQ(*counter, 0);
    }
    {
        // Upgrade -> Upgrade -> Downgrade -> Downgrade
        auto region = makeTmpRegion();
        region->updateRaftLogEagerIndex(5678);
        const auto path = dir_path + "/region5.test";
        WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
        size_t region_ser_size = std::get<0>(region->serializeImpl(2, ext_cnt_2, mockSerFactory(0), write_buf));
        write_buf.next();
        write_buf.sync();
        ASSERT_EQ(region_ser_size, (size_t)Poco::File(path).getSize());
        {
            // 2 -> 3
            auto counter = std::make_shared<int>(0);
            ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
            auto new_region = Region::deserializeImpl(3, mockDeserFactory(1, counter), read_buf);
            ASSERT_EQ(new_region->getRaftLogEagerGCRange().first, 5678);
            ASSERT_REGION_EQ(*new_region, *region);
            WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
            region->serializeImpl(3, ext_cnt_3, mockSerFactory(1), write_buf);
            ASSERT_EQ(*counter, 0);
        }

        {
            // 3 -> 4
            auto counter = std::make_shared<int>(0);
            ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
            auto new_region = Region::deserializeImpl(4, mockDeserFactory(1 | 2, counter), read_buf);
            ASSERT_EQ(*counter, 1);
            ASSERT_EQ(new_region->getRaftLogEagerGCRange().first, 5678);
            ASSERT_REGION_EQ(*new_region, *region);
            WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
            region->serializeImpl(4, ext_cnt_4, mockSerFactory(1 | 2), write_buf);
        }

        {
            // 4 -> 2
            auto counter = std::make_shared<int>(0);
            ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
            region->serializeImpl(2, ext_cnt_2, mockSerFactory(0), write_buf);
            EXPECT_THROW(Region::deserializeImpl(2, mockDeserFactory(0, counter), read_buf), Exception);
        }
    }
    {
        // Downgrade. V2 store. V1 load.
        auto counter = std::make_shared<int>(0);
        auto region = makeTmpRegion();
        region->updateRaftLogEagerIndex(5678);
        const auto path = dir_path + "/region6.test";
        WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
        size_t region_ser_size = std::get<0>(region->serializeImpl(2, ext_cnt_2, mockSerFactory(0), write_buf));
        write_buf.next();
        write_buf.sync();
        ASSERT_EQ(region_ser_size, (size_t)Poco::File(path).getSize());

        ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
        EXPECT_THROW(Region::deserializeImpl(1, mockDeserFactory(0, counter), read_buf), Exception);
    }
    {
        // Downgrade. V3 store. V1 load.
        auto counter = std::make_shared<int>(0);
        auto region = makeTmpRegion();
        region->updateRaftLogEagerIndex(5678);
        const auto path = dir_path + "/region7.test";
        WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
        size_t region_ser_size = std::get<0>(region->serializeImpl(3, ext_cnt_3, mockSerFactory(1), write_buf));
        write_buf.next();
        write_buf.sync();
        ASSERT_EQ(region_ser_size, (size_t)Poco::File(path).getSize());

        ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
        EXPECT_THROW(Region::deserializeImpl(1, mockDeserFactory(0, counter), read_buf), Exception);
    }
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

TEST_P(RegionPersisterTest, Concurrency)
try
{
    RegionManager region_manager;

    auto ctx = TiFlashTestEnv::getGlobalContext();

    RegionMap regions;
    const TableID table_id = 100;

    PageStorageConfig config;
    config.file_roll_size = 128 * MB;

    UInt64 diff = 0;
    RegionPersister persister(ctx);
    persister.restore(*mocked_path_pool, nullptr, config);

    // Persist region by region
    const RegionID region_100 = 100;
    FailPointHelper::enableFailPoint(FailPoints::pause_when_persist_region, region_100);
    SCOPE_EXIT({ FailPointHelper::disableFailPoint(FailPoints::pause_when_persist_region); });

    auto sp_persist_region_100 = SyncPointCtl::enableInScope("before_RegionPersister::persist_write_done");
    auto th_persist_region_100 = std::async([&]() {
        auto region_task_lock = region_manager.genRegionTaskLock(region_100);

        auto region = makeRegion(createRegionMeta(region_100, table_id));
        TiKVKey key = RecordKVFormat::genKey(table_id, region_100, diff++);
        region->insert(ColumnFamilyType::Default, TiKVKey::copyFrom(key), TiKVValue("value1"));
        region->insert(ColumnFamilyType::Write, TiKVKey::copyFrom(key), RecordKVFormat::encodeWriteCfValue('P', 0));
        region->insert(
            ColumnFamilyType::Lock,
            TiKVKey::copyFrom(key),
            RecordKVFormat::encodeLockCfValue('P', "", 0, 0));

        persister.persist(*region, region_task_lock);

        regions.emplace(region->id(), region);
    });
    LOG_INFO(log, "paused before persisting region 100");
    sp_persist_region_100.waitAndPause();

    LOG_INFO(log, "before persisting region 101");
    const RegionID region_101 = 101;
    {
        auto region_task_lock = region_manager.genRegionTaskLock(region_101);

        auto region = makeRegion(createRegionMeta(region_101, table_id));
        TiKVKey key = RecordKVFormat::genKey(table_id, region_101, diff++);
        region->insert(ColumnFamilyType::Default, TiKVKey::copyFrom(key), TiKVValue("value1"));
        region->insert(ColumnFamilyType::Write, TiKVKey::copyFrom(key), RecordKVFormat::encodeWriteCfValue('P', 0));
        region->insert(
            ColumnFamilyType::Lock,
            TiKVKey::copyFrom(key),
            RecordKVFormat::encodeLockCfValue('P', "", 0, 0));

        persister.persist(*region, region_task_lock);

        regions.emplace(region->id(), region);
    }
    LOG_INFO(log, "after persisting region 101");

    sp_persist_region_100.next();
    th_persist_region_100.get();

    LOG_INFO(log, "finished");
}
CATCH

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
        RegionPersister persister(ctx);
        persister.restore(*mocked_path_pool, nullptr, config);

        // Persist region by region
        for (size_t i = 0; i < region_num; ++i)
        {
            auto region_task_lock = region_manager.genRegionTaskLock(i);

            auto region = makeRegion(createRegionMeta(i, table_id));
            TiKVKey key = RecordKVFormat::genKey(table_id, i, diff++);
            region->insert(ColumnFamilyType::Default, TiKVKey::copyFrom(key), TiKVValue("value1"));
            region->insert(ColumnFamilyType::Write, TiKVKey::copyFrom(key), RecordKVFormat::encodeWriteCfValue('P', 0));
            region->insert(
                ColumnFamilyType::Lock,
                TiKVKey::copyFrom(key),
                RecordKVFormat::encodeLockCfValue('P', "", 0, 0));

            persister.persist(*region, region_task_lock);

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
        RegionPersister persister(ctx);
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
        RegionPersister persister(ctx);
        persister.restore(*mocked_path_pool, nullptr, config);

        // Persist region
        auto gen_region_data = [&](RegionID region_id, UInt64 expect_size) {
            auto region = makeRegion(createRegionMeta(region_id, table_id));
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
                region->insert(
                    ColumnFamilyType::Write,
                    TiKVKey::copyFrom(key),
                    RecordKVFormat::encodeWriteCfValue('P', 0));
                region->insert(
                    ColumnFamilyType::Lock,
                    TiKVKey::copyFrom(key),
                    RecordKVFormat::encodeLockCfValue('P', "", 0, 0));
                handle_id += 1;
            }
            return region;
        };

        std::vector<double> test_scales{0.5, 1.0, 1.5, 2.5};
        for (size_t idx = 0; idx < test_scales.size(); ++idx)
        {
            auto region_task_lock = region_manager.genRegionTaskLock(region_id_base + idx);

            auto scale = test_scales[idx];
            auto region = gen_region_data(region_id_base + idx, config.blob_file_limit_size * scale);
            persister.persist(*region, region_task_lock);
            regions.emplace(region->id(), region);
        }
        ASSERT_EQ(regions.size(), test_scales.size());
    }

    RegionMap restored_regions;
    {
        RegionPersister persister(ctx);
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
