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
#include <Common/Stopwatch.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionManager.h>
#include <Storages/Transaction/RegionPersister.h>
#include <Storages/Transaction/TiKVRecordFormat.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <ext/scope_guard.h>
#include <sstream>

#include "region_helper.h"

namespace DB
{
namespace FailPoints
{
extern const char force_enable_region_persister_compatible_mode[];
extern const char force_disable_region_persister_compatible_mode[];
} // namespace FailPoints

namespace tests
{
class RegionPersister_test : public ::testing::Test
{
public:
    RegionPersister_test()
        : dir_path(TiFlashTestEnv::getTemporaryPath("/region_persister_tmp"))
    {}

    static void SetUpTestCase() {}

    void SetUp() override { dropFiles(); }

    void dropFiles()
    {
        // cleanup
        Poco::File file(dir_path);
        if (file.exists())
            file.remove(true);
        file.createDirectories();
    }

    void runTest(const String & path, bool sync_on_write);
    void testFunc(const String & path, const PageStorage::Config & config, int region_num, bool is_gc, bool clean_up);

protected:
    String dir_path;

    DB::Timestamp tso = 0;
};

static ::testing::AssertionResult PeerCompare(
    const char * lhs_expr,
    const char * rhs_expr,
    const metapb::Peer & lhs,
    const metapb::Peer & rhs)
{
    if (lhs.id() == rhs.id() && lhs.role() == rhs.role())
        return ::testing::AssertionSuccess();
    else
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
    else
        return ::testing::internal::EqFailure(lhs_expr, rhs_expr, lhs.toString(), rhs.toString(), false);
}
#define ASSERT_REGION_EQ(val1, val2) ASSERT_PRED_FORMAT2(::DB::tests::RegionCompare, val1, val2)

TEST_F(RegionPersister_test, peer)
try
{
    auto peer = createPeer(100, true);
    auto path = dir_path + "/peer.test";
    WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
    auto size = writeBinary2(peer, write_buf);
    write_buf.next();
    write_buf.sync();
    ASSERT_EQ(size, (size_t)Poco::File(path).getSize());

    ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
    auto new_peer = readPeer(read_buf);
    ASSERT_PEER_EQ(new_peer, peer);
}
CATCH

TEST_F(RegionPersister_test, region_info)
try
{
    auto region_info = createRegionInfo(233, "", "");
    auto path = dir_path + "/region_info.test";
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

TEST_F(RegionPersister_test, region_meta)
try
{
    RegionMeta meta = createRegionMeta(888, 66);
    auto path = dir_path + "/meta.test";
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

TEST_F(RegionPersister_test, region)
try
{
    TableID table_id = 100;
    auto region = std::make_shared<Region>(createRegionMeta(1001, table_id));
    TiKVKey key = RecordKVFormat::genKey(table_id, 323, 9983);
    region->insert("default", TiKVKey::copyFrom(key), TiKVValue("value1"));
    region->insert("write", TiKVKey::copyFrom(key), RecordKVFormat::encodeWriteCfValue('P', 0));
    region->insert("lock", TiKVKey::copyFrom(key), RecordKVFormat::encodeLockCfValue('P', "", 0, 0));

    auto path = dir_path + "/region.test";
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

TEST_F(RegionPersister_test, region_stat)
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

    auto path = dir_path + "/region_state.test";
    WriteBufferFromFile write_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
    size_t region_ser_size = std::get<0>(region->serialize(write_buf));
    write_buf.next();

    ASSERT_EQ(region_ser_size, (size_t)Poco::File(path).getSize());
    ReadBufferFromFile read_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
    auto new_region = Region::deserialize(read_buf);
    ASSERT_EQ(*new_region, *region);
}
CATCH

TEST_F(RegionPersister_test, persister)
try
{
    RegionManager region_manager;

    std::string path = dir_path + "/broken_file";

    auto ctx = TiFlashTestEnv::getContext(DB::Settings(),
                                          Strings{
                                              path,
                                          });

    size_t region_num = 100;
    RegionMap regions;
    TableID table_id = 100;

    PageStorage::Config config;
    config.file_roll_size = 128 * MB;
    {
        UInt64 diff = 0;
        RegionPersister persister(ctx, region_manager);
        persister.restore(nullptr, config);

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

        // If we truncate page data file, exception will throw instead of droping last region.
        auto meta_path = path + "/kvstore/page_1_0/meta"; // First page
        Poco::File meta_file(meta_path);
        size_t size = meta_file.getSize();
        int rt = ::truncate(meta_path.c_str(), size - 1); // Remove last one byte
        ASSERT_EQ(rt, 0);
    }

    RegionMap new_regions;
    {
        RegionPersister persister(ctx, region_manager);
        new_regions = persister.restore(nullptr, config);
        size_t num_regions_missed = 0;
        for (size_t i = 0; i < region_num; ++i)
        {
            auto new_iter = new_regions.find(i);
            if (new_iter == new_regions.end())
            {
                LOG_FMT_ERROR(&Poco::Logger::get("RegionPersister_test"), "Region missed, id={}", i);
                ++num_regions_missed;
            }
            else
            {
                auto old_region = regions[i];
                auto new_region = new_regions[i];
                ASSERT_EQ(*new_region, *old_region);
            }
        }
        ASSERT_EQ(num_regions_missed, 1UL);
    }
}
CATCH

TEST_F(RegionPersister_test, persister_compatible_mode)
try
{
    std::string path = dir_path + "/compatible_mode";

    // Force to run in compatible mode for the default region persister
    FailPointHelper::enableFailPoint(FailPoints::force_enable_region_persister_compatible_mode);
    SCOPE_EXIT({ FailPointHelper::disableFailPoint(FailPoints::force_enable_region_persister_compatible_mode); });
    auto ctx = TiFlashTestEnv::getContext(DB::Settings(),
                                          Strings{
                                              path,
                                          });

    size_t region_num = 500;
    RegionMap regions;
    TableID table_id = 100;

    PageStorage::Config config;
    config.file_roll_size = 16 * 1024;
    RegionManager region_manager;
    DB::Timestamp tso = 0;
    {
        RegionPersister persister(ctx, region_manager);
        // Force to run in compatible mode
        FailPointHelper::enableFailPoint(FailPoints::force_enable_region_persister_compatible_mode);
        persister.restore(nullptr, config);
        ASSERT_EQ(persister.page_writer, nullptr);
        ASSERT_EQ(persister.page_reader, nullptr);
        ASSERT_NE(persister.stable_page_storage, nullptr);

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
    }

    {
        RegionPersister persister(ctx, region_manager);
        // restore normally, should run in compatible mode.
        RegionMap new_regions = persister.restore(nullptr, config);
        ASSERT_EQ(persister.page_writer, nullptr);
        ASSERT_EQ(persister.page_reader, nullptr);
        ASSERT_NE(persister.stable_page_storage, nullptr);
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
        RegionPersister persister(ctx, region_manager);
        // Force to run in normal mode
        FailPointHelper::enableFailPoint(FailPoints::force_disable_region_persister_compatible_mode);
        RegionMap new_regions = persister.restore(nullptr, config);
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
        // Try to write more regions under normal mode
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
        RegionPersister persister(ctx, region_manager);
        // Restore normally, should run in normal mode.
        RegionMap new_regions = persister.restore(nullptr, config);
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


void RegionPersister_test::testFunc(const String & path, const PageStorage::Config & config, int region_num, bool is_gc, bool clean_up)
{
    if (clean_up)
        dropFiles();

    auto ctx = TiFlashTestEnv::getContext(DB::Settings(),
                                          Strings{
                                              path,
                                          });

    RegionManager region_manager;
    RegionPersister persister(ctx, region_manager);
    persister.restore(nullptr, config);

    TableID table_id = 100;
    RegionMap regions;
    for (int i = 0; i < region_num; ++i)
    {
        auto region = std::make_shared<Region>(createRegionMeta(i, table_id));
        TiKVKey key = RecordKVFormat::genKey(table_id, i, tso++);
        region->insert("default", TiKVKey::copyFrom(key), TiKVValue("value1"));
        region->insert("write", TiKVKey::copyFrom(key), RecordKVFormat::encodeWriteCfValue('P', 0));
        region->insert("lock", TiKVKey::copyFrom(key), RecordKVFormat::encodeLockCfValue('P', "", 0, 0));

        persister.persist(*region);

        regions.emplace(region->id(), region);
    }

    if (is_gc)
        persister.gc();

    RegionMap new_regions;
    new_regions = persister.restore(nullptr, config);

    for (int i = 0; i < region_num; ++i)
    {
        auto old_region = regions[i];
        auto new_region = new_regions[i];
        ASSERT_EQ(*new_region, *old_region);
    }

    if (clean_up)
        dropFiles();
}

void RegionPersister_test::runTest(const String & path, bool sync_on_write)
{
    Stopwatch watch;

    dropFiles();
    SCOPE_EXIT({ dropFiles(); });

    {
        PageStorage::Config conf;
        conf.sync_on_write = sync_on_write;
        conf.file_roll_size = 1;
        conf.gc_min_bytes = 1;
        conf.num_write_slots = 4;

        testFunc(path, conf, 10, false, false);
        testFunc(path, conf, 10, true, false);

        testFunc(path, conf, 10, false, true);
        testFunc(path, conf, 10, true, true);
    }
    {
        PageStorage::Config conf;
        conf.sync_on_write = sync_on_write;
        conf.file_roll_size = 500;
        conf.gc_min_bytes = 1;
        conf.num_write_slots = 4;

        testFunc(path, conf, 100, false, false);
        testFunc(path, conf, 100, false, false);
        testFunc(path, conf, 100, false, false);
        testFunc(path, conf, 100, false, true);

        testFunc(path, conf, 100, true, false);
        testFunc(path, conf, 100, true, false);
        testFunc(path, conf, 100, true, false);
        testFunc(path, conf, 100, true, false);
    }
    {
        PageStorage::Config conf;
        conf.sync_on_write = sync_on_write;
        conf.file_roll_size = 500;
        conf.gc_min_bytes = 1;
        conf.num_write_slots = 4;

        testFunc(path, conf, 100, false, false);
        testFunc(path, conf, 100, false, false);
        testFunc(path, conf, 100, false, false);
        testFunc(path, conf, 100, false, true);

        testFunc(path, conf, 100, true, false);
        testFunc(path, conf, 100, true, false);
        testFunc(path, conf, 100, true, false);
        testFunc(path, conf, 100, true, false);
    }
    {
        PageStorage::Config conf;
        conf.sync_on_write = sync_on_write;
        conf.num_write_slots = 4;

        testFunc(path, conf, 10000, false, false);
        testFunc(path, conf, 10000, false, false);
        testFunc(path, conf, 10000, false, false);
        testFunc(path, conf, 10000, false, false);
        testFunc(path, conf, 10000, false, false);
        testFunc(path, conf, 10000, false, false);
        testFunc(path, conf, 10000, false, false);
    }
    {
        PageStorage::Config conf;
        conf.sync_on_write = sync_on_write;
        conf.num_write_slots = 4;

        testFunc(path, conf, 10000, true, false);
        testFunc(path, conf, 10000, true, false);
        testFunc(path, conf, 10000, true, false);
        testFunc(path, conf, 10000, true, false);
        testFunc(path, conf, 10000, true, false);
        testFunc(path, conf, 10000, true, false);
        testFunc(path, conf, 10000, true, false);
    }

    auto seconds = watch.elapsedSeconds();
    LOG_FMT_INFO(&Poco::Logger::get("RegionPersister_test"), "[sync_on_write={}], [time={:.4f}s]", sync_on_write, seconds);
}

// This test takes about 10 minutes. Disable by default
TEST_F(RegionPersister_test, DISABLED_persister_sync_on_write)
{
    runTest(dir_path + "region_persist_storage_sow_false", false);
    runTest(dir_path + "region_persist_storage_sow_true", true);
}

} // namespace tests
} // namespace DB
