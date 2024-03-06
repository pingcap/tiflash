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

#include <Core/Types.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/Page/V3/PageDefines.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/PathPool.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/logger_useful.h>
#include <fmt/format.h>


namespace DB
{
namespace tests
{
class PathPoolTest : public ::testing::Test
{
public:
    PathPoolTest()
        : log(Logger::get("PathPoolTest"))
    {}

    static void SetUpTestCase() {}

    static constexpr const char * DIR_PREFIX_OF_TABLE = "/data/t/";
    static constexpr size_t TEST_NUMBER_FOR_FOLDER = 6;
    static constexpr size_t TEST_NUMBER_FOR_CHOOSE = 1000;
    static Strings getMultiTestPaths()
    {
        Strings paths;
        for (size_t i = 0; i < TEST_NUMBER_FOR_FOLDER; ++i)
            paths.emplace_back(TiFlashTestEnv::getTemporaryPath(fmt::format("/path_pool_test/data{}", i)));
        return paths;
    }

protected:
    LoggerPtr log;
};

TEST_F(PathPoolTest, AlignPaths)
try
{
    Strings paths = getMultiTestPaths();
    auto ctx = TiFlashTestEnv::getContext();

    PathPool pool(paths, paths, Strings{}, ctx->getPathCapacity(), ctx->getFileProvider());
    auto spool = pool.withTable("test", "t", false);

    // Stable delegate
    {
        auto delegate = spool.getStableDiskDelegator();
        auto res = delegate.listPaths();
        EXPECT_EQ(res.size(), paths.size());
        for (size_t i = 0; i < res.size(); ++i)
        {
            EXPECT_EQ(res[i], paths[i] + DIR_PREFIX_OF_TABLE + StoragePathPool::STABLE_FOLDER_NAME);
        }

        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            auto chosen = delegate.choosePath();
            ASSERT_NE(std::find(res.begin(), res.end(), chosen), res.end());
            delegate.addDTFile(i, 200, chosen);
            auto path_get = delegate.getDTFilePath(i);
            ASSERT_EQ(path_get, chosen);
        }

        for (const auto & r : res)
        {
            auto stat = std::get<0>(ctx->getPathCapacity()->getFsStatsOfPath(r));
            LOG_INFO(log, "[path={}] [used_size={}]", r, stat.used_size);
        }

        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            delegate.removeDTFile(i);
        }
    }
    // PS-multi delegate
    {
        auto delegate = spool.getPSDiskDelegatorMulti("log");
        auto res = delegate->listPaths();
        EXPECT_EQ(res.size(), paths.size());
        for (size_t i = 0; i < res.size(); ++i)
        {
            EXPECT_EQ(res[i], paths[i] + DIR_PREFIX_OF_TABLE + "log");
        }
        EXPECT_EQ(delegate->numPaths(), res.size());

        size_t bytes_written = 200;
        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            auto chosen = delegate->choosePath(id);
            ASSERT_NE(std::find(res.begin(), res.end(), chosen), res.end());
            delegate->addPageFileUsedSize(id, bytes_written, chosen, true);
            auto path_get = delegate->getPageFilePath(id);
            ASSERT_EQ(path_get, chosen);
        }

        for (const auto & r : res)
        {
            auto stat = std::get<0>(ctx->getPathCapacity()->getFsStatsOfPath(r));
            LOG_INFO(log, "[path={}] [used_size={}]", r, stat.used_size);
        }

        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            delegate->removePageFile(id, bytes_written, false, false);
        }
    }
    // PS-single delegate
    {
        auto delegate = spool.getPSDiskDelegatorSingle("meta");
        auto res = delegate->listPaths();
        EXPECT_EQ(res.size(), 1UL);
        for (size_t i = 0; i < res.size(); ++i)
        {
            EXPECT_EQ(res[i], paths[i] + DIR_PREFIX_OF_TABLE + "meta");
        }
        EXPECT_EQ(delegate->numPaths(), 1UL);

        size_t bytes_written = 200;
        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            auto chosen = delegate->choosePath(id);
            ASSERT_NE(std::find(res.begin(), res.end(), chosen), res.end());
            delegate->addPageFileUsedSize(id, bytes_written, chosen, true);
            auto path_get = delegate->getPageFilePath(id);
            ASSERT_EQ(path_get, chosen);
        }

        for (const auto & r : res)
        {
            auto stat = std::get<0>(ctx->getPathCapacity()->getFsStatsOfPath(r));
            LOG_INFO(log, "[path={}] [used_size={}]", r, stat.used_size);
        }

        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            delegate->removePageFile(id, bytes_written, false, false);
        }
    }
    // PS-Raft delegate
    {
        auto delegate = pool.getPSDiskDelegatorRaft();
        auto res = delegate->listPaths();
        EXPECT_EQ(res.size(), paths.size());
        for (size_t i = 0; i < res.size(); ++i)
        {
            EXPECT_EQ(res[i], paths[i] + "/kvstore");
        }
        EXPECT_EQ(delegate->numPaths(), res.size());

        size_t bytes_written = 200;
        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            auto chosen = delegate->choosePath(id);
            ASSERT_NE(std::find(res.begin(), res.end(), chosen), res.end());
            delegate->addPageFileUsedSize(id, bytes_written, chosen, true);
            auto path_get = delegate->getPageFilePath(id);
            ASSERT_EQ(path_get, chosen);
        }

        for (const auto & r : res)
        {
            auto stat = std::get<0>(ctx->getPathCapacity()->getFsStatsOfPath(r));
            LOG_INFO(log, "[path={}] [used_size={}]", r, stat.used_size);
        }

        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            delegate->removePageFile(id, bytes_written, false, false);
        }
    }
}
CATCH

TEST_F(PathPoolTest, UnalignPaths)
try
{
    Strings paths = getMultiTestPaths();
    Strings latest_paths(paths.begin(), paths.begin() + 1);
    auto ctx = TiFlashTestEnv::getContext();

    PathPool pool(paths, latest_paths, Strings{}, ctx->getPathCapacity(), ctx->getFileProvider());
    auto spool = pool.withTable("test", "t", false);
    // Stable delegate
    {
        auto delegate = spool.getStableDiskDelegator();
        auto res = delegate.listPaths();
        EXPECT_EQ(res.size(), paths.size());
        for (size_t i = 0; i < res.size(); ++i)
        {
            EXPECT_EQ(res[i], paths[i] + DIR_PREFIX_OF_TABLE + StoragePathPool::STABLE_FOLDER_NAME);
        }

        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            auto chosen = delegate.choosePath();
            ASSERT_NE(std::find(res.begin(), res.end(), chosen), res.end());
            delegate.addDTFile(i, 200, chosen);
            auto path_get = delegate.getDTFilePath(i);
            ASSERT_EQ(path_get, chosen);
        }

        for (const auto & r : res)
        {
            auto stat = std::get<0>(ctx->getPathCapacity()->getFsStatsOfPath(r));
            LOG_INFO(log, "[path={}] [used_size={}]", r, stat.used_size);
        }

        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            delegate.removeDTFile(i);
        }
    }
    // PS-multi delegate
    {
        auto delegate = spool.getPSDiskDelegatorMulti("log");
        auto res = delegate->listPaths();
        EXPECT_EQ(res.size(), latest_paths.size());
        for (size_t i = 0; i < res.size(); ++i)
        {
            EXPECT_EQ(res[i], latest_paths[i] + DIR_PREFIX_OF_TABLE + "log");
        }
        EXPECT_EQ(delegate->numPaths(), res.size());

        size_t bytes_written = 200;
        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            auto chosen = delegate->choosePath(id);
            ASSERT_NE(std::find(res.begin(), res.end(), chosen), res.end());
            delegate->addPageFileUsedSize(id, bytes_written, chosen, true);
            auto path_get = delegate->getPageFilePath(id);
            ASSERT_EQ(path_get, chosen);
        }

        for (const auto & r : res)
        {
            auto stat = std::get<0>(ctx->getPathCapacity()->getFsStatsOfPath(r));
            LOG_INFO(log, "[path={}] [used_size={}]", r, stat.used_size);
        }

        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            delegate->removePageFile(id, bytes_written, false, false);
        }
    }
    // PS-single delegate
    {
        auto delegate = spool.getPSDiskDelegatorSingle("meta");
        auto res = delegate->listPaths();
        EXPECT_EQ(res.size(), 1UL);
        for (size_t i = 0; i < res.size(); ++i)
        {
            EXPECT_EQ(res[i], paths[i] + DIR_PREFIX_OF_TABLE + "meta");
        }
        EXPECT_EQ(delegate->numPaths(), 1UL);

        size_t bytes_written = 200;
        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            auto chosen = delegate->choosePath(id);
            ASSERT_NE(std::find(res.begin(), res.end(), chosen), res.end());
            delegate->addPageFileUsedSize(id, bytes_written, chosen, true);
            auto path_get = delegate->getPageFilePath(id);
            ASSERT_EQ(path_get, chosen);
        }

        for (const auto & r : res)
        {
            auto stat = std::get<0>(ctx->getPathCapacity()->getFsStatsOfPath(r));
            LOG_INFO(log, "[path={}] [used_size={}]", r, stat.used_size);
        }

        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            delegate->removePageFile(id, bytes_written, false, false);
        }
    }
    // PS-Raft delegate
    {
        auto delegate = pool.getPSDiskDelegatorRaft();
        auto res = delegate->listPaths();
        EXPECT_EQ(res.size(), latest_paths.size());
        for (size_t i = 0; i < res.size(); ++i)
        {
            EXPECT_EQ(res[i], latest_paths[i] + "/kvstore");
        }
        EXPECT_EQ(delegate->numPaths(), res.size());

        size_t bytes_written = 200;
        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            auto chosen = delegate->choosePath(id);
            ASSERT_NE(std::find(res.begin(), res.end(), chosen), res.end());
            delegate->addPageFileUsedSize(id, bytes_written, chosen, true);
            auto path_get = delegate->getPageFilePath(id);
            ASSERT_EQ(path_get, chosen);
        }

        for (const auto & r : res)
        {
            auto stat = std::get<0>(ctx->getPathCapacity()->getFsStatsOfPath(r));
            LOG_INFO(log, "[path={}] [used_size={}]", r, stat.used_size);
        }

        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            delegate->removePageFile(id, bytes_written, false, false);
        }
    }
}
CATCH

TEST_F(PathPoolTest, FileLifecycle)
{
    Strings paths = getMultiTestPaths();
    Strings latest_paths(paths.begin(), paths.begin() + 1);
    auto ctx = TiFlashTestEnv::getContext();

    PathPool pool(paths, paths, Strings{}, ctx->getPathCapacity(), ctx->getFileProvider());
    auto delegator = pool.getPSDiskDelegatorGlobalMulti("log");
    PageFileIdAndLevel id_lvl{100, 0};
    // create new page data file
    const String chosen_path = delegator->choosePath(id_lvl);
    EXPECT_FALSE(delegator->fileExist(id_lvl));
    delegator->addPageFileUsedSize(id_lvl, 1024, chosen_path, true);
    // add size to page data file
    delegator->addPageFileUsedSize(id_lvl, 2048, chosen_path, false);
    // remove size to page data file
    delegator->freePageFileUsedSize(id_lvl, 2048, chosen_path);
    delegator->freePageFileUsedSize(id_lvl, 512, chosen_path);
    delegator->freePageFileUsedSize(id_lvl, 512, chosen_path);
    EXPECT_TRUE(delegator->fileExist(id_lvl));
    // get page data file path
    EXPECT_EQ(delegator->getPageFilePath(id_lvl), chosen_path);
    // add size to page data file
    delegator->addPageFileUsedSize(id_lvl, 256, chosen_path, false);
    // remove page data file
    delegator->removePageFile(id_lvl, 256, false, false);
    EXPECT_FALSE(delegator->fileExist(id_lvl));
}

class MockPathCapacityMetrics : public PathCapacityMetrics
{
public:
    MockPathCapacityMetrics(
        const size_t capacity_quota_,
        const Strings & main_paths_,
        const std::vector<size_t> main_capacity_quota_,
        const Strings & latest_paths_,
        const std::vector<size_t> latest_capacity_quota_)
        : PathCapacityMetrics(capacity_quota_, main_paths_, main_capacity_quota_, latest_paths_, latest_capacity_quota_)
    {}

    std::map<FSID, DiskCapacity> getDiskStats() override { return disk_stats_map; }

    void setDiskStats(std::map<FSID, DiskCapacity> & disk_stats_map_) { disk_stats_map = disk_stats_map_; }

private:
    std::map<FSID, DiskCapacity> disk_stats_map;
};

class PathCapacity : public DB::base::TiFlashStorageTestBasic
{
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        if (int code = statvfs(".", &vfs_info); code != 0)
        {
            FAIL() << "statvfs failed.";
        }

        main_data_path = getTemporaryPath() + "/main";
        createIfNotExist(main_data_path);

        latest_data_path = getTemporaryPath() + "/lastest";
        createIfNotExist(latest_data_path);
    }

    void TearDown() override
    {
        dropDataOnDisk(main_data_path);
        dropDataOnDisk(latest_data_path);
        TiFlashStorageTestBasic::TearDown();
    }

protected:
    struct statvfs vfs_info = {};
    std::string main_data_path;
    std::string latest_data_path;
};

TEST_F(PathCapacity, SingleDiskSinglePathTest)
{
    size_t capactity = 100;
    size_t used = 10;

    ASSERT_GE(vfs_info.f_bavail * vfs_info.f_frsize, capactity * 2);

    // Single disk with single path
    {
        auto capacity = PathCapacityMetrics(0, {main_data_path}, {capactity}, {latest_data_path}, {capactity});

        capacity.addUsedSize(main_data_path, used);
        auto stats = capacity.getFsStats(false);
        ASSERT_EQ(stats.capacity_size, capactity * 2);
        ASSERT_EQ(stats.used_size, used);
        ASSERT_EQ(stats.avail_size, capactity * 2 - used);

        auto main_path_stats = std::get<0>(capacity.getFsStatsOfPath(main_data_path));
        ASSERT_EQ(main_path_stats.capacity_size, capactity);
        ASSERT_EQ(main_path_stats.used_size, used);
        ASSERT_EQ(main_path_stats.avail_size, capactity - used);

        auto lastest_path_stats = std::get<0>(capacity.getFsStatsOfPath(latest_data_path));
        ASSERT_EQ(lastest_path_stats.capacity_size, capactity);
        ASSERT_EQ(lastest_path_stats.used_size, 0);
        ASSERT_EQ(lastest_path_stats.avail_size, capactity);
    }

    // Single disk with multi path
    {
        String main_data_path1 = getTemporaryPath() + "/main1";
        createIfNotExist(main_data_path1);
        String lastest_data_path1 = getTemporaryPath() + "/lastest1";
        createIfNotExist(lastest_data_path1);

        // Not use the capacity limit
        auto capacity = PathCapacityMetrics(
            0,
            {main_data_path, main_data_path1},
            {capactity * 2, capactity * 2},
            {latest_data_path, lastest_data_path1},
            {capactity, capactity});

        capacity.addUsedSize(main_data_path, used);
        capacity.addUsedSize(main_data_path1, used);
        capacity.addUsedSize(latest_data_path, used);

        auto stats = capacity.getFsStats(false);
        ASSERT_EQ(stats.capacity_size, capactity * 6);
        ASSERT_EQ(stats.used_size, 3 * used);
        ASSERT_EQ(stats.avail_size, capactity * 6 - (3 * used));

        dropDataOnDisk(main_data_path1);
        dropDataOnDisk(lastest_data_path1);
    }
}

TEST_F(PathCapacity, MultiDiskMultiPathTest)
{
    MockPathCapacityMetrics capacity = MockPathCapacityMetrics(0, {main_data_path}, {100}, {latest_data_path}, {100});

    std::map<FSID, DiskCapacity> disk_capacity_map;

    /// disk 1 :
    ///     - disk status:
    ///         - total size = 100 * 1
    ///         - avail size = 50 * 1
    ///     - path status:
    ///         - path1:
    ///             - capacity size : 100
    ///             - used size     : 4
    ///             - avail size    : 50  // min(capacity size - used size, disk avail size);
    ///         - path2:
    ///             - capacity size : 1000
    ///             - used size     : 12
    ///             - avail size    : 50  // min(capacity size - used size, disk avail size);
    struct statvfs fake_vfs = {};
    fake_vfs.f_blocks = 100;
    fake_vfs.f_bavail = 50;
    fake_vfs.f_frsize = 1;

    disk_capacity_map[100]
        = {.vfs_info = fake_vfs,
           .path_stats = {
               {.used_size = 4, .avail_size = 50, .capacity_size = 100, .ok = 1},
               {.used_size = 12, .avail_size = 50, .capacity_size = 1000, .ok = 1},
           }};
    capacity.setDiskStats(disk_capacity_map);
    FsStats total_stats = capacity.getFsStats(false);
    ASSERT_EQ(total_stats.capacity_size, 100);
    ASSERT_EQ(total_stats.used_size, 16);
    ASSERT_EQ(total_stats.avail_size, 50);

    /// disk 2:
    ///     - disk status:
    ///         - total size = 100 * 1
    ///         - avail size = 50 * 1
    ///     - path status:
    ///         - path1:
    ///             - capacity size : 48
    ///             - used size     : 40
    ///             - avail size    : 8  // min(capacity size - used size, disk avail size);
    ///         - path2:
    ///             - capacity size : 50
    ///             - used size     : 12
    ///             - avail size    : 38  // min(capacity size - used size, disk avail size);
    disk_capacity_map[101]
        = {.vfs_info = fake_vfs,
           .path_stats = {
               {.used_size = 40, .avail_size = 8, .capacity_size = 48, .ok = 1},
               {.used_size = 12, .avail_size = 38, .capacity_size = 50, .ok = 1},
           }};
    capacity.setDiskStats(disk_capacity_map);

    total_stats = capacity.getFsStats(false);
    ASSERT_EQ(total_stats.capacity_size, 100 + 98);
    ASSERT_EQ(total_stats.used_size, 16 + 52);
    ASSERT_EQ(total_stats.avail_size, 50 + 46);
}

TEST_F(PathCapacity, FsStats)
try
{
    size_t global_capacity_quota = 10;
    size_t capacity = 100;
    {
        PathCapacityMetrics
            path_capacity(global_capacity_quota, {main_data_path}, {capacity}, {latest_data_path}, {capacity});

        FsStats fs_stats = path_capacity.getFsStats(false);
        EXPECT_EQ(fs_stats.capacity_size, 2 * capacity); // summing the capacity of main and latest path
    }

    {
        PathCapacityMetrics path_capacity(global_capacity_quota, {main_data_path}, {}, {latest_data_path}, {});

        FsStats fs_stats = path_capacity.getFsStats(false);
        EXPECT_EQ(
            fs_stats.capacity_size,
            global_capacity_quota); // Use `global_capacity_quota` when `main_capacity_quota_` is empty
    }
}
CATCH

} // namespace tests
} // namespace DB
