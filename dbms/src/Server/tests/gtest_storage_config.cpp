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

/// Suppress gcc warning: ‘*((void*)&<anonymous> +4)’ may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#include <cpptoml.h>
#if !__clang__
#pragma GCC diagnostic pop
#endif

#include <Common/Config/ConfigProcessor.h>
#include <Poco/Logger.h>
#include <Server/StorageConfigParser.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/PathCapacityMetrics.h>
#include <TestUtils/ConfigTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
namespace DB
{
namespace tests
{
class StorageConfigTest : public ::testing::Test
{
public:
    StorageConfigTest()
        : log(&Poco::Logger::get("StorageConfigTest"))
    {}

    static void SetUpTestCase() {}

protected:
    Poco::Logger * log;
};

TEST_F(StorageConfigTest, SimpleSinglePath)
try
{
    Strings tests = {
        // Deprecated style
        R"(
path="/data0/tiflash"
        )",
        // Deprecated style with capacity
        R"(
path="/data0/tiflash"
capacity=1024000000
        )",
        // New style
        R"(
[storage]
[storage.main]
dir=["/data0/tiflash"]
        )",
    };

    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);

        LOG_FMT_INFO(log, "parsing [index={}] [content={}]", i, test_case);

        size_t global_capacity_quota = 0;
        TiFlashStorageConfig storage;
        std::tie(global_capacity_quota, storage) = TiFlashStorageConfig::parseSettings(*config, log);

        ASSERT_EQ(storage.main_data_paths.size(), 1);
        EXPECT_EQ(storage.main_data_paths[0], "/data0/tiflash/");

        ASSERT_EQ(storage.latest_data_paths.size(), 1);
        EXPECT_EQ(storage.latest_data_paths[0], "/data0/tiflash/");

        ASSERT_EQ(storage.kvstore_data_path.size(), 1);
        EXPECT_EQ(storage.kvstore_data_path[0], "/data0/tiflash/kvstore/");

        auto all_paths = storage.getAllNormalPaths();
        EXPECT_EQ(all_paths[0], "/data0/tiflash/");

        // Ensure that creating PathCapacityMetrics is OK.
        PathCapacityMetrics path_capacity(global_capacity_quota, storage.main_data_paths, storage.main_capacity_quota, storage.latest_data_paths, storage.latest_capacity_quota);
    }
}
CATCH

TEST_F(StorageConfigTest, ExplicitKVStorePath)
try
{
    Strings tests = {
        // Deprecated style
        R"(
path="/data0/tiflash"
[raft]
kvstore_path="/data1111/kvstore"
        )",
        // New style
        R"(
[storage]
[storage.main]
dir=["/data0/tiflash"]
[storage.raft]
dir=["/data1111/kvstore"]
        )",
        // New style with remaining `raft.kvstore_path`, will be overwrite for backward compatibility
        R"(
[raft]
kvstore_path="/data1111/kvstore"
[storage]
[storage.main]
dir=["/data0/tiflash"]
[storage.raft]
dir=["/data222/kvstore"]
        )",
    };

    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);

        LOG_FMT_INFO(log, "parsing [index={}] [content={}]", i, test_case);

        size_t global_capacity_quota = 0;
        TiFlashStorageConfig storage;
        std::tie(global_capacity_quota, storage) = TiFlashStorageConfig::parseSettings(*config, log);

        ASSERT_EQ(storage.main_data_paths.size(), 1);
        EXPECT_EQ(storage.main_data_paths[0], "/data0/tiflash/");

        ASSERT_EQ(storage.latest_data_paths.size(), 1);
        EXPECT_EQ(storage.latest_data_paths[0], "/data0/tiflash/");

        ASSERT_EQ(storage.kvstore_data_path.size(), 1);
        EXPECT_EQ(storage.kvstore_data_path[0], "/data1111/kvstore/");

        auto all_paths = storage.getAllNormalPaths();
        EXPECT_EQ(all_paths[0], "/data0/tiflash/");

        // Ensure that creating PathCapacityMetrics is OK.
        PathCapacityMetrics path_capacity(global_capacity_quota, storage.main_data_paths, storage.main_capacity_quota, storage.latest_data_paths, storage.latest_capacity_quota);
    }
}
CATCH

TEST_F(StorageConfigTest, MultiSSDSettings)
try
{
    Strings tests = {
        // Deprecated style
        R"(
path="/data0/tiflash,/data1/tiflash,/data2/tiflash"
path_realtime_mode = false # default value
        )",
        // New style
        R"(
[storage]
[storage.main]
dir=["/data0/tiflash", "/data1/tiflash", "/data2/tiflash"]
[storage.latest]
dir=["/data0/tiflash"]
        )",
    };

    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);

        LOG_FMT_INFO(log, "parsing [index={}] [content={}]", i, test_case);

        size_t global_capacity_quota = 0;
        TiFlashStorageConfig storage;
        std::tie(global_capacity_quota, storage) = TiFlashStorageConfig::parseSettings(*config, log);

        ASSERT_EQ(storage.main_data_paths.size(), 3);
        EXPECT_EQ(storage.main_data_paths[0], "/data0/tiflash/");
        EXPECT_EQ(storage.main_data_paths[1], "/data1/tiflash/");
        EXPECT_EQ(storage.main_data_paths[2], "/data2/tiflash/");

        ASSERT_EQ(storage.latest_data_paths.size(), 1);
        EXPECT_EQ(storage.latest_data_paths[0], "/data0/tiflash/");

        ASSERT_EQ(storage.kvstore_data_path.size(), 1);
        EXPECT_EQ(storage.kvstore_data_path[0], "/data0/tiflash/kvstore/");

        auto all_paths = storage.getAllNormalPaths();
        EXPECT_EQ(all_paths[0], "/data0/tiflash/");

        // Ensure that creating PathCapacityMetrics is OK.
        PathCapacityMetrics path_capacity(global_capacity_quota, storage.main_data_paths, storage.main_capacity_quota, storage.latest_data_paths, storage.latest_capacity_quota);
    }
}
CATCH

TEST_F(StorageConfigTest, MultiNVMeSSDSettings)
try
{
    Strings tests = {
        R"(
[storage]
[storage.main]
dir=["/data0/tiflash", "/data1/tiflash", "/data2/tiflash"]
        )",
        R"(
[storage]
[storage.main]
dir=["/data0/tiflash", "/data1/tiflash", "/data2/tiflash"]
[storage.latest]
dir=["/data0/tiflash", "/data1/tiflash", "/data2/tiflash"]
        )",
    };

    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);

        LOG_FMT_INFO(log, "parsing [index={}] [content={}]", i, test_case);

        size_t global_capacity_quota = 0;
        TiFlashStorageConfig storage;
        std::tie(global_capacity_quota, storage) = TiFlashStorageConfig::parseSettings(*config, log);

        ASSERT_EQ(storage.main_data_paths.size(), 3);
        EXPECT_EQ(storage.main_data_paths[0], "/data0/tiflash/");
        EXPECT_EQ(storage.main_data_paths[1], "/data1/tiflash/");
        EXPECT_EQ(storage.main_data_paths[2], "/data2/tiflash/");

        ASSERT_EQ(storage.latest_data_paths.size(), 3);
        EXPECT_EQ(storage.latest_data_paths[0], "/data0/tiflash/");
        EXPECT_EQ(storage.latest_data_paths[1], "/data1/tiflash/");
        EXPECT_EQ(storage.latest_data_paths[2], "/data2/tiflash/");

        ASSERT_EQ(storage.kvstore_data_path.size(), 3);
        EXPECT_EQ(storage.kvstore_data_path[0], "/data0/tiflash/kvstore/");
        EXPECT_EQ(storage.kvstore_data_path[1], "/data1/tiflash/kvstore/");
        EXPECT_EQ(storage.kvstore_data_path[2], "/data2/tiflash/kvstore/");

        auto all_paths = storage.getAllNormalPaths();
        EXPECT_EQ(all_paths[0], "/data0/tiflash/");

        // Ensure that creating PathCapacityMetrics is OK.
        PathCapacityMetrics path_capacity(global_capacity_quota, storage.main_data_paths, storage.main_capacity_quota, storage.latest_data_paths, storage.latest_capacity_quota);
    }
}
CATCH

TEST_F(StorageConfigTest, SSDHDDSettings)
try
{
    Strings tests = {
        // Deprecated style
        R"(
path="/ssd0/tiflash,/hdd0/tiflash,/hdd1/tiflash"
path_realtime_mode = true
        )",
        // New style
        R"(
[storage]
[storage.main]
dir=["/hdd0/tiflash", "/hdd1/tiflash", ]
[storage.latest]
dir=["/ssd0/tiflash"]
        )",
    };

    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);

        LOG_FMT_INFO(log, "parsing [index={}] [content={}]", i, test_case);

        size_t global_capacity_quota = 0;
        TiFlashStorageConfig storage;
        std::tie(global_capacity_quota, storage) = TiFlashStorageConfig::parseSettings(*config, log);

        ASSERT_EQ(storage.main_data_paths.size(), 2);
        EXPECT_EQ(storage.main_data_paths[0], "/hdd0/tiflash/");
        EXPECT_EQ(storage.main_data_paths[1], "/hdd1/tiflash/");

        ASSERT_EQ(storage.latest_data_paths.size(), 1);
        EXPECT_EQ(storage.latest_data_paths[0], "/ssd0/tiflash/");

        ASSERT_EQ(storage.kvstore_data_path.size(), 1);
        EXPECT_EQ(storage.kvstore_data_path[0], "/ssd0/tiflash/kvstore/");

        auto all_paths = storage.getAllNormalPaths();
        EXPECT_EQ(all_paths[0], "/ssd0/tiflash/");

        // Ensure that creating PathCapacityMetrics is OK.
        PathCapacityMetrics path_capacity(global_capacity_quota, storage.main_data_paths, storage.main_capacity_quota, storage.latest_data_paths, storage.latest_capacity_quota);
    }
}
CATCH

TEST_F(StorageConfigTest, ParseMaybeBrokenCases)
try
{
    Strings tests = {
        // case for storage.main.dir is defined but empty
        R"(
path = "/tmp/tiflash/data/db"
[storage]
[storage.main]
# empty storage.main.dir
dir = [ ]
# capacity = [ 10737418240, 10737418240 ]
# [storage.latest]
# dir = [ ]
# capacity = [ 10737418240, 10737418240 ]
# [storage.raft]
# dir = [ ]
        )",
        // case for storage.main.dir is not defined
        R"(
path = "/data0/tiflash,/data1/tiflash"
[storage]
[storage.main]
# not defined storage.main.dir
# dir = [ "/data0/tiflash", "/data1/tiflash" ]
# capacity = [ 10737418240, 10737418240 ]
# [storage.latest]
# dir = [ ]
# capacity = [ 10737418240, 10737418240 ]
# [storage.raft]
# dir = [ ]
        )",
        // case for the length of storage.main.dir is not the same with storage.main.capacity
        R"(
path = "/data0/tiflash,/data1/tiflash"
[storage]
[storage.main]
dir = [ "/data0/tiflash", "/data1/tiflash" ]
capacity = [ 10737418240 ]
        )",
        // case for the length of storage.latest.dir is not the same with storage.latest.capacity
        R"(
path = "/data0/tiflash,/data1/tiflash"
[storage]
[storage.main]
dir = [ "/data0/tiflash", "/data1/tiflash" ]
capacity = [ 10737418240, 10737418240 ]
[storage.latest]
dir = [ "/data0/tiflash", "/data1/tiflash" ]
capacity = [ 10737418240 ]
        )",
        // case for storage.main.dir is not an string array
        R"(
[storage]
[storage.main]
dir = "/data0/tiflash,/data1/tiflash"
        )",
        // case for storage.latest.dir is not an string array
        R"(
[storage]
[storage.main]
dir = [ "/data0/tiflash", "/data1/tiflash" ] 
[storage.latest]
dir = "/data0/tiflash"
        )",
        // case for storage.raft.dir is not an string array
        R"(
[storage]
[storage.main]
dir = [ "/data0/tiflash", "/data1/tiflash" ] 
[storage.raft]
dir = "/data0/tiflash"
        )",
        // case for storage.main.dir is not an string array
        R"(
[storage]
[storage.main]
dir = 123
        )",
        // case for storage.main.dir is not an string array
        R"(
[storage]
[storage.main]
dir = [["/data0/tiflash", "/data1/tiflash"], ["/data2/tiflash", ]]
        )",
        // case for storage.main.dir is not an string array
        R"(
[storage]
[storage.main]
dir = [1,2,3]
        )",
    };

    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);

        LOG_FMT_INFO(log, "parsing [index={}] [content={}]", i, test_case);

        size_t global_capacity_quota = 0;
        TiFlashStorageConfig storage;
        ASSERT_ANY_THROW({ std::tie(global_capacity_quota, storage) = TiFlashStorageConfig::parseSettings(*config, log); });
    }
}
CATCH

TEST(PathCapacityMetricsTest, Quota)
try
{
    Strings tests = {
        // case for keep unlimited 1
        R"(
[storage]
[storage.main]
dir=["/data0/tiflash", "/data1/tiflash", "/data2/tiflash"]
capacity=[ 0, 3072, 4196 ]
[storage.latest]
dir=["/data0/tiflash"]
capacity=[ 1024 ]
        )",
        // case for keep unlimited 2
        R"(
[storage]
[storage.main]
dir=["/data0/tiflash", "/data1/tiflash", "/data2/tiflash"]
capacity=[ 2048, 3072, 4196 ]
[storage.latest]
dir=["/data0/tiflash"]
capacity=[ 0 ]
        )",
        // case for use the largest capacity when there are multiple capacity for one path
        R"(
[storage]
[storage.main]
dir=["/data0/tiflash", "/data1/tiflash", "/data2/tiflash"]
capacity=[ 2048, 3072, 4196 ]
[storage.latest]
dir=["/data0/tiflash"]
capacity=[ 1024 ]
        )",
    };
    Poco::Logger * log = &Poco::Logger::get("PathCapacityMetrics_test");

    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);

        LOG_FMT_INFO(log, "parsing [index={}] [content={}]", i, test_case);

        size_t global_capacity_quota = 0;
        TiFlashStorageConfig storage;
        std::tie(global_capacity_quota, storage) = TiFlashStorageConfig::parseSettings(*config, log);

        ASSERT_EQ(storage.main_data_paths.size(), 3);
        EXPECT_EQ(storage.main_data_paths[0], "/data0/tiflash/");
        EXPECT_EQ(storage.main_data_paths[1], "/data1/tiflash/");
        EXPECT_EQ(storage.main_data_paths[2], "/data2/tiflash/");

        ASSERT_EQ(storage.latest_data_paths.size(), 1);
        EXPECT_EQ(storage.latest_data_paths[0], "/data0/tiflash/");

        auto all_paths = storage.getAllNormalPaths();
        EXPECT_EQ(all_paths[0], "/data0/tiflash/");

        // Ensure that creating PathCapacityMetrics is OK.
        PathCapacityMetrics path_capacity(global_capacity_quota, storage.main_data_paths, storage.main_capacity_quota, storage.latest_data_paths, storage.latest_capacity_quota);

        auto idx = path_capacity.locatePath("/data0/tiflash/");
        ASSERT_NE(idx, PathCapacityMetrics::INVALID_INDEX);
        switch (i)
        {
        case 0:
        case 1:
            EXPECT_EQ(path_capacity.path_infos[idx].capacity_bytes, 0);
            break;
        case 2:
            EXPECT_EQ(path_capacity.path_infos[idx].capacity_bytes, 2048);
            break;
        }
        idx = path_capacity.locatePath("/data1/tiflash/");
        ASSERT_NE(idx, PathCapacityMetrics::INVALID_INDEX);
        EXPECT_EQ(path_capacity.path_infos[idx].capacity_bytes, 3072);
        idx = path_capacity.locatePath("/data2/tiflash/");
        ASSERT_NE(idx, PathCapacityMetrics::INVALID_INDEX);
        EXPECT_EQ(path_capacity.path_infos[idx].capacity_bytes, 4196);
    }
}
CATCH

TEST_F(StorageConfigTest, CompatibilityWithIORateLimitConfig)
try
{
    Strings tests = {
        R"(
path = "/tmp/tiflash/data/db0/,/tmp/tiflash/data/db1/"
[storage]
format_version = 123
lazily_init_store = 1
        )",
        R"(
path = "/tmp/tiflash/data/db0/,/tmp/tiflash/data/db1/"
[storage]
format_version = 123
lazily_init_store = 1
[storage.main]
dir = [ "/data0/tiflash/", "/data1/tiflash/" ]
        )",
        R"(
path = "/data0/tiflash/,/data1/tiflash/"
[storage]
format_version = 123
lazily_init_store = 1
[storage.io_rate_limit]
max_bytes_per_sec=1024000
        )",
    };

    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);
        LOG_FMT_INFO(log, "parsing [index={}] [content={}]", i, test_case);
        auto [global_capacity_quota, storage] = TiFlashStorageConfig::parseSettings(*config, log);
        std::ignore = global_capacity_quota;
        Strings paths;
        if (i == 0)
        {
            paths = Strings{"/tmp/tiflash/data/db0/", "/tmp/tiflash/data/db1/"};
        }
        else if (i == 1)
        {
            paths = Strings{"/data0/tiflash/", "/data1/tiflash/"};
        }
        else if (i == 2)
        {
            paths = Strings{"/data0/tiflash/", "/data1/tiflash/"};
        }
        ASSERT_EQ(storage.main_data_paths, paths);
        ASSERT_EQ(storage.format_version, 123);
        ASSERT_EQ(storage.lazily_init_store, 1);
    }
}
CATCH

TEST(StorageIORateLimitConfigTest, StorageIORateLimitConfig)
try
{
    Strings tests = {
        R"(
[storage]
[storage.io_rate_limit]
max_bytes_per_sec=0
max_read_bytes_per_sec=0
max_write_bytes_per_sec=0
foreground_write_weight=1
background_write_weight=2
foreground_read_weight=5
background_read_weight=2
        )",
        R"(
[storage]
[storage.io_rate_limit]
max_bytes_per_sec=1024000
max_read_bytes_per_sec=0
max_write_bytes_per_sec=0
foreground_write_weight=1
background_write_weight=2
foreground_read_weight=5
background_read_weight=2
        )",
        R"(
[storage]
[storage.io_rate_limit]
max_bytes_per_sec=0
max_read_bytes_per_sec=1024000
max_write_bytes_per_sec=1024000
foreground_write_weight=1
background_write_weight=2
foreground_read_weight=5
background_read_weight=2
        )",
        R"(
[storage]
[storage.io_rate_limit]
max_bytes_per_sec=1024000
max_read_bytes_per_sec=1024000
max_write_bytes_per_sec=1024000
foreground_write_weight=1
background_write_weight=2
foreground_read_weight=5
background_read_weight=2
        )",
    };

    Poco::Logger * log = &Poco::Logger::get("StorageIORateLimitConfigTest");

    auto verify_default = [](const StorageIORateLimitConfig & io_config) {
        ASSERT_EQ(io_config.max_bytes_per_sec, 0);
        ASSERT_EQ(io_config.max_read_bytes_per_sec, 0);
        ASSERT_EQ(io_config.max_write_bytes_per_sec, 0);
        ASSERT_TRUE(io_config.use_max_bytes_per_sec);
        ASSERT_EQ(io_config.fg_write_weight, 25);
        ASSERT_EQ(io_config.bg_write_weight, 25);
        ASSERT_EQ(io_config.fg_read_weight, 25);
        ASSERT_EQ(io_config.bg_read_weight, 25);
        ASSERT_EQ(io_config.readWeight(), 50);
        ASSERT_EQ(io_config.writeWeight(), 50);
        ASSERT_EQ(io_config.totalWeight(), 100);
        ASSERT_EQ(io_config.getFgReadMaxBytesPerSec(), 0);
        ASSERT_EQ(io_config.getFgWriteMaxBytesPerSec(), 0);
        ASSERT_EQ(io_config.getBgReadMaxBytesPerSec(), 0);
        ASSERT_EQ(io_config.getBgWriteMaxBytesPerSec(), 0);
    };

    auto verify_case0 = [](const StorageIORateLimitConfig & io_config) {
        ASSERT_EQ(io_config.max_bytes_per_sec, 0);
        ASSERT_EQ(io_config.max_read_bytes_per_sec, 0);
        ASSERT_EQ(io_config.max_write_bytes_per_sec, 0);
        ASSERT_TRUE(io_config.use_max_bytes_per_sec);
        ASSERT_EQ(io_config.fg_write_weight, 1);
        ASSERT_EQ(io_config.bg_write_weight, 2);
        ASSERT_EQ(io_config.fg_read_weight, 5);
        ASSERT_EQ(io_config.bg_read_weight, 2);
        ASSERT_EQ(io_config.readWeight(), 7);
        ASSERT_EQ(io_config.writeWeight(), 3);
        ASSERT_EQ(io_config.totalWeight(), 10);
        ASSERT_EQ(io_config.getFgReadMaxBytesPerSec(), 0);
        ASSERT_EQ(io_config.getFgWriteMaxBytesPerSec(), 0);
        ASSERT_EQ(io_config.getBgReadMaxBytesPerSec(), 0);
        ASSERT_EQ(io_config.getBgWriteMaxBytesPerSec(), 0);
    };

    auto verify_case1 = [](const StorageIORateLimitConfig & io_config) {
        ASSERT_EQ(io_config.max_bytes_per_sec, 1024000);
        ASSERT_EQ(io_config.max_read_bytes_per_sec, 0);
        ASSERT_EQ(io_config.max_write_bytes_per_sec, 0);
        ASSERT_TRUE(io_config.use_max_bytes_per_sec);
        ASSERT_EQ(io_config.fg_write_weight, 1);
        ASSERT_EQ(io_config.bg_write_weight, 2);
        ASSERT_EQ(io_config.fg_read_weight, 5);
        ASSERT_EQ(io_config.bg_read_weight, 2);
        ASSERT_EQ(io_config.readWeight(), 7);
        ASSERT_EQ(io_config.writeWeight(), 3);
        ASSERT_EQ(io_config.totalWeight(), 10);
        ASSERT_EQ(io_config.getFgWriteMaxBytesPerSec(), 102400);
        ASSERT_EQ(io_config.getBgWriteMaxBytesPerSec(), 102400 * 2);
        ASSERT_EQ(io_config.getFgReadMaxBytesPerSec(), 102400 * 5);
        ASSERT_EQ(io_config.getBgReadMaxBytesPerSec(), 102400 * 2);
    };

    auto verify_case2 = [](const StorageIORateLimitConfig & io_config) {
        ASSERT_EQ(io_config.max_bytes_per_sec, 0);
        ASSERT_EQ(io_config.max_read_bytes_per_sec, 1024000);
        ASSERT_EQ(io_config.max_write_bytes_per_sec, 1024000);
        ASSERT_FALSE(io_config.use_max_bytes_per_sec);
        ASSERT_EQ(io_config.fg_write_weight, 1);
        ASSERT_EQ(io_config.bg_write_weight, 2);
        ASSERT_EQ(io_config.fg_read_weight, 5);
        ASSERT_EQ(io_config.bg_read_weight, 2);
        ASSERT_EQ(io_config.readWeight(), 7);
        ASSERT_EQ(io_config.writeWeight(), 3);
        ASSERT_EQ(io_config.totalWeight(), 10);
        ASSERT_EQ(io_config.getFgReadMaxBytesPerSec(), 731428);
        ASSERT_EQ(io_config.getFgWriteMaxBytesPerSec(), 341333);
        ASSERT_EQ(io_config.getBgReadMaxBytesPerSec(), 292571);
        ASSERT_EQ(io_config.getBgWriteMaxBytesPerSec(), 682666);
    };

    auto verify_case3 = [](const StorageIORateLimitConfig & io_config) {
        ASSERT_EQ(io_config.max_bytes_per_sec, 1024000);
        ASSERT_EQ(io_config.max_read_bytes_per_sec, 1024000);
        ASSERT_EQ(io_config.max_write_bytes_per_sec, 1024000);
        ASSERT_TRUE(io_config.use_max_bytes_per_sec);
        ASSERT_EQ(io_config.fg_write_weight, 1);
        ASSERT_EQ(io_config.bg_write_weight, 2);
        ASSERT_EQ(io_config.fg_read_weight, 5);
        ASSERT_EQ(io_config.bg_read_weight, 2);
        ASSERT_EQ(io_config.readWeight(), 7);
        ASSERT_EQ(io_config.writeWeight(), 3);
        ASSERT_EQ(io_config.totalWeight(), 10);
        ASSERT_EQ(io_config.getFgReadMaxBytesPerSec(), 102400);
        ASSERT_EQ(io_config.getFgWriteMaxBytesPerSec(), 102400 * 2);
        ASSERT_EQ(io_config.getBgReadMaxBytesPerSec(), 102400 * 5);
        ASSERT_EQ(io_config.getBgWriteMaxBytesPerSec(), 102400 * 2);
    };

    std::vector<std::function<void(const StorageIORateLimitConfig &)>> case_verifiers;
    case_verifiers.push_back(verify_case0);
    case_verifiers.push_back(verify_case1);
    case_verifiers.push_back(verify_case2);
    case_verifiers.push_back(verify_case3);

    for (size_t i = 0; i < 2u /*tests.size()*/; ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);

        LOG_FMT_INFO(log, "parsing [index={}] [content={}]", i, test_case);
        ASSERT_TRUE(config->has("storage.io_rate_limit"));

        StorageIORateLimitConfig io_config;
        verify_default(io_config);
        io_config.parse(config->getString("storage.io_rate_limit"), log);
        case_verifiers[i](io_config);
    }
}
CATCH
} // namespace tests
} // namespace DB
