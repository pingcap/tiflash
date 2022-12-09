#include <Common/Config/ConfigProcessor.h>
#include <Common/Config/TOMLConfiguration.h>
#include <Interpreters/Quota.h>
#include <Poco/Logger.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Server/StorageConfigParser.h>
#include <TestUtils/TiFlashTestBasic.h>

#define private public // hack for test
#include <Storages/PathCapacityMetrics.h>
#undef private

/// Suppress gcc warning: ‘*((void*)&<anonymous> +4)’ may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#include <Common/Config/cpptoml.h>
#if !__clang__
#pragma GCC diagnostic pop
#endif

namespace DB
{
namespace tests
{
static auto loadConfigFromString(const String & s)
{
    std::istringstream ss(s);
    cpptoml::parser p(ss);
    auto table = p.parse();
    Poco::AutoPtr<Poco::Util::LayeredConfiguration> config = new Poco::Util::LayeredConfiguration();
    config->add(new DB::TOMLConfiguration(table), /*shared=*/false); // Take ownership of TOMLConfig
    return config;
}

class StorageConfigTest : public ::testing::Test
{
public:
    StorageConfigTest() : log(&Poco::Logger::get("StorageConfigTest")) {}

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

        LOG_INFO(log, "parsing [index=" << i << "] [content=" << test_case << "]");

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
        PathCapacityMetrics path_capacity(global_capacity_quota, storage.main_data_paths, storage.main_capacity_quota,
            storage.latest_data_paths, storage.latest_capacity_quota);
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

        LOG_INFO(log, "parsing [index=" << i << "] [content=" << test_case << "]");

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
        PathCapacityMetrics path_capacity(global_capacity_quota, storage.main_data_paths, storage.main_capacity_quota,
            storage.latest_data_paths, storage.latest_capacity_quota);
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

        LOG_INFO(log, "parsing [index=" << i << "] [content=" << test_case << "]");

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
        PathCapacityMetrics path_capacity(global_capacity_quota, storage.main_data_paths, storage.main_capacity_quota,
            storage.latest_data_paths, storage.latest_capacity_quota);
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

        LOG_INFO(log, "parsing [index=" << i << "] [content=" << test_case << "]");

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
        PathCapacityMetrics path_capacity(global_capacity_quota, storage.main_data_paths, storage.main_capacity_quota,
            storage.latest_data_paths, storage.latest_capacity_quota);
    }
}
CATCH

TEST_F(StorageConfigTest, SSD_HDD_Settings)
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

        LOG_INFO(log, "parsing [index=" << i << "] [content=" << test_case << "]");

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
        PathCapacityMetrics path_capacity(global_capacity_quota, storage.main_data_paths, storage.main_capacity_quota,
            storage.latest_data_paths, storage.latest_capacity_quota);
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
# [storage.latest]
# dir = [ ]
# capacity = [ 10737418240, 10737418240 ]
# [storage.raft]
# dir = [ ]
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

        LOG_INFO(log, "parsing [index=" << i << "] [content=" << test_case << "]");

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
    Poco::Logger * log = &Poco::Logger::get("PathCapacityMetricsTest");

    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);

        LOG_INFO(log, "parsing [index=" << i << "] [content=" << test_case << "]");

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
        PathCapacityMetrics path_capacity(global_capacity_quota, storage.main_data_paths, storage.main_capacity_quota,
            storage.latest_data_paths, storage.latest_capacity_quota);

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

class UsersConfigParserTest : public ::testing::Test
{
public:
    UsersConfigParserTest() : log(&Poco::Logger::get("UsersConfigParserTest")) {}

    static void SetUpTestCase() {}

protected:
    Poco::Logger * log;
};


TEST_F(UsersConfigParserTest, ParseConfigs)
try
{
    Strings tests = {
        // case for original user settings
        R"(
[users]
[users.default]
password = ""
profile = "default"
quota = "default"
[users.default.networks]
ip = "::/0"

[users.readonly]
password = ""
profile = "readonly"
quota = "default"
[users.readonly.networks]
ip = "::/0"

[profiles]
[profiles.default]
load_balancing = "random"
max_memory_usage = 0
use_uncompressed_cache = 1
[profiles.readonly]
readonly = 1

[quotas]
[quotas.default]
[quotas.default.interval]
duration = 3600
errors = 0
execution_time = 0
queries = 0
read_rows = 0
result_rows = 0
)",
        // case for omit all default user settings
        R"(
)",
        // case for set some settings
        R"(
[profiles]
[profiles.default]
max_memory_usage = 123456
dt_enable_rough_set_filter = false
)",
    };

    // Ensure that connection is not blocked by any address
    const std::vector<std::string> test_addrs = {
        "127.0.0.1:443",
        "8.8.8.8:1080",
    };

    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);

        LOG_INFO(log, "parsing [index=" << i << "] [content=" << test_case << "]");

        // Reload users config with test case
        auto & global_ctx = TiFlashTestEnv::getGlobalContext();
        global_ctx.setUsersConfig(config);

        // Create a copy of global_ctx
        auto ctx = global_ctx;
        for (const auto & addr_ : test_addrs)
        {
            // Ensure "default" user can build connection
            Poco::Net::SocketAddress addr(addr_);

            // `setUser` will check user, password, address, update settings and quota for current user
            ASSERT_NO_THROW(ctx.setUser("default", "", addr, ""));
            const auto & settings = ctx.getSettingsRef();
            EXPECT_EQ(settings.use_uncompressed_cache, 1U);
            if (i == 2)
            {
                EXPECT_EQ(settings.max_memory_usage, 123456UL);
                EXPECT_FALSE(settings.dt_enable_rough_set_filter);
            }
            QuotaForIntervals * quota_raw_ptr = nullptr;
            ASSERT_NO_THROW(quota_raw_ptr = &ctx.getQuota(););
            ASSERT_NE(quota_raw_ptr, nullptr);

            // Won't block by database access right check
            ASSERT_NO_THROW(ctx.checkDatabaseAccessRights("system"));
            ASSERT_NO_THROW(ctx.checkDatabaseAccessRights("test"));
        }
    }
}
CATCH

} // namespace tests
} // namespace DB
