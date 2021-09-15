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
#include <Common/Config/TOMLConfiguration.h>
#include <Common/TiflashLogFileChannel.h>
#include <Interpreters/Quota.h>
#include <Poco/Ext/LevelFilterChannel.h>
#include <Poco/Logger.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Server/StorageConfigParser.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionManager.h>
#include <Storages/Transaction/RegionPersister.h>
#include <Storages/Transaction/TiKVRecordFormat.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <daemon/BaseDaemon.h>
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

class StorageConfig_test : public ::testing::Test
{
public:
    StorageConfig_test()
        : log(&Poco::Logger::get("StorageConfig_test"))
    {}

    static void SetUpTestCase() {}

protected:
    Poco::Logger * log;
};

TEST_F(StorageConfig_test, MultiSSDSettings)
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

        ASSERT_EQ(storage.main_data_paths.size(), 3UL);
        EXPECT_EQ(storage.main_data_paths[0], "/data0/tiflash/");
        EXPECT_EQ(storage.main_data_paths[1], "/data1/tiflash/");
        EXPECT_EQ(storage.main_data_paths[2], "/data2/tiflash/");

        ASSERT_EQ(storage.latest_data_paths.size(), 1UL);
        EXPECT_EQ(storage.latest_data_paths[0], "/data0/tiflash/");

        auto all_paths = storage.getAllNormalPaths();
        EXPECT_EQ(all_paths[0], "/data0/tiflash/");

        // Ensure that creating PathCapacityMetrics is OK.
        PathCapacityMetrics path_capacity(global_capacity_quota, storage.main_data_paths, storage.main_capacity_quota, storage.latest_data_paths, storage.latest_capacity_quota);
    }
}
CATCH

TEST_F(StorageConfig_test, SSD_HDD_Settings)
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

        ASSERT_EQ(storage.main_data_paths.size(), 2UL);
        EXPECT_EQ(storage.main_data_paths[0], "/hdd0/tiflash/");
        EXPECT_EQ(storage.main_data_paths[1], "/hdd1/tiflash/");

        ASSERT_EQ(storage.latest_data_paths.size(), 1UL);
        EXPECT_EQ(storage.latest_data_paths[0], "/ssd0/tiflash/");

        auto all_paths = storage.getAllNormalPaths();
        EXPECT_EQ(all_paths[0], "/ssd0/tiflash/");

        // Ensure that creating PathCapacityMetrics is OK.
        PathCapacityMetrics path_capacity(global_capacity_quota, storage.main_data_paths, storage.main_capacity_quota, storage.latest_data_paths, storage.latest_capacity_quota);
    }
}
CATCH

TEST_F(StorageConfig_test, ParseMaybeBrokenCases)
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

TEST(PathCapacityMetrics_test, Quota)
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

        LOG_INFO(log, "parsing [index=" << i << "] [content=" << test_case << "]");

        size_t global_capacity_quota = 0;
        TiFlashStorageConfig storage;
        std::tie(global_capacity_quota, storage) = TiFlashStorageConfig::parseSettings(*config, log);

        ASSERT_EQ(storage.main_data_paths.size(), 3UL);
        EXPECT_EQ(storage.main_data_paths[0], "/data0/tiflash/");
        EXPECT_EQ(storage.main_data_paths[1], "/data1/tiflash/");
        EXPECT_EQ(storage.main_data_paths[2], "/data2/tiflash/");

        ASSERT_EQ(storage.latest_data_paths.size(), 1UL);
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
            EXPECT_EQ(path_capacity.path_infos[idx].capacity_bytes, 0UL);
            break;
        case 2:
            EXPECT_EQ(path_capacity.path_infos[idx].capacity_bytes, 2048UL);
            break;
        }
        idx = path_capacity.locatePath("/data1/tiflash/");
        ASSERT_NE(idx, PathCapacityMetrics::INVALID_INDEX);
        EXPECT_EQ(path_capacity.path_infos[idx].capacity_bytes, 3072UL);
        idx = path_capacity.locatePath("/data2/tiflash/");
        ASSERT_NE(idx, PathCapacityMetrics::INVALID_INDEX);
        EXPECT_EQ(path_capacity.path_infos[idx].capacity_bytes, 4196UL);
    }
}
CATCH

class UsersConfigParser_test : public ::testing::Test
{
public:
    UsersConfigParser_test()
        : log(&Poco::Logger::get("UsersConfigParser_test"))
    {
        auto & global_ctx = TiFlashTestEnv::getGlobalContext();
        origin_settings = global_ctx.getSettings();
    }

    static void SetUpTestCase() {}

protected:
    Poco::Logger * log;
    Settings origin_settings;
};


TEST_F(UsersConfigParser_test, ParseConfigs)
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

TEST_F(StorageConfig_test, CompatibilityWithIORateLimitConfig)
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
        LOG_INFO(log, "parsing [index=" << i << "] [content=" << test_case << "]");
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

TEST(StorageIORateLimitConfig_test, StorageIORateLimitConfig)
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

    Poco::Logger * log = &Poco::Logger::get("StorageIORateLimitConfig_test");

    auto verifyDefault = [](const StorageIORateLimitConfig & io_config) {
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

    auto verifyCase0 = [](const StorageIORateLimitConfig & io_config) {
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

    auto verifyCase1 = [](const StorageIORateLimitConfig & io_config) {
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

    auto verifyCase2 = [](const StorageIORateLimitConfig & io_config) {
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

    auto verifyCase3 = [](const StorageIORateLimitConfig & io_config) {
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
    case_verifiers.push_back(verifyCase0);
    case_verifiers.push_back(verifyCase1);
    case_verifiers.push_back(verifyCase2);
    case_verifiers.push_back(verifyCase3);

    for (size_t i = 0; i < 2u /*tests.size()*/; ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);

        LOG_INFO(log, "parsing [index=" << i << "] [content=" << test_case << "]");
        ASSERT_TRUE(config->has("storage.io_rate_limit"));

        StorageIORateLimitConfig io_config;
        verifyDefault(io_config);
        io_config.parse(config->getString("storage.io_rate_limit"), log);
        case_verifiers[i](io_config);
    }
}
CATCH

TEST_F(UsersConfigParser_test, ReloadDtConfig)
try
{
    Strings tests = {
        R"(
[profiles]
[profiles.default]
max_rows_in_set = 455
dt_segment_limit_rows = 1000005
dt_enable_rough_set_filter = 0
max_memory_usage = 102000
dt_storage_pool_data_gc_min_file_num = 8
dt_storage_pool_data_gc_min_legacy_num = 2
dt_storage_pool_data_gc_min_bytes = 256
dt_storage_pool_data_gc_max_valid_rate = 0.5
        )"};

    auto & global_ctx = TiFlashTestEnv::getGlobalContext();
    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);

        LOG_INFO(log, "parsing [index=" << i << "] [content=" << test_case << "]");

        // Reload users config with test case
        global_ctx.reloadDeltaTreeConfig(*config);
        // Only reload the configuration of the storage module starting with "dt" in Settings.h
        ASSERT_EQ(global_ctx.getSettingsRef().max_rows_in_set, 0);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_enable_rough_set_filter, 0);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_segment_limit_rows, 1000005);
        ASSERT_EQ(global_ctx.getSettingsRef().max_memory_usage, 0);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_min_file_num, 8);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_min_legacy_num, 2);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_min_bytes, 256);
        ASSERT_FLOAT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_max_valid_rate, 0.5);
    }
    global_ctx.setSettings(origin_settings);
}
CATCH

TEST_F(UsersConfigParser_test, ReloadPersisterConfig)
try
{
    Strings tests = {
        R"(
[profiles]
[profiles.default]
max_rows_in_set = 455
dt_segment_limit_rows = 1000005
dt_enable_rough_set_filter = 0
max_memory_usage = 102000
dt_storage_pool_data_gc_min_file_num = 8
dt_storage_pool_data_gc_min_legacy_num = 2
dt_storage_pool_data_gc_min_bytes = 256
dt_storage_pool_data_gc_max_valid_rate = 0.5
dt_open_file_max_idle_seconds = 20
dt_page_gc_low_write_prob = 0.2
        )"};
    auto & global_ctx = TiFlashTestEnv::getGlobalContext();
    RegionManager region_manager;
    RegionPersister persister(global_ctx, region_manager);
    persister.restore(nullptr, PageStorage::Config{});

    auto verifyPersisterReloadConfig = [&global_ctx](RegionPersister & persister) {
        DB::Settings & settings = global_ctx.getSettingsRef();

        EXPECT_NE(persister.page_storage->config.gc_min_files, settings.dt_storage_pool_data_gc_min_file_num);
        EXPECT_NE(persister.page_storage->config.gc_min_legacy_num, settings.dt_storage_pool_data_gc_min_legacy_num);
        EXPECT_NE(persister.page_storage->config.gc_min_bytes, settings.dt_storage_pool_data_gc_min_bytes);
        EXPECT_NE(persister.page_storage->config.gc_max_valid_rate, settings.dt_storage_pool_data_gc_max_valid_rate);
        EXPECT_NE(persister.page_storage->config.open_file_max_idle_time, settings.dt_open_file_max_idle_seconds);
        EXPECT_NE(persister.page_storage->config.prob_do_gc_when_write_is_low, settings.dt_page_gc_low_write_prob * 1000);

        persister.gc();

        EXPECT_NE(persister.page_storage->config.gc_min_files, settings.dt_storage_pool_data_gc_min_file_num);
        EXPECT_NE(persister.page_storage->config.gc_min_legacy_num, settings.dt_storage_pool_data_gc_min_legacy_num);
        EXPECT_NE(persister.page_storage->config.gc_min_bytes, settings.dt_storage_pool_data_gc_min_bytes);
        EXPECT_NE(persister.page_storage->config.gc_max_valid_rate, settings.dt_storage_pool_data_gc_max_valid_rate);
        EXPECT_EQ(persister.page_storage->config.open_file_max_idle_time, settings.dt_open_file_max_idle_seconds);
        EXPECT_EQ(persister.page_storage->config.prob_do_gc_when_write_is_low, settings.dt_page_gc_low_write_prob * 1000);
    };

    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);

        LOG_INFO(log, "parsing [index=" << i << "] [content=" << test_case << "]");

        // Reload users config with test case
        global_ctx.reloadDeltaTreeConfig(*config);
        // Only reload the configuration of the storage module starting with "dt" in Settings.h
        ASSERT_EQ(global_ctx.getSettingsRef().max_rows_in_set, 0);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_enable_rough_set_filter, 0);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_segment_limit_rows, 1000005);
        ASSERT_EQ(global_ctx.getSettingsRef().max_memory_usage, 0);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_min_file_num, 8);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_min_legacy_num, 2);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_min_bytes, 256);
        ASSERT_FLOAT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_max_valid_rate, 0.5);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_open_file_max_idle_seconds, 20);
        ASSERT_FLOAT_EQ(global_ctx.getSettingsRef().dt_page_gc_low_write_prob, 0.2);
        verifyPersisterReloadConfig(persister);
    }
    global_ctx.setSettings(origin_settings);
}
CATCH

TEST_F(UsersConfigParser_test, ReloadStoragePoolConfig)
try
{
    Strings tests = {
        R"(
[profiles]
[profiles.default]
max_rows_in_set = 455
dt_segment_limit_rows = 1000005
dt_enable_rough_set_filter = 0
max_memory_usage = 102000
dt_storage_pool_data_gc_min_file_num = 8
dt_storage_pool_data_gc_min_legacy_num = 2
dt_storage_pool_data_gc_min_bytes = 256
dt_storage_pool_data_gc_max_valid_rate = 0.5
dt_open_file_max_idle_seconds = 20
dt_page_gc_low_write_prob = 0.2
        )"};

    auto & global_ctx = TiFlashTestEnv::getGlobalContext();
    std::unique_ptr<StoragePathPool> path_pool = std::make_unique<StoragePathPool>(global_ctx.getPathPool().withTable("test", "t1", false));
    std::unique_ptr<DM::StoragePool> storage_pool = std::make_unique<DM::StoragePool>("test.t1", *path_pool, global_ctx, global_ctx.getSettingsRef());

    auto verifyStoragePoolReloadConfig = [&global_ctx](std::unique_ptr<DM::StoragePool> & storage_pool) {
        DB::Settings & settings = global_ctx.getSettingsRef();

        EXPECT_NE(storage_pool->data().config.gc_min_files, settings.dt_storage_pool_data_gc_min_file_num);
        EXPECT_NE(storage_pool->data().config.gc_min_legacy_num, settings.dt_storage_pool_data_gc_min_legacy_num);
        EXPECT_NE(storage_pool->data().config.gc_min_bytes, settings.dt_storage_pool_data_gc_min_bytes);
        EXPECT_NE(storage_pool->data().config.gc_max_valid_rate, settings.dt_storage_pool_data_gc_max_valid_rate);
        EXPECT_NE(storage_pool->data().config.open_file_max_idle_time, settings.dt_open_file_max_idle_seconds);
        EXPECT_NE(storage_pool->data().config.prob_do_gc_when_write_is_low, settings.dt_page_gc_low_write_prob * 1000);

        storage_pool->gc(settings, DM::StoragePool::Seconds(0));

        EXPECT_EQ(storage_pool->data().config.gc_min_files, settings.dt_storage_pool_data_gc_min_file_num);
        EXPECT_EQ(storage_pool->data().config.gc_min_legacy_num, settings.dt_storage_pool_data_gc_min_legacy_num);
        EXPECT_EQ(storage_pool->data().config.gc_min_bytes, settings.dt_storage_pool_data_gc_min_bytes);
        EXPECT_DOUBLE_EQ(storage_pool->data().config.gc_max_valid_rate, settings.dt_storage_pool_data_gc_max_valid_rate);
        EXPECT_EQ(storage_pool->data().config.open_file_max_idle_time, settings.dt_open_file_max_idle_seconds);
        EXPECT_EQ(storage_pool->data().config.prob_do_gc_when_write_is_low, settings.dt_page_gc_low_write_prob * 1000);
    };

    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);

        LOG_INFO(log, "parsing [index=" << i << "] [content=" << test_case << "]");

        // Reload users config with test case
        global_ctx.reloadDeltaTreeConfig(*config);
        // Only reload the configuration of the storage module starting with "dt" in Settings.h
        ASSERT_EQ(global_ctx.getSettingsRef().max_rows_in_set, 0);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_enable_rough_set_filter, 0);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_segment_limit_rows, 1000005);
        ASSERT_EQ(global_ctx.getSettingsRef().max_memory_usage, 0);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_min_file_num, 8);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_min_legacy_num, 2);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_min_bytes, 256);
        ASSERT_FLOAT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_max_valid_rate, 0.5);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_open_file_max_idle_seconds, 20);
        ASSERT_FLOAT_EQ(global_ctx.getSettingsRef().dt_page_gc_low_write_prob, 0.2);
        verifyStoragePoolReloadConfig(storage_pool);
    }
    global_ctx.setSettings(origin_settings);
}
CATCH

static void verifyChannelConfig(Poco::Channel * channel, Poco::Util::AbstractConfiguration & config)
{
    if (typeid(channel) == typeid(TiflashLogFileChannel))
    {
        TiflashLogFileChannel * fileChannel = dynamic_cast<TiflashLogFileChannel *>(channel);
        ASSERT_EQ(fileChannel->getProperty(Poco::FileChannel::PROP_ROTATION), config.getRawString("logger.size", "100M"));
        ASSERT_EQ(fileChannel->getProperty(Poco::FileChannel::PROP_PURGECOUNT), config.getRawString("logger.count", "1"));
        return;
    }
    if (typeid(channel) == typeid(Poco::LevelFilterChannel))
    {
        Poco::LevelFilterChannel * levelFilterChannel = dynamic_cast<Poco::LevelFilterChannel *>(channel);
        verifyChannelConfig(levelFilterChannel->getChannel(), config);
        return;
    }
    if (typeid(channel) == typeid(Poco::FormattingChannel))
    {
        Poco::FormattingChannel * formattingChannel = dynamic_cast<Poco::FormattingChannel *>(channel);
        verifyChannelConfig(formattingChannel->getChannel(), config);
    }
}

TEST_F(UsersConfigParser_test, ReloadLoggerConfig)
try
{
    Strings tests = {
        R"(
[profiles]
[profiles.default]
max_rows_in_set = 455
dt_page_gc_low_write_prob = 0.2
[logger]
count = 20
errorlog = "./tmp/log/tiflash_error.log"
level = "debug"
log = "./tmp/log/tiflash.log"
size = "1M"
        )",
        R"(
[application]
runAsDaemon = false
[profiles]
[profiles.default]
max_rows_in_set = 455
dt_page_gc_low_write_prob = 0.2
[logger]
count = 10
errorlog = "./tmp/log/tiflash_error.log"
level = "debug"
log = "./tmp/log/tiflash.log"
size = "1K"
        )",
        R"(
[profiles]
[profiles.default]
max_rows_in_set = 455
dt_page_gc_low_write_prob = 0.2
[logger]
count = 1
errorlog = "./tmp/log/tiflash_error.log"
level = "debug"
log = "./tmp/log/tiflash.log"
size = "1"
        )",
    };
    BaseDaemon app;
    auto verifyLoggersConfig = [](size_t logger_num, Poco::Util::AbstractConfiguration & config) {
        for (size_t j = 0; j < logger_num; j++)
        {
            Poco::Logger & cur_logger = Poco::Logger::get(fmt::format("ReloadLoggerConfig_test{}", j));
            verifyChannelConfig(cur_logger.getChannel(), config);
        }
    };

    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);
        Poco::Logger::get(fmt::format("ReloadLoggerConfig_test{}", i));

        app.buildLoggers(*config);
        LOG_INFO(log, "parsing [index=" << i << "] [content=" << test_case << "]");
        verifyLoggersConfig(i + 1, *config);
    }
}
CATCH
} // namespace tests
} // namespace DB
