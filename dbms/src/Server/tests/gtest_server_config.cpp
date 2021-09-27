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
#include <Interpreters/Quota.h>
#include <Poco/Logger.h>
#include <Server/StorageConfigParser.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionManager.h>
#include <Storages/Transaction/RegionPersister.h>
#include <TestUtils/ConfigTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
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

static void validate_bool_config(SettingBool & setting, String input, bool expect_value, bool expect_exception)
{
    try
    {
        setting.set(input);
    }
    catch (const DB::Exception & e)
    {
        if (e.code() != ErrorCodes::CANNOT_PARSE_BOOL)
        {
            throw;
        }
        ASSERT_EQ(expect_exception, true);
        ASSERT_EQ(setting, expect_value);
        return;
    }
    ASSERT_EQ(expect_exception, false);
    ASSERT_EQ(setting, expect_value);
}

TEST_F(UsersConfigParser_test, ReloadBoolSetting)
try
{
    Strings tests = {
        R"(
[profiles]
[profiles.default]
dt_enable_rough_set_filter = false
dt_raw_filter_range = 0
dt_read_delta_only = 1
dt_read_stable_only = true
        )"};

    auto & global_ctx = TiFlashTestEnv::getGlobalContext();
    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);

        LOG_INFO(log, "parsing [index=" << i << "] [content=" << test_case << "]");

        global_ctx.reloadDeltaTreeConfig(*config);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_enable_rough_set_filter, false);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_raw_filter_range, false);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_read_delta_only, true);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_read_stable_only, true);
    }
    SettingBool test_config = false;
    validate_bool_config(test_config, "1", true, false);
    validate_bool_config(test_config, "0", false, false);
    validate_bool_config(test_config, "2", false, true);
    validate_bool_config(test_config, "10", false, true);

    validate_bool_config(test_config, "false", false, false);
    validate_bool_config(test_config, "ture", false, true);
    validate_bool_config(test_config, "true", true, false);
    validate_bool_config(test_config, "flase", true, true);
    validate_bool_config(test_config, "false", false, false);
    validate_bool_config(test_config, "true", true, false);

    global_ctx.setSettings(origin_settings);
}
CATCH
} // namespace tests
} // namespace DB
