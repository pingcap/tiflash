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
#include <Interpreters/Context.h>
#include <Interpreters/Quota.h>
#include <Poco/Logger.h>
#include <Server/StorageConfigParser.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/StoragePool/GlobalStoragePool.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/KVStore/MultiRaft/RegionManager.h>
#include <Storages/KVStore/MultiRaft/RegionPersister.h>
#include <Storages/KVStore/Region.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/PathPool.h>
#include <TestUtils/ConfigTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
class UsersConfigParserTest : public ::testing::Test
{
public:
    UsersConfigParserTest()
        : log(&Poco::Logger::get("UsersConfigParserTest"))
    {
        auto & global_ctx = TiFlashTestEnv::getGlobalContext();
        origin_settings = global_ctx.getSettings();
    }

    static void SetUpTestCase() {}

protected:
    Poco::Logger * log;
    Settings origin_settings;
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

        LOG_INFO(log, "parsing [index={}] [content={}]", i, test_case);

        // Reload users config with test case
        auto & global_ctx = TiFlashTestEnv::getGlobalContext();
        global_ctx.setUsersConfig(config);

        // Create a copy of global_ctx
        auto ctx = global_ctx;
        for (const auto & t_addr : test_addrs)
        {
            // Ensure "default" user can build connection
            Poco::Net::SocketAddress addr(t_addr);

            // `setUser` will check user, password, address, update settings and quota for current user
            ASSERT_NO_THROW(ctx.setUser("default", "", addr, ""));
            const auto & settings = ctx.getSettingsRef();
            if (i == 2)
            {
                EXPECT_EQ(settings.max_memory_usage.getActualBytes(0), 123456UL);
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

TEST_F(UsersConfigParserTest, MemoryLimit)
try
{
    UInt64 total = 1'000'000;

    std::vector<std::pair<String, Int64>> tests = {
        {R"(
[profiles]
[profiles.default]
# default
        )",
         800'000},
        {R"(
[profiles]
[profiles.default]
max_memory_usage_for_all_queries = 0
        )",
         0},
        {R"(
[profiles]
[profiles.default]
max_memory_usage_for_all_queries = 0.0
        )",
         0},
        {R"(
[profiles]
[profiles.default]
max_memory_usage_for_all_queries = 0.001
        )",
         1'000},
        {R"(
[profiles]
[profiles.default]
max_memory_usage_for_all_queries = 0.1
        )",
         100'000},
        {R"(
[profiles]
[profiles.default]
max_memory_usage_for_all_queries = 0.999
        )",
         999'000},
        {R"(
[profiles]
[profiles.default]
max_memory_usage_for_all_queries = 1
        )",
         1},
        {R"(
[profiles]
[profiles.default]
max_memory_usage_for_all_queries = 1.0
        )",
         -1},
        {R"(
[profiles]
[profiles.default]
max_memory_usage_for_all_queries = 10000
        )",
         10000},
    };
    auto & global_ctx = TiFlashTestEnv::getGlobalContext();
    for (const auto & [cfg_string, limit] : tests)
    {
        LOG_INFO(log, "parsing [content={}]", cfg_string);
        auto config = loadConfigFromString(cfg_string);
        auto settings = Settings();

        try
        {
            settings.setProfile("default", *config);
            EXPECT_EQ(settings.max_memory_usage_for_all_queries.getActualBytes(total), limit);
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(limit, -1);
            EXPECT_EQ(e.code(), ErrorCodes::INVALID_CONFIG_PARAMETER);
        }
    }
    global_ctx.setSettings(origin_settings);
}
CATCH

TEST_F(UsersConfigParserTest, ReloadDtConfig)
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
dt_page_gc_threshold = 0.2
dt_storage_pool_data_gc_min_file_num = 8
dt_storage_pool_data_gc_min_legacy_num = 2
dt_storage_pool_data_gc_min_bytes = 256
dt_storage_pool_data_gc_max_valid_rate = 0.5
dt_compression_method = "zstd"
dt_compression_level = -1
        )",
        R"(
[profiles]
[profiles.default]
dt_compression_method = "zstd"
dt_compression_level = 1
        )",
        R"(
[profiles]
[profiles.default]
dt_compression_method = "lz4"
dt_compression_level = 2
        )",
        R"(
[profiles]
[profiles.default]
dt_compression_method = "lz4hc"
dt_compression_level = 9
        )",
        R"(
[profiles]
[profiles.default]
dt_compression_method = "Zstd"
dt_compression_level = 1
        )",
        R"(
[profiles]
[profiles.default]
dt_compression_method = "LZ4"
dt_compression_level = 1
        )"};

    auto & global_ctx = TiFlashTestEnv::getGlobalContext();
    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);

        LOG_INFO(log, "parsing [index={}] [content={}]", i, test_case);

        // Reload users config with test case
        global_ctx.reloadDeltaTreeConfig(*config);
        // Only reload the configuration of the storage module starting with "dt" in Settings.h
        ASSERT_EQ(global_ctx.getSettingsRef().max_rows_in_set, 0);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_enable_rough_set_filter, 0);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_segment_limit_rows, 1000005);
        ASSERT_EQ(global_ctx.getSettingsRef().max_memory_usage.getActualBytes(0), 0);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_min_file_num, 8);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_min_legacy_num, 2);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_min_bytes, 256);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_page_gc_threshold, 0.2);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_segment_delta_small_column_file_size, 8388608);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_segment_delta_small_column_file_rows, 2048);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_segment_limit_size, 536870912);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_segment_delta_limit_size, 42991616);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_segment_force_merge_delta_size, 1073741824);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_segment_force_split_size, 1610612736);
        ASSERT_FLOAT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_max_valid_rate, 0.5);

        if (i == 0)
        {
            ASSERT_EQ(global_ctx.getSettingsRef().dt_compression_method, CompressionMethod::ZSTD);
            ASSERT_EQ(global_ctx.getSettingsRef().dt_compression_level, -1);
        }
        if (i == 1)
        {
            ASSERT_EQ(global_ctx.getSettingsRef().dt_compression_method, CompressionMethod::ZSTD);
            ASSERT_EQ(global_ctx.getSettingsRef().dt_compression_level, 1);
        }
        if (i == 2)
        {
            ASSERT_EQ(global_ctx.getSettingsRef().dt_compression_method, CompressionMethod::LZ4);
            ASSERT_EQ(global_ctx.getSettingsRef().dt_compression_level, 2);
        }
        if (i == 3)
        {
            ASSERT_EQ(global_ctx.getSettingsRef().dt_compression_method, CompressionMethod::LZ4HC);
            ASSERT_EQ(global_ctx.getSettingsRef().dt_compression_level, 9);
        }
        if (i == 4)
        {
            ASSERT_EQ(global_ctx.getSettingsRef().dt_compression_method, CompressionMethod::ZSTD);
            ASSERT_EQ(global_ctx.getSettingsRef().dt_compression_level, 1);
        }
        if (i == 5)
        {
            ASSERT_EQ(global_ctx.getSettingsRef().dt_compression_method, CompressionMethod::LZ4);
            ASSERT_EQ(global_ctx.getSettingsRef().dt_compression_level, 1);
        }
    }
    global_ctx.setSettings(origin_settings);
}
CATCH

TEST_F(UsersConfigParserTest, ReloadPersisterConfig)
try
{
    Strings tests = {
        R"(
[profiles]
[profiles.default]
max_rows_in_set = 455
dt_segment_limit_rows = 1000005
dt_enable_rough_set_filter = 0
dt_page_gc_threshold = 0.3
max_memory_usage = 102000
dt_storage_pool_data_gc_min_file_num = 8
dt_storage_pool_data_gc_min_legacy_num = 2
dt_storage_pool_data_gc_min_bytes = 256
dt_storage_pool_data_gc_max_valid_rate = 0.5
dt_open_file_max_idle_seconds = 20
dt_page_gc_low_write_prob = 0.2
        )"};
    auto & global_ctx = TiFlashTestEnv::getGlobalContext();
    if (global_ctx.getPageStorageRunMode() == PageStorageRunMode::UNI_PS)
    {
        // don't support reload uni ps config through region persister
        return;
    }
    auto & global_path_pool = global_ctx.getPathPool();
    RegionPersister persister(global_ctx);
    persister.restore(global_path_pool, nullptr, PageStorageConfig{});

    auto verify_persister_reload_config = [&global_ctx](RegionPersister & persister) {
        DB::Settings & settings = global_ctx.getSettingsRef();

        auto cfg = persister.getPageStorageSettings();
        EXPECT_NE(cfg.gc_min_files, settings.dt_storage_pool_data_gc_min_file_num);
        EXPECT_NE(cfg.gc_min_legacy_num, settings.dt_storage_pool_data_gc_min_legacy_num);
        EXPECT_NE(cfg.gc_min_bytes, settings.dt_storage_pool_data_gc_min_bytes);
        EXPECT_NE(cfg.gc_max_valid_rate, settings.dt_storage_pool_data_gc_max_valid_rate);
        EXPECT_NE(cfg.blob_heavy_gc_valid_rate, settings.dt_page_gc_threshold);
        EXPECT_NE(cfg.open_file_max_idle_time, settings.dt_open_file_max_idle_seconds);
        EXPECT_NE(cfg.prob_do_gc_when_write_is_low, settings.dt_page_gc_low_write_prob * 1000);
        persister.gc();

        cfg = persister.getPageStorageSettings();
        EXPECT_NE(cfg.gc_min_files, settings.dt_storage_pool_data_gc_min_file_num);
        EXPECT_NE(cfg.gc_min_legacy_num, settings.dt_storage_pool_data_gc_min_legacy_num);
        EXPECT_NE(cfg.gc_min_bytes, settings.dt_storage_pool_data_gc_min_bytes);
        EXPECT_NE(cfg.gc_max_valid_rate, settings.dt_storage_pool_data_gc_max_valid_rate);
        EXPECT_EQ(cfg.blob_heavy_gc_valid_rate, settings.dt_page_gc_threshold);
        EXPECT_EQ(cfg.open_file_max_idle_time, settings.dt_open_file_max_idle_seconds);
        EXPECT_EQ(cfg.prob_do_gc_when_write_is_low, settings.dt_page_gc_low_write_prob * 1000);
    };

    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);

        LOG_INFO(log, "parsing [index={}] [content={}]", i, test_case);

        // Reload users config with test case
        global_ctx.reloadDeltaTreeConfig(*config);
        // Only reload the configuration of the storage module starting with "dt" in Settings.h
        ASSERT_EQ(global_ctx.getSettingsRef().max_rows_in_set, 0);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_enable_rough_set_filter, 0);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_segment_limit_rows, 1000005);
        ASSERT_EQ(global_ctx.getSettingsRef().max_memory_usage.getActualBytes(0), 0);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_min_file_num, 8);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_min_legacy_num, 2);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_min_bytes, 256);
        ASSERT_FLOAT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_max_valid_rate, 0.5);
        ASSERT_DOUBLE_EQ(global_ctx.getSettingsRef().dt_page_gc_threshold, 0.3);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_open_file_max_idle_seconds, 20);
        ASSERT_FLOAT_EQ(global_ctx.getSettingsRef().dt_page_gc_low_write_prob, 0.2);
        verify_persister_reload_config(persister);
    }
    global_ctx.setSettings(origin_settings);
}
CATCH

TEST_F(UsersConfigParserTest, ReloadStoragePoolConfig)
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
dt_page_gc_threshold = 0.3
dt_storage_pool_data_gc_min_file_num = 8
dt_storage_pool_data_gc_min_legacy_num = 2
dt_storage_pool_data_gc_min_bytes = 256
dt_storage_pool_data_gc_max_valid_rate = 0.5
dt_open_file_max_idle_seconds = 20
dt_page_gc_low_write_prob = 0.2
        )"};

    auto & global_ctx = TiFlashTestEnv::getGlobalContext();
    if (global_ctx.getPageStorageRunMode() == PageStorageRunMode::UNI_PS)
    {
        // don't support reload uni ps config through storage pool
        return;
    }
    std::unique_ptr<StoragePathPool> path_pool
        = std::make_unique<StoragePathPool>(global_ctx.getPathPool().withTable("test", "t1", false));
    std::unique_ptr<DM::StoragePool> storage_pool
        = std::make_unique<DM::StoragePool>(global_ctx, NullspaceID, /*ns_id*/ 100, *path_pool, "test.t1");

    auto verify_storage_pool_reload_config = [&](std::unique_ptr<DM::StoragePool> & storage_pool) {
        DB::Settings & settings = global_ctx.getSettingsRef();

        auto cfg = storage_pool->dataWriter()->getSettings();
        EXPECT_NE(cfg.gc_min_files, settings.dt_storage_pool_data_gc_min_file_num);
        EXPECT_NE(cfg.gc_min_legacy_num, settings.dt_storage_pool_data_gc_min_legacy_num);
        EXPECT_NE(cfg.gc_min_bytes, settings.dt_storage_pool_data_gc_min_bytes);
        EXPECT_NE(cfg.gc_max_valid_rate, settings.dt_storage_pool_data_gc_max_valid_rate);
        EXPECT_NE(cfg.blob_heavy_gc_valid_rate, settings.dt_page_gc_threshold);
        EXPECT_NE(cfg.open_file_max_idle_time, settings.dt_open_file_max_idle_seconds);
        EXPECT_NE(cfg.prob_do_gc_when_write_is_low, settings.dt_page_gc_low_write_prob * 1000);

        global_ctx.getGlobalStoragePool()->gc();

        cfg = storage_pool->dataWriter()->getSettings();
        EXPECT_EQ(cfg.gc_min_files, settings.dt_storage_pool_data_gc_min_file_num);
        EXPECT_EQ(cfg.gc_min_legacy_num, settings.dt_storage_pool_data_gc_min_legacy_num);
        EXPECT_EQ(cfg.gc_min_bytes, settings.dt_storage_pool_data_gc_min_bytes);
        EXPECT_DOUBLE_EQ(cfg.gc_max_valid_rate, settings.dt_storage_pool_data_gc_max_valid_rate);
        EXPECT_DOUBLE_EQ(cfg.blob_heavy_gc_valid_rate, settings.dt_page_gc_threshold);
        EXPECT_EQ(cfg.open_file_max_idle_time, settings.dt_open_file_max_idle_seconds);
        EXPECT_EQ(cfg.prob_do_gc_when_write_is_low, settings.dt_page_gc_low_write_prob * 1000);
    };

    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);

        LOG_INFO(log, "parsing [index={}] [content={}]", i, test_case);

        // Reload users config with test case
        global_ctx.reloadDeltaTreeConfig(*config);
        // Only reload the configuration of the storage module starting with "dt" in Settings.h
        ASSERT_EQ(global_ctx.getSettingsRef().max_rows_in_set, 0);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_enable_rough_set_filter, 0);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_segment_limit_rows, 1000005);
        ASSERT_EQ(global_ctx.getSettingsRef().max_memory_usage.getActualBytes(0), 0);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_min_file_num, 8);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_min_legacy_num, 2);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_min_bytes, 256);
        ASSERT_FLOAT_EQ(global_ctx.getSettingsRef().dt_storage_pool_data_gc_max_valid_rate, 0.5);
        ASSERT_DOUBLE_EQ(global_ctx.getSettingsRef().dt_page_gc_threshold, 0.3);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_open_file_max_idle_seconds, 20);
        ASSERT_FLOAT_EQ(global_ctx.getSettingsRef().dt_page_gc_low_write_prob, 0.2);
        verify_storage_pool_reload_config(storage_pool);
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

TEST_F(UsersConfigParserTest, ReloadBoolSetting)
try
{
    Strings tests = {
        R"(
[profiles]
[profiles.default]
dt_enable_rough_set_filter = false
dt_read_delta_only = 1
dt_read_stable_only = true
        )"};

    auto & global_ctx = TiFlashTestEnv::getGlobalContext();
    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);

        LOG_INFO(log, "parsing [index={}] [content={}]", i, test_case);

        global_ctx.reloadDeltaTreeConfig(*config);
        ASSERT_EQ(global_ctx.getSettingsRef().dt_enable_rough_set_filter, false);
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
