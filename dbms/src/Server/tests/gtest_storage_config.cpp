#include <Common/Config/ConfigProcessor.h>
#include <Common/Config/TOMLConfiguration.h>
#include <Poco/Logger.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Server/StorageConfigParser.h>
#include <test_utils/TiflashTestBasic.h>

namespace DB
{
namespace tests
{

class StorageConfig_test : public ::testing::Test
{
public:
    StorageConfig_test() : log(&Poco::Logger::get("StorageConfig_test")) {}

    static void SetUpTestCase() { TiFlashTestEnv::setupLogger(); }

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
path_realtime_mode = false # 默认值
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
        std::istringstream ss(test_case);
        cpptoml::parser p(ss);
        auto table = p.parse();
        std::shared_ptr<Poco::Util::AbstractConfiguration> configuration(new DB::TOMLConfiguration(table));
        Poco::AutoPtr<Poco::Util::LayeredConfiguration> config = new Poco::Util::LayeredConfiguration();
        config->add(configuration.get());

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
        std::istringstream ss(test_case);
        cpptoml::parser p(ss);
        auto table = p.parse();
        std::shared_ptr<Poco::Util::AbstractConfiguration> configuration(new DB::TOMLConfiguration(table));
        Poco::AutoPtr<Poco::Util::LayeredConfiguration> config = new Poco::Util::LayeredConfiguration();
        config->add(configuration.get());

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
    }
}
CATCH

TEST_F(StorageConfig_test, ParseMaybeBrokenCases)
try
{
    Strings tests = {
        R"(
path = "/tmp/tiflash/data/db"
capacity = "10737418240"
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
        R"(
path = "/data0/tiflash,/data1/tiflash"
capacity = "10737418240"
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
    };

    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        std::istringstream ss(test_case);
        cpptoml::parser p(ss);
        auto table = p.parse();
        std::shared_ptr<Poco::Util::AbstractConfiguration> configuration(new DB::TOMLConfiguration(table));
        Poco::AutoPtr<Poco::Util::LayeredConfiguration> config = new Poco::Util::LayeredConfiguration();
        config->add(configuration.get());

        LOG_INFO(log, "parsing [index=" << i << "] [content=" << test_case << "]");

        size_t global_capacity_quota = 0;
        TiFlashStorageConfig storage;
        ASSERT_ANY_THROW({ std::tie(global_capacity_quota, storage) = TiFlashStorageConfig::parseSettings(*config, log); });
    }
}
CATCH

} // namespace tests
} // namespace DB
