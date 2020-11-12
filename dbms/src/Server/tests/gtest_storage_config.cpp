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
