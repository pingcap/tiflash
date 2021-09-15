#include <gtest/gtest.h>
#include <boost/algorithm/string.hpp>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/Config/cpptoml.h>
#include <Common/Config/TOMLConfiguration.h>
#define private public
#include <Common/CPUAffinityManager.h>
#undef private

#include <unistd.h>

namespace DB
{
namespace tests
{

static auto loadConfigFromString(const std::string & s)
{
    std::istringstream ss(s);
    cpptoml::parser p(ss);
    auto table = p.parse();
    Poco::AutoPtr<Poco::Util::LayeredConfiguration> config = new Poco::Util::LayeredConfiguration();
    config->add(new DB::TOMLConfiguration(table), /*shared=*/false); // Take ownership of TOMLConfig
    return config;
}

TEST(CPUAffinityManager_test, CPUAffinityManager)
{
    auto config = loadConfigFromString("");
    CPUAffinityManager cpu_affinity(80, 38, *config);

    ASSERT_TRUE(cpu_affinity.enable());
    ASSERT_EQ(cpu_affinity.getWriteCPUCores(), 8);
    ASSERT_EQ(cpu_affinity.getReadCPUCores(), 30);

    auto s = cpu_affinity.cpuSetToString(cpu_affinity.write_cpu_set);
    boost::algorithm::trim(s);
    std::string write_cpu{"0 1 2 3 4 5 6 7"};
    ASSERT_EQ(s, write_cpu);   

    s = cpu_affinity.cpuSetToString(cpu_affinity.read_cpu_set);
    boost::algorithm::trim(s);
    std::string read_cpu{"8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37"};
    ASSERT_EQ(s, read_cpu);
}

TEST(CPUAffinityManager_test, Threads)
{
    std::string s = R"(
[flash.cpu]
read_threads=["BkgPool", "cop-pool"]
    )";
    auto config = loadConfigFromString(s);
    CPUAffinityManager cpu_affinity(80, 38, *config);

    ASSERT_EQ(cpu_affinity.read_threads.size(), 2ul);
    ASSERT_EQ(cpu_affinity.read_threads[0], "BkgPool");
    ASSERT_EQ(cpu_affinity.read_threads[1], "cop-pool");
    ASSERT_TRUE(cpu_affinity.isReadThread("BkgPool-0"));
    ASSERT_TRUE(cpu_affinity.isReadThread("BkgPool"));
    ASSERT_FALSE(cpu_affinity.isReadThread("apply"));
}

}
}