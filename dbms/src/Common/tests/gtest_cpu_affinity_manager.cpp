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

TEST(CPUAffinityManager_test, readConfig)
{
    std::vector<std::string> vs = {
R"(
[cpu]
)",
R"(
[cpu]
query_cpu_percent=55
)",
R"(
[cpu]
query_cpu_percent=77
)",
    };
    std::vector<int> vi = { /*default*/ 0, 55, 77 };

    for (size_t i = 0; i < vs.size(); i++)
    {
        const auto & s = vs[i];
        auto config = CPUAffinityManager::readConfig(*loadConfigFromString(s));
        ASSERT_EQ(config.query_cpu_percent, vi[i]);
        ASSERT_EQ(config.cpu_cores, static_cast<int>(std::thread::hardware_concurrency()));
        auto default_query_threads = std::vector<std::string>{"cop-pool", "batch-cop-pool", "grpcpp_sync_ser"};
        ASSERT_EQ(config.query_threads, default_query_threads);
    }
}

#ifdef __linux__
TEST(CPUAffinityManager_test, CPUAffinityManager)
{
    auto & cpu_affinity = CPUAffinityManager::getInstance();

    ASSERT_FALSE(cpu_affinity.enable());

    cpu_set_t cpu_set;
    int ret = sched_getaffinity(0, sizeof(cpu_set), &cpu_set);
    ASSERT_EQ(ret , 0) << strerror(errno);

    cpu_affinity.bindSelfQueryThread();
    cpu_set_t cpu_set0;
    ret = sched_getaffinity(0, sizeof(cpu_set0), &cpu_set0);
    ASSERT_EQ(ret , 0) << strerror(errno);
    ASSERT_TRUE(CPU_EQUAL(&cpu_set0, &cpu_set));

    cpu_affinity.bindSelfOtherThread();
    cpu_set_t cpu_set1;
    ret = sched_getaffinity(0, sizeof(cpu_set1), &cpu_set1);
    ASSERT_EQ(ret , 0) << strerror(errno);
    ASSERT_TRUE(CPU_EQUAL(&cpu_set1, &cpu_set));

    CPUAffinityConfig config;
    config.query_cpu_percent = 60;
    config.cpu_cores = 40;
    cpu_affinity.init(config);

    ASSERT_TRUE(cpu_affinity.enable());
    ASSERT_EQ(cpu_affinity.getOtherCPUCores(), 16);
    ASSERT_EQ(cpu_affinity.getQueryCPUCores(), 24);

    std::vector<int> except_other_cpu_set{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    auto other_cpu_set = cpu_affinity.cpuSetToVec(cpu_affinity.other_cpu_set);
    ASSERT_EQ(other_cpu_set, except_other_cpu_set);   

    std::vector<int> except_query_cpu_set{16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39};
    auto query_cpu_set = cpu_affinity.cpuSetToVec(cpu_affinity.query_cpu_set);
    ASSERT_EQ(query_cpu_set, except_query_cpu_set);

    cpu_affinity.bindSelfQueryThread();
    cpu_set_t cpu_set2;
    ret = sched_getaffinity(0, sizeof(cpu_set2), &cpu_set2);
    ASSERT_EQ(ret , 0) << strerror(errno);
    ASSERT_TRUE(CPU_EQUAL(&cpu_set2, &(cpu_affinity.query_cpu_set)));

    cpu_affinity.bindSelfOtherThread();
    cpu_set_t cpu_set3;
    ret = sched_getaffinity(0, sizeof(cpu_set3), &cpu_set3);
    ASSERT_EQ(ret , 0) << strerror(errno);
    ASSERT_TRUE(CPU_EQUAL(&cpu_set3, &(cpu_affinity.other_cpu_set)));

    ASSERT_TRUE(cpu_affinity.isQueryThread("cop-pool0"));
    ASSERT_FALSE(cpu_affinity.isQueryThread("cop-po"));
    ASSERT_TRUE(cpu_affinity.isQueryThread("grpcpp_sync_server"));
    ASSERT_FALSE(cpu_affinity.isQueryThread("grpcpp_sync"));
}
#endif

}
}