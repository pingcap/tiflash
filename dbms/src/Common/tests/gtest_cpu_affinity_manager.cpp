#include <gtest/gtest.h>
#include <boost/algorithm/string.hpp>
#define private public
#include <Common/CPUAffinityManager.h>
#undef private

#include <unistd.h>

namespace DB
{
namespace tests
{
TEST(CPUAffinityManager_test, CPUAffinityManager)
{
    CPUAffinityManager cpu_affinity(80, 38);

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

    std::cout << cpu_affinity.toString() << std::endl;
}

TEST(CPUAffinityManager_test, getThreads)
{
    CPUAffinityManager cpu_affinity(80, 38);
    cpu_affinity.setThreadCPUAffinity();
    cpu_affinity.checkThreadCPUAffinity();
}
}
}