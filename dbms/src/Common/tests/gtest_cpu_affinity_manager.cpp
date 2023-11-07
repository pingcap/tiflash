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

#include <Common/CPUAffinityManager.h>
#include <Common/Config/TOMLConfiguration.h>
#include <Common/Logger.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <boost_wrapper/string.h>
#include <common/logger_useful.h>
#include <gtest/gtest.h>
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

TEST(CPUAffinityManagerTest, readConfig)
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
    std::vector<int> vi = {/*default*/ 0, 55, 77};

    for (size_t i = 0; i < vs.size(); i++)
    {
        const auto & s = vs[i];
        auto config = CPUAffinityManager::readConfig(*loadConfigFromString(s));
        ASSERT_EQ(config.query_cpu_percent, vi[i]);
        ASSERT_EQ(config.cpu_cores, static_cast<int>(std::thread::hardware_concurrency()));
        auto default_query_threads = std::vector<std::string>{"grpcpp_sync_ser"};
        ASSERT_EQ(config.query_threads, default_query_threads);
    }
}

#ifdef __linux__
TEST(CPUAffinityManagerTest, CPUAffinityManager)
{
    auto & cpu_affinity = CPUAffinityManager::getInstance();

    ASSERT_FALSE(cpu_affinity.enable());

    cpu_set_t cpu_set;
    int ret = sched_getaffinity(0, sizeof(cpu_set), &cpu_set);
    ASSERT_EQ(ret, 0) << strerror(errno);

    auto n_cpu = std::thread::hardware_concurrency();
    auto cpu_cores = cpu_affinity.cpuSetToVec(cpu_set);
    if (n_cpu != cpu_cores.size())
    {
        LOG_INFO(
            Logger::get(),
            "n_cpu = {}, cpu_cores = {}, CPU number and CPU cores not match, don't not check CPUAffinityManager",
            n_cpu,
            cpu_cores);
        return;
    }
    LOG_DEBUG(Logger::get(), "n_cpu = {}, cpu_cores = {}", n_cpu, cpu_cores);

    cpu_affinity.bindSelfQueryThread();
    cpu_set_t cpu_set0;
    ret = sched_getaffinity(0, sizeof(cpu_set0), &cpu_set0);
    ASSERT_EQ(ret, 0) << strerror(errno);
    ASSERT_TRUE(CPU_EQUAL(&cpu_set0, &cpu_set));

    cpu_affinity.bindSelfOtherThread();
    cpu_set_t cpu_set1;
    ret = sched_getaffinity(0, sizeof(cpu_set1), &cpu_set1);
    ASSERT_EQ(ret, 0) << strerror(errno);
    ASSERT_TRUE(CPU_EQUAL(&cpu_set1, &cpu_set));

    CPUAffinityConfig config;
    config.query_cpu_percent = 60;
    config.cpu_cores = std::thread::hardware_concurrency();
    cpu_affinity.init(config);

    ASSERT_TRUE(cpu_affinity.enable());

    std::vector<int> except_other_cpu_set;
    for (int i = 0; i < cpu_affinity.getOtherCPUCores(); ++i)
    {
        except_other_cpu_set.push_back(i);
    }
    auto other_cpu_set = cpu_affinity.cpuSetToVec(cpu_affinity.other_cpu_set);
    ASSERT_EQ(other_cpu_set, except_other_cpu_set);

    std::vector<int> except_query_cpu_set;
    for (int i = 0; i < cpu_affinity.getQueryCPUCores(); ++i)
    {
        except_query_cpu_set.push_back(cpu_affinity.getOtherCPUCores() + i);
    }
    auto query_cpu_set = cpu_affinity.cpuSetToVec(cpu_affinity.query_cpu_set);
    ASSERT_EQ(query_cpu_set, except_query_cpu_set);

    cpu_affinity.bindSelfQueryThread();
    cpu_set_t cpu_set2;
    ret = sched_getaffinity(0, sizeof(cpu_set2), &cpu_set2);
    ASSERT_EQ(ret, 0) << strerror(errno);
    ASSERT_TRUE(CPU_EQUAL(&cpu_set2, &(cpu_affinity.query_cpu_set)));

    cpu_affinity.bindSelfOtherThread();
    cpu_set_t cpu_set3;
    ret = sched_getaffinity(0, sizeof(cpu_set3), &cpu_set3);
    ASSERT_EQ(ret, 0) << strerror(errno);
    ASSERT_TRUE(CPU_EQUAL(&cpu_set3, &(cpu_affinity.other_cpu_set)));

    ASSERT_TRUE(cpu_affinity.isQueryThread("grpcpp_sync_server"));
    ASSERT_FALSE(cpu_affinity.isQueryThread("grpcpp_sync"));
}
#endif

} // namespace tests
} // namespace DB
