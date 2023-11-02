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

#pragma once

#include <Common/nocopyable.h>
#include <common/defines.h>

#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace Poco
{
namespace Util
{
class LayeredConfiguration;
}
} // namespace Poco

namespace DB
{
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;
namespace tests
{
class CPUAffinityManagerTest_CPUAffinityManager_Test;
} // namespace tests

struct CPUAffinityConfig
{
    CPUAffinityConfig()
        : query_cpu_percent(0)
        , cpu_cores(std::thread::hardware_concurrency())
    {}
    // About {cpu_cores * query_cpu_percent / 100} cpu cores are used for running query threads.
    int query_cpu_percent;
    int cpu_cores;
    // query_threads are the {thread name prefix}.
    // grpcpp_sync_ser is the thread name of grpc sync request thread-pool. However, this thread-pool is resize dynamically and we set these threads' cpu affinity in FlashService for simplicity.
    // Query threads of MPP tasks are created dynamiccally and we set these threads' cpu affinity when they are created.
    std::vector<std::string> query_threads = {"grpcpp_sync_ser"};
};

// CPUAffinityManager is a singleton.
// CPUAffinityManager is use to bind thread on logical CPU core by the linux system call sched_setaffinity.
// The main purpose of bind different threads on different CPUs is to isolating heavy query requests and other requests.
// So CPUAffinityManager simply divide cpu cores and threads into two categories:
// 1. Query threads and query cpu set.
// 2. Other threads and other cpu set.
class CPUAffinityManager
{
public:
    static void initCPUAffinityManager(Poco::Util::LayeredConfiguration & config);
    static CPUAffinityConfig readConfig(Poco::Util::LayeredConfiguration & config);
    static CPUAffinityManager & getInstance();

#ifdef __linux__
    void init(const CPUAffinityConfig & config);

    void bindQueryThread(pid_t tid) const;
    void bindOtherThread(pid_t tid) const;

    void bindSelfQueryThread() const;
    void bindSelfOtherThread() const;
    void bindSelfGrpcThread() const;

    std::string toString() const;

    void bindThreadCPUAffinity() const;
#else
    void init(const CPUAffinityConfig &) {}

    void bindQueryThread(pid_t) const {}
    void bindOtherThread(pid_t) const {}

    void bindSelfQueryThread() const {}
    void bindSelfOtherThread() const {}
    void bindSelfGrpcThread() const {}

    static std::string toString() { return "Not Support"; }

    void bindThreadCPUAffinity() const {}
#endif

private:
#ifdef __linux__
    // for unittest
    friend class DB::tests::CPUAffinityManagerTest_CPUAffinityManager_Test;

    void initCPUSet();
    int getCPUCores() const;
    int getQueryCPUCores() const;
    int getOtherCPUCores() const;
    static void initCPUSet(cpu_set_t & cpu_set, int start, int count);
    void checkThreadCPUAffinity() const;
    // Bind thread t on cpu_set.
    void setAffinity(pid_t tid, const cpu_set_t & cpu_set) const;
    bool enable() const;

    static std::string cpuSetToString(const cpu_set_t & cpu_set);
    static std::vector<int> cpuSetToVec(const cpu_set_t & cpu_set);


    std::unordered_map<pid_t, std::string> getThreads(pid_t pid) const;
    std::vector<pid_t> getThreadIDs(const std::string & dir) const;
    static std::string getThreadName(const std::string & fname);
    static std::string getShortFilename(const std::string & path);
    bool isQueryThread(const std::string & name) const;

    cpu_set_t query_cpu_set{};
    cpu_set_t other_cpu_set{};
#endif

    // unused except Linux
    MAYBE_UNUSED_MEMBER int query_cpu_percent;
    MAYBE_UNUSED_MEMBER int cpu_cores;

    std::vector<std::string> query_threads;
    LoggerPtr log;

    CPUAffinityManager();
    // Disable copy and move
public:
    DISALLOW_COPY_AND_MOVE(CPUAffinityManager);
};
} // namespace DB
