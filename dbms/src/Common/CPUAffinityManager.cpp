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
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/setThreadName.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <boost_wrapper/string.h>
#include <common/logger_useful.h>
#include <errno.h>
#include <unistd.h>

#include <cstring>
#include <fstream>
namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_EXCEPTION;
extern const int CPUID_ERROR;
} // namespace ErrorCodes

void CPUAffinityManager::initCPUAffinityManager(Poco::Util::LayeredConfiguration & config)
{
    auto cpu_config = readConfig(config);
    CPUAffinityManager::getInstance().init(cpu_config);
}

CPUAffinityConfig CPUAffinityManager::readConfig(Poco::Util::LayeredConfiguration & config)
{
    CPUAffinityConfig cpu_config;
    if (config.has("cpu"))
    {
        std::string s = config.getString("cpu");
        std::istringstream ss(s);
        cpptoml::parser p(ss);
        auto table = p.parse();
        if (auto query_cpu_pct = table->get_qualified_as<int>("query_cpu_percent"); query_cpu_pct)
        {
            cpu_config.query_cpu_percent = *query_cpu_pct;
        }
    }
    return cpu_config;
}

CPUAffinityManager & CPUAffinityManager::getInstance()
{
    static CPUAffinityManager cpu_affinity_mgr;
    return cpu_affinity_mgr;
}

CPUAffinityManager::CPUAffinityManager()
    : query_cpu_percent(0)
    , cpu_cores(0)
    , log(Logger::get())
{}

#ifdef __linux__
void CPUAffinityManager::init(const CPUAffinityConfig & config)
{
    query_cpu_percent = config.query_cpu_percent;
    cpu_cores = config.cpu_cores;
    query_threads = config.query_threads;
    CPU_ZERO(&query_cpu_set);
    CPU_ZERO(&other_cpu_set);
    if (enable())
    {
        initCPUSet();
    }
}

bool CPUAffinityManager::isQueryThread(const std::string & name) const
{
    for (const auto & t : query_threads)
    {
        if (boost::algorithm::starts_with(name, t))
        {
            return true;
        }
    }
    return false;
}

void CPUAffinityManager::bindQueryThread(pid_t tid) const
{
    if (enable())
    {
        setAffinity(tid, query_cpu_set);
    }
}

void CPUAffinityManager::bindOtherThread(pid_t tid) const
{
    if (enable())
    {
        setAffinity(tid, other_cpu_set);
    }
}

void CPUAffinityManager::bindSelfQueryThread() const
{
    if (enable())
    {
        LOG_INFO(log, "Thread: {} bindQueryThread.", ::getThreadName());
        // If tid is zero, then the calling thread is used.
        bindQueryThread(0);
    }
}

void CPUAffinityManager::bindSelfOtherThread() const
{
    if (enable())
    {
        LOG_INFO(log, "Thread: {} bindOtherThread.", ::getThreadName());
        // If tid is zero, then the calling thread is used.
        bindOtherThread(0);
    }
}

void CPUAffinityManager::bindSelfGrpcThread() const
{
    static thread_local bool is_binding = false;
    if (!is_binding)
    {
        bindSelfQueryThread();
        is_binding = true;
    }
}

std::string CPUAffinityManager::toString() const
{
    // clang-format off
    return "enable " + std::to_string(enable()) + " query_cpu_percent " + std::to_string(query_cpu_percent) +
        " cpu_cores " + std::to_string(cpu_cores) + " query_cpu_set " + cpuSetToString(query_cpu_set) +
        " other_cpu_set " + cpuSetToString(other_cpu_set);
    // clang-format on
}

void CPUAffinityManager::initCPUSet()
{
    int query_cpu_cores = getQueryCPUCores();
    int other_cpu_cores = getOtherCPUCores();
    // [0, other_cpu_cores) is for other threads.
    initCPUSet(other_cpu_set, 0, other_cpu_cores);
    // [other_cpu_cores, other_cpu_cores + query_cpu_cores) is for query threads.
    initCPUSet(query_cpu_set, other_cpu_cores, query_cpu_cores);
}

int CPUAffinityManager::getCPUCores() const
{
    return cpu_cores;
}

int CPUAffinityManager::getQueryCPUCores() const
{
    return getCPUCores() * query_cpu_percent / 100;
}

int CPUAffinityManager::getOtherCPUCores() const
{
    return getCPUCores() - getQueryCPUCores();
}

void CPUAffinityManager::initCPUSet(cpu_set_t & cpu_set, int start, int count)
{
    for (int i = 0; i < count; i++)
    {
        CPU_SET(start + i, &cpu_set);
    }
}

void CPUAffinityManager::setAffinity(pid_t tid, const cpu_set_t & cpu_set) const
{
    int ret = sched_setaffinity(tid, sizeof(cpu_set), &cpu_set);
    if (ret != 0)
    {
        LOG_ERROR(log, "sched_setaffinity fail but ignore error: {}", std::strerror(errno));
    }
}

bool CPUAffinityManager::enable() const
{
    return 0 < query_cpu_percent && query_cpu_percent < 100 && cpu_cores > 1;
}

std::string CPUAffinityManager::cpuSetToString(const cpu_set_t & cpu_set)
{
    auto v = cpuSetToVec(cpu_set);
    std::string s;
    for (auto i : v)
    {
        s += std::to_string(i) + " ";
    }
    return s;
}

std::vector<int> CPUAffinityManager::cpuSetToVec(const cpu_set_t & cpu_set)
{
    std::vector<int> v;
    for (int i = 0; i < static_cast<int>(sizeof(cpu_set)); i++)
    {
        if (CPU_ISSET(i, &cpu_set))
        {
            v.push_back(i);
        }
    }
    return v;
}

// /proc/17022/task/17022 -> 17022
std::string CPUAffinityManager::getShortFilename(const std::string & path)
{
    auto pos = path.find_last_of('/');
    if (pos == std::string::npos)
    {
        return path;
    }
    else
    {
        return path.substr(pos + 1);
    }
}

std::vector<pid_t> CPUAffinityManager::getThreadIDs(const std::string & dir) const
{
    std::vector<pid_t> tids;
    Poco::DirectoryIterator end;
    for (Poco::DirectoryIterator iter(dir); iter != end; ++iter)
    {
        try
        {
            auto fname = getShortFilename(iter->path());
            tids.push_back(std::stol(fname));
        }
        catch (std::exception & e)
        {
            LOG_ERROR(log, "dir {} path {} exception {}", dir, iter->path(), e.what());
        }
    }
    return tids;
}

std::string CPUAffinityManager::getThreadName(const std::string & fname)
{
    std::ifstream ifs(fname);
    if (ifs.fail())
    {
        return ""; // Thread maybe exit and related proc files are removed.
    }
    std::string s;
    std::getline(ifs, s);
    return s;
}

std::unordered_map<pid_t, std::string> CPUAffinityManager::getThreads(pid_t pid) const
{
    std::string task_dir = "/proc/" + std::to_string(pid) + "/task";
    auto tids = getThreadIDs(task_dir);
    LOG_DEBUG(log, "{} thread count {}", task_dir, tids.size());
    std::unordered_map<pid_t, std::string> threads;
    for (auto tid : tids)
    {
        std::string file = task_dir + "/" + std::to_string(tid) + "/comm";
        threads.emplace(tid, getThreadName(file));
    }
    return threads;
}

void CPUAffinityManager::bindThreadCPUAffinity() const
{
    if (!enable())
    {
        return;
    }
    auto threads = getThreads(getpid());
    for (const auto & t : threads)
    {
        if (isQueryThread(t.second))
        {
            LOG_INFO(log, "Thread: {} {} bindQueryThread.", t.first, t.second);
            bindQueryThread(t.first);
        }
        else
        {
            LOG_INFO(log, "Thread: {} {} bindOtherThread.", t.first, t.second);
            bindOtherThread(t.first);
        }
    }

    // Log threads cpu bind info.
    checkThreadCPUAffinity();
}

void CPUAffinityManager::checkThreadCPUAffinity() const
{
    auto threads = getThreads(getpid());
    for (const auto & t : threads)
    {
        cpu_set_t cpu_set;
        int ret = sched_getaffinity(t.first, sizeof(cpu_set), &cpu_set);
        if (ret != 0)
        {
            LOG_ERROR(log, "Thread: {} {} sched_getaffinity ret {} error {}", t.first, t.second, ret, strerror(errno));
            continue;
        }
        LOG_INFO(log, "Thread: {} {} bind on CPU: {}", t.first, t.second, cpuSetToString(cpu_set));
        if (isQueryThread(t.second) && !CPU_EQUAL(&cpu_set, &query_cpu_set))
        {
            LOG_ERROR(log, "Thread: {} {} is query thread and bind CPU info is error.", t.first, t.second);
        }
        else if (!isQueryThread(t.second) && !CPU_EQUAL(&cpu_set, &other_cpu_set))
        {
            LOG_ERROR(log, "Thread: {} {} is other thread and bind CPU info is error.", t.first, t.second);
        }
    }
}
#endif
} // namespace DB
