#include <Common/CPUAffinityManager.h>
#include <Common/Config/cpptoml.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/Logger.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <common/logger_useful.h>
#include <errno.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>
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
    , log(&Poco::Logger::get("CPUAffinityManager"))
{}

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
        LOG_INFO(log, "Thread: " << ::getThreadName() << " bindQueryThread.");
        // If tid is zero, then the calling thread is used.
        bindQueryThread(0);
    }
}

void CPUAffinityManager::bindSelfOtherThread() const
{
    if (enable())
    {
        LOG_INFO(log, "Thread: " << ::getThreadName() << " bindOtherThread.");
        // If tid is zero, then the calling thread is used.
        bindOtherThread(0);
    }
}

void CPUAffinityManager::bindSelfGrpcThread() const
{
#if __APPLE__ && __clang__
    static __thread bool is_binding = false;
#else
    static thread_local bool is_binding = false;
#endif

    if (!is_binding)
    {
        bindSelfQueryThread();
        is_binding = true;
    }
}

std::string CPUAffinityManager::toString() const
{
// clang-format off
#ifdef __linux__
    return "enable " + std::to_string(enable()) + " query_cpu_percent " + std::to_string(query_cpu_percent) +
        " cpu_cores " + std::to_string(cpu_cores) + " query_cpu_set " + cpuSetToString(query_cpu_set) +
        " other_cpu_set " + cpuSetToString(other_cpu_set);
#elif
    return "not support";
#endif
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
#ifdef __linux__
    int ret = sched_setaffinity(tid, sizeof(cpu_set), &cpu_set);
    if (ret != 0)
    {
        LOG_ERROR(log, "sched_setaffinity fail but ignore error: " << std::strerror(errno));
    }
#endif
}

bool CPUAffinityManager::enable() const
{
    return 0 < query_cpu_percent && query_cpu_percent < 100 && cpu_cores > 1;
}

std::string CPUAffinityManager::cpuSetToString(const cpu_set_t & cpu_set) const
{
    auto v = cpuSetToVec(cpu_set);
    std::string s;
    for (auto i : v)
    {
        s += std::to_string(i) + " ";
    }
    return s;
}

std::vector<int> CPUAffinityManager::cpuSetToVec(const cpu_set_t & cpu_set) const
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
std::string CPUAffinityManager::getShortFilename(const std::string & path) const
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
            LOG_ERROR(log, "dir " << dir << " path " << iter->path() << " exception " << e.what());
        }
    }
    return tids;
}

std::string CPUAffinityManager::getThreadName(const std::string & fname) const
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
    LOG_DEBUG(log, task_dir << " thread count " << tids.size());
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
            LOG_INFO(log, "Thread: " << t.first << " " << t.second << " bindQueryThread.");
            bindQueryThread(t.first);
        }
        else
        {
            LOG_INFO(log, "Thread: " << t.first << " " << t.second << " bindOtherThread.");
            bindOtherThread(t.first);
        }
    }

    // Log threads cpu bind info.
    checkThreadCPUAffinity();
}

void CPUAffinityManager::checkThreadCPUAffinity() const
{
#ifdef __linux__
    auto threads = getThreads(getpid());
    for (const auto & t : threads)
    {
        cpu_set_t cpu_set;
        int ret = sched_getaffinity(t.first, sizeof(cpu_set), &cpu_set);
        if (ret != 0)
        {
            LOG_ERROR(log, "Thread: " << t.first << " " << t.second << " sched_getaffinity ret " << ret << " error " << strerror(errno));
            continue;
        }
        LOG_INFO(log, "Thread: " << t.first << " " << t.second << " bind on CPU: " << cpuSetToString(cpu_set));
        if (isQueryThread(t.second) && !CPU_EQUAL(&cpu_set, &query_cpu_set))
        {
            LOG_ERROR(log, "Thread: " << t.first << " " << t.second << " is query thread and bind CPU info is error.");
        }
        else if (!isQueryThread(t.second) && !CPU_EQUAL(&cpu_set, &other_cpu_set))
        {
            LOG_ERROR(log, "Thread: " << t.first << " " << t.second << " is other thread and bind CPU info is error.");
        }
    }
#endif
}

} // namespace DB