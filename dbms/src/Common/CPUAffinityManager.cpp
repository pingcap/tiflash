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

#include <cstring>
#include <fstream>
namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_EXCEPTION;
extern const int CPUID_ERROR;
} // namespace ErrorCodes

CPUAffinityManager::CPUAffinityManager(int read_cpu_percent_, int cpu_cores_, Poco::Util::LayeredConfiguration & config)
    : read_cpu_percent(read_cpu_percent_)
    , cpu_cores(cpu_cores_)
    , log(&Poco::Logger::get("CPUAffinityManager"))
{
    CPU_ZERO(&write_cpu_set);
    CPU_ZERO(&read_cpu_set);
    if (enable())
    {
        initCPUSet();
    }
    initReadThreadNames(config);
}

// Threads of MPP request should call `bindSelfReadThread` when it is created.
void CPUAffinityManager::initReadThreadNames(Poco::Util::LayeredConfiguration & config)
{
    if (config.has("flash.cpu"))
    {
        std::string cpu_config = config.getString("flash.cpu");
        std::istringstream ss(cpu_config);
        cpptoml::parser p(ss);
        auto table = p.parse();
        if (auto threads = table->get_qualified_array_of<std::string>("read_threads"); threads)
        {
            for (const auto & name : *threads)
            {
                read_threads.push_back(name);
            }
        }
    }

    // Default read threads
    if (read_threads.empty())
    {
        read_threads = {"cop-pool", "batch-cop-pool", "grpcpp_sync_ser"};
    }
}

bool CPUAffinityManager::isReadThread(const std::string & name) const
{
    for (const auto & t : read_threads)
    {
        if (name.find(t) == 0) // t is name's prefix.
        {
            return true;
        }
    }
    return false;
}

void CPUAffinityManager::bindReadThread(pid_t tid) const
{
    if (enable())
    {
        setAffinity(tid, read_cpu_set);
    }
}

void CPUAffinityManager::bindWriteThread(pid_t tid) const
{
    if (enable())
    {
        setAffinity(tid, write_cpu_set);
    }
}

void CPUAffinityManager::bindSelfReadThread() const
{
    LOG_INFO(log, "Thread: " << ::getThreadName() << " bindReadThread.");
    // If tid is zero, then the calling thread is used.
    bindReadThread(0);
}

void CPUAffinityManager::bindSelfWriteThread() const
{
    LOG_INFO(log, "Thread: " << ::getThreadName() << " bindWriteThread.");
    // If tid is zero, then the calling thread is used.
    bindWriteThread(0);
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
        bindSelfReadThread();
        is_binding = true;
    }
}

std::string CPUAffinityManager::toString() const
{
// clang-format off
#ifdef __linux__
    return "enable " + std::to_string(enable()) + " read_cpu_percent " + std::to_string(read_cpu_percent) +
        " cpu_cores " + std::to_string(cpu_cores) + " read_cpu_set " + cpuSetToString(read_cpu_set) +
        " write_cpu_set " + cpuSetToString(write_cpu_set);
#elif
    return "not support";
#endif
    // clang-format on
}

void CPUAffinityManager::initCPUSet()
{
    int read_cpu_cores = getReadCPUCores();
    int write_cpu_cores = getWriteCPUCores();
    // [0, write_cpu_cores) is for write threads.
    initCPUSet(write_cpu_set, 0, write_cpu_cores);
    // [write_cpu_cores, write_cpu_cores + read_cpu_cores) is for read threads.
    initCPUSet(read_cpu_set, write_cpu_cores, read_cpu_cores);
}

int CPUAffinityManager::getCPUCores() const
{
    return cpu_cores;
}

int CPUAffinityManager::getReadCPUCores() const
{
    return getCPUCores() * read_cpu_percent / 100;
}

int CPUAffinityManager::getWriteCPUCores() const
{
    return getCPUCores() - getReadCPUCores();
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
        throw DB::Exception(std::string("sched_setaffinity: ") + std::strerror(errno), DB::ErrorCodes::UNKNOWN_EXCEPTION);
    }
#endif
}

bool CPUAffinityManager::enable() const
{
    return 0 < read_cpu_percent && read_cpu_percent < 100 && cpu_cores > 1;
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
        if (isReadThread(t.second))
        {
            LOG_INFO(log, "Thread: " << t.first << " " << t.second << " bindReadThread.");
            bindReadThread(t.first);
        }
        else
        {
            LOG_INFO(log, "Thread: " << t.first << " " << t.second << " bindWriteThread.");
            bindWriteThread(t.first);
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
            LOG_ERROR(log, "Thread: " << t.first << " " << t.second << " sched_getaffinity ret " << ret << " error " << strerror(errno));
            continue;
        }
        LOG_INFO(log, "Thread: " << t.first << " " << t.second << " bind on CPU: " << cpuSetToString(cpu_set));
        if (isReadThread(t.second) && !CPU_EQUAL(&cpu_set, &read_cpu_set))
        {
            LOG_ERROR(log, "Thread: " << t.first << " " << t.second << " is read thread and bind CPU info is error.");
        }
        else if (!isReadThread(t.second) && !CPU_EQUAL(&cpu_set, &write_cpu_set))
        {
            LOG_ERROR(log, "Thread: " << t.first << " " << t.second << " is write thread and bind CPU info is error.");
        }
    }
}

} // namespace DB