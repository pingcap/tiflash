#include <Common/CPUAffinityManager.h>
#include <Common/Exception.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/Logger.h>
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

CPUAffinityManager::CPUAffinityManager(int read_cpu_percent_, int cpu_cores_)
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
}

void CPUAffinityManager::setReadThread(pid_t tid) const
{
    if (enable())
    {
        setAffinity(tid, read_cpu_set);
    }
}

void CPUAffinityManager::setWriteThread(pid_t tid) const
{
    if (enable())
    {
        setAffinity(tid, write_cpu_set);
    }
}

void CPUAffinityManager::setBackgroundThread(pid_t tid) const
{
    // Currently, read threads and background threads are running on the same cpu set.
    setReadThread(tid);
}

void CPUAffinityManager::setSelfReadThread() const
{
    // If tid is zero, then the calling thread is used.
    setReadThread(0);
}

void CPUAffinityManager::setSelfWriteThread() const
{
    // If tid is zero, then the calling thread is used.
    setWriteThread(0);
}

std::string CPUAffinityManager::toString() const
{
#ifdef __linux__
    return "enable " + std::to_string(enable()) + " read_cpu_percent " + std::to_string(read_cpu_percent) + " cpu_cores " + std::to_string(cpu_cores) + " read_cpu_set " + cpuSetToString(read_cpu_set) + " write_cpu_set " + cpuSetToString(write_cpu_set);
#elif
    return "not support";
#endif
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

bool isGrpcThread(const std::string & name)
{
    return name.find("grpcpp_sync_ser") == 0 || name.find("grpc_global_tim") == 0 || name.find("grpc-server") == 0;
}

bool shouldBindOnReadCPUSet(const std::string & name)
{
    // clang-format off
    return name.find("BkgPool") == 0 ||
        name.find("sst-importer") == 0 ||
        name.find("status-server") == 0 ||
        name.find("pd-worker") == 0 ||
        name.find("stats-monitor") == 0 ||
        name.find("time updater") == 0||
        name.find("steady-timer") == 0 ||
        name.find("cleanup-worker") == 0 ||
        name.find("pdmonitor") == 0 ||
        name.find("resolver-execut") == 0 ||
        name.find("default-executo") == 0 ||
        name.find("SignalListener") == 0 ||
        name.find("tso-worker") == 0 ||
        name.find("sched-hi-pri") == 0 ||
        name.find("sched-wkr") == 0 ||
        name.find("unf-rd-pool") == 0 ||
        name.find("rocksdb:dump_st") == 0 ||
        name.find("rocksdb:pst_st") == 0 ||
        name.find("rocksdb:high") == 0 ||
        name.find("snap-sender") == 0 ||
        name.find("jemalloc_bg_thd") == 0 ||
        name.find("rocksdb:low") == 0 ||
        name.find("slogger") == 0 ||
        name.find("time-monitor") == 0 ||
        name.find("backtrace-loade") == 0 ||
        name.find("gc-worker") == 0 ||
        name.find("background") == 0 ||
        name.find("debugger") == 0 ||
        name.find("region-collecto") == 0 ||
        name.find("HTTPServer") == 0 ||
        name.find("transport-stats") == 0 ||
        name.find("PDLeaderLoop") == 0 ||
        name.find("snap-handler") == 0 ||
        name.find("PDUpdateTS") == 0 ||
        name.find("CfgReloader") == 0 ||
        name.find("TiFlashMain") == 0 ||
        name.find("AsyncMetrics") == 0 ||
        name.find("Prometheus") == 0 ||
        name.find("UserCfgReloader") == 0 ||
        name.find("civetweb-master") == 0 ||
        name.find("civetweb-worker") == 0 ||
        name.find("SessionCleaner") == 0 ||
        name.find("ClusterManager") == 0 ||
        name.find("timer") == 0 ||
        name.find("cop-pool") == 0 ||
        name.find("batch-cop-pool") == 0;
        // clang-format on
}

bool shouldBindOnWriteCPUSet(const std::string & name)
{
    // clang-format off
    return name.find("apply") == 0 ||
        name.find("region-task") == 0 ||
        name.find("region-worker") == 0 ||
        name.find("raft-stream") == 0 ||
        name.find("RaftStoreProxy") == 0 ||
        name.find("raftstore") == 0 ||
        name.find("readindex-timer") == 0 ||
        name.find("TCPServer") == 0 ||
        isGrpcThread(name);
    // clang-format on
}

/*
std::vector<std::string> CPUAffinityManager::getShouldBindOnReadCPUSetThreadNames(Poco::Util::LayeredConfiguration & config)
{
    std::vector<std::string> threads;
    if (config.has("flash.cpu"))
    {
        std::string cpu_config = config.getString("flash.cpu");
        std::istringstream ss(cpu_config);
        cpptoml::parser p(ss);
        auto table = p.parse();
        if (auto read_threads = table->get_qualified_array_of<std::string>("cpu.read_threads"); read_threads)
        {
            for (const auto & name : *read_threads)
            {
                threads.push_back(name);
            }
        }
    }
    
    // Default threads
    if (threads.empty())
    {
        threads = {"cop-pool", "batch-cop-pool", "BkgPool"};
    }

    return threads;
}*/

void CPUAffinityManager::setThreadCPUAffinity() const
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
        if (shouldBindOnWriteCPUSet(t.second))
        {
            if (!CPU_EQUAL(&cpu_set, &write_cpu_set))
            {
                LOG_INFO(log, "Thread: " << t.first << " " << t.second << " setWriteThread.");
                setWriteThread(t.first);
            }
        }
        else if (shouldBindOnReadCPUSet(t.second))
        {
            if (!CPU_EQUAL(&cpu_set, &read_cpu_set))
            {
                LOG_INFO(log, "Thread: " << t.first << " " << t.second << " setReadThread.");
                setReadThread(t.first);
            }
        }
        else
        {
            LOG_ERROR(log, "Thread: " << t.first << " " << t.second << "not recognized.");
        }
    }
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
        if ((shouldBindOnWriteCPUSet(t.second) && !CPU_EQUAL(&cpu_set, &write_cpu_set)) || (shouldBindOnReadCPUSet(t.second) && !CPU_EQUAL(&cpu_set, &read_cpu_set)))
        {
            LOG_ERROR(log, "Thread: " << t.first << " " << t.second << " bind CPU info is error.");
        }
        else if (!shouldBindOnWriteCPUSet(t.second) && !shouldBindOnReadCPUSet(t.second))
        {
            LOG_ERROR(log, "Thread: " << t.first << " " << t.second << "not recognized.");
        }
    }
}

} // namespace DB