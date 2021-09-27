#pragma once

#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace Poco
{
class Logger;
namespace Util
{
class LayeredConfiguration;
}
} // namespace Poco

namespace DB
{
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
    // cop-pool and batch-cop-pool are the thread name of thread-pool that handle coprocessor request.
    // grpcpp_sync_ser is the thread name of grpc sync request thread-pool. However, this thread-pool is resize dynamically and we set these threads' cpu affinity in FlashService for simplicity.
    // Query threads of MPP tasks are created dynamiccally and we set these threads' cpu affinity when they are created.
    std::vector<std::string> query_threads = {"cop-pool", "batch-cop-pool", "grpcpp_sync_ser"};
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
    void init(const CPUAffinityConfig &)
    {}

    void bindQueryThread(pid_t) const {}
    void bindOtherThread(pid_t) const {}

    void bindSelfQueryThread() const {}
    void bindSelfOtherThread() const {}
    void bindSelfGrpcThread() const {}

    std::string toString() const { return "Not Support"; }

    void bindThreadCPUAffinity() const {}
#endif

private:
#ifdef __linux__
    void initCPUSet();
    int getCPUCores() const;
    int getQueryCPUCores() const;
    int getOtherCPUCores() const;
    void initCPUSet(cpu_set_t & cpu_set, int start, int count);
    void checkThreadCPUAffinity() const;
    // Bind thread t on cpu_set.
    void setAffinity(pid_t tid, const cpu_set_t & cpu_set) const;
    bool enable() const;

    std::string cpuSetToString(const cpu_set_t & cpu_set) const;
    std::vector<int> cpuSetToVec(const cpu_set_t & cpu_set) const;

    std::unordered_map<pid_t, std::string> getThreads(pid_t pid) const;
    std::vector<pid_t> getThreadIDs(const std::string & dir) const;
    std::string getThreadName(const std::string & fname) const;
    std::string getShortFilename(const std::string & path) const;
    bool isQueryThread(const std::string & name) const;

    cpu_set_t query_cpu_set;
    cpu_set_t other_cpu_set;
#endif

    int query_cpu_percent;
    int cpu_cores;
    std::vector<std::string> query_threads;
    Poco::Logger * log;

    CPUAffinityManager();
    // Disable copy and move
    CPUAffinityManager(const CPUAffinityManager &) = delete;
    CPUAffinityManager & operator=(const CPUAffinityManager &) = delete;
    CPUAffinityManager(CPUAffinityManager &&) = delete;
    CPUAffinityManager & operator=(CPUAffinityManager &&) = delete;
};
} // namespace DB