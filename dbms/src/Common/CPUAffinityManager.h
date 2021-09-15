#pragma once

#include <string>
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
// Bind thread on logical CPU core.
class CPUAffinityManager
{
public:
    CPUAffinityManager(int read_cpu_pencent_, int cpu_cores_, Poco::Util::LayeredConfiguration & config);

    void bindReadThread(pid_t tid) const;
    void bindWriteThread(pid_t tid) const;

    void bindSelfReadThread() const;
    void bindSelfWriteThread() const;
    void bindSelfGrpcThread() const;

    std::string toString() const;

    void bindThreadCPUAffinity() const;

private:
    void initCPUSet();
    int getCPUCores() const;
    int getReadCPUCores() const;
    int getWriteCPUCores() const;
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

    void initReadThreadNames(Poco::Util::LayeredConfiguration & config);
    bool isReadThread(const std::string & name) const;

    int read_cpu_percent;
    int cpu_cores;
    cpu_set_t read_cpu_set;
    cpu_set_t write_cpu_set;
    std::vector<std::string> read_threads;
    Poco::Logger * log;

    // Disable copy and move
    CPUAffinityManager(const CPUAffinityManager &) = delete;
    CPUAffinityManager & operator=(const CPUAffinityManager &) = delete;
    CPUAffinityManager(CPUAffinityManager &&) = delete;
    CPUAffinityManager & operator=(CPUAffinityManager &&) = delete;
};
} // namespace DB