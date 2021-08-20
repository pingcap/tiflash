#pragma once

#include <vector>
#include <string>

namespace DB
{

// Bind thread on logical CPU core.
class CPUAffinityManager
{
public:
    CPUAffinityManager(int read_cpu_pencent_, int cpu_cores_);
    void setReadThread(pid_t tid) const;
    void setWriteThread(pid_t tid) const;
    void setBackgroundThread(pid_t tid) const;
    void setSelfReadThread() const;
    void setSelfWriteThread() const;
    std::string toString() const;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    void initCPUSet();
    int getCPUCores() const;
    int getReadCPUCores() const;
    int getWriteCPUCores() const;
    void initCPUSet(cpu_set_t & cpu_set, int start, int count);
    // Bind thread t on cpu_set.
    void setAffinity(pid_t tid, const cpu_set_t & cpu_set) const;
    bool enable() const;
    std::string cpuSetToString(const cpu_set_t & cpu_set) const;

    int read_cpu_percent;
    int cpu_cores;
    cpu_set_t read_cpu_set;
    cpu_set_t write_cpu_set;
};

//using CPUAffinityManagerPtr = std::shared_ptr<CPUAffinityManager>;

} // namespace DB