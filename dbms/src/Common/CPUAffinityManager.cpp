#include <errno.h>
#include <cstring>

#include <Common/CPUAffinityManager.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_EXCEPTION;
extern const int CPUID_ERROR;
}

CPUAffinityManager::CPUAffinityManager(int read_cpu_percent_, int cpu_cores_) : read_cpu_percent(read_cpu_percent_), cpu_cores(cpu_cores_)
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
    if (!enable())
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
    return "enable " + std::to_string(enable()) + " read_cpu_percent " + std::to_string(read_cpu_percent) + 
        " cpu_cores " + std::to_string(cpu_cores) + " read_cpu_set " + cpuSetToString(read_cpu_set) + 
        " write_cpu_set " + cpuSetToString(write_cpu_set);
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

int CPUAffinityManager::getReadCPUCores() const { return getCPUCores() * read_cpu_percent / 100; }

int CPUAffinityManager::getWriteCPUCores() const { return getCPUCores() - getReadCPUCores(); }

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
    std::string s;
    for (int i = 0; i < static_cast<int>(sizeof(cpu_set)); i++)
    {
        if (CPU_ISSET(i, &cpu_set))
        {
            s += std::to_string(i) + " ";
        }
    }
    return s;
}
} // namespace DB