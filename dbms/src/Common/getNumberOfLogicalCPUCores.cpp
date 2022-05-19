#include <Common/Exception.h>
#include <Common/getNumberOfLogicalCPUCores.h>
#include <common/likely.h>

#include <thread>
#include "common/logger_useful.h"

#if defined(__linux__)
#include <cmath>
#include <fstream>
#endif // __linux__

namespace DB::ErrorCodes
{
extern const int CPUID_ERROR;
} // namespace DB::ErrorCodes

#if defined(__linux__)
// Try to look at cgroups limit if it is available.

// read int a value from file
static inline int read_int_from(const char * filename, int default_value)
{
    std::ifstream infile(filename);
    if (!infile.is_open())
    {
        return default_value;
    }
    int idata;
    if (infile >> idata)
    {
        return idata;
    }
    else
    {
        return default_value;
    }
}

// logical_cpu_cores = min(cpuset.cpus, quota/period)
static unsigned calCPUCores(int cgroup_quota, int cgroup_period, unsigned cpuset_count)
{
    unsigned quota_count = cpuset_count;

    if (cgroup_quota > -1 && cgroup_period > 0)
    {
        quota_count = ceil(static_cast<float>(cgroup_quota) / static_cast<float>(cgroup_period));
    }

    return std::min(cpuset_count, quota_count);
}

static unsigned read_cpuset_count_from(const char * filename, unsigned default_value)
{
    // cpuset.cpus
    // A read-write multiple values file which exists on non-root cpuset-enabled cgroups.
    // It lists the requested CPUs to be used by tasks within this cgroup. The actual list of CPUs to be granted, however, is subjected to constraints imposed by its parent and can differ from the requested CPUs.
    // The CPU numbers are comma-separated numbers or ranges. For example:

    // # cat cpuset.cpus
    // 0-4,6,8-10

    // An empty value indicates that the cgroup is using the same setting as the nearest cgroup ancestor with a non-empty "cpuset.cpus" or all the available CPUs if none is found.
    // The value of "cpuset.cpus" stays constant until the next update and won't be affected by any CPU hotplug events.
    std::ifstream infile(filename);
    if (!infile.is_open())
    {
        return default_value;
    }
    std::string line;
    std::getline(infile, line);
    unsigned cpu_count = 0;
    size_t first = 0;
    while (first < line.size())
    {
        size_t last = line.find(',', first);
        if (last == std::string::npos)
        {
            last = line.size();
        }
        std::string cpu_set = line.substr(first, last - first);
        size_t dash = cpu_set.find('-');
        if (dash != std::string::npos)
        {
            std::string start_str = cpu_set.substr(0, dash);
            std::string end_str = cpu_set.substr(dash + 1);
            int start = std::stoi(start_str);
            int end = std::stoi(end_str);
            cpu_count += end - start + 1;
        }
        else
        {
            cpu_count++;
        }
        first = last + 1;
    }
    return cpu_count;
}

static std::pair<int, int> read_quota_and_period_v2(const char * filename)
{
    // cpu.max
    // A read-write two value file which exists on non-root cgroups. The default is "max 100000".
    // The maximum bandwidth limit. It's in the following format:

    // $MAX $PERIOD

    // which indicates that the group may consume upto $MAX in each $PERIOD duration.
    // "max" for $MAX indicates no limit. If only one number is written, $MAX is updated.
    std::ifstream infile(filename);
    if (!infile.is_open())
    {
        return {-2, -2};
    }
    std::string quota;
    int period;
    infile >> quota >> period;
    return {((quota == "max") ? period : std::stoi(quota)), period};
}


static unsigned getCGroupDefaultLimitedCPUCores(unsigned default_cpu_count)
{
    // update default cpu count to the count of cpuset.cpus
    default_cpu_count = read_cpuset_count_from("/sys/fs/cgroup/cpuset/cpuset.cpus", default_cpu_count);
    // Return the number of milliseconds per period process is guaranteed to run.
    // -1 for no quota
    int cgroup_quota = read_int_from("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", -1);
    int cgroup_period = read_int_from("/sys/fs/cgroup/cpu/cpu.cfs_period_us", -1);

    return calCPUCores(cgroup_quota, cgroup_period, default_cpu_count);
}

static unsigned getCGroupLimitedCPUCores(unsigned default_cpu_count)
{
    std::string cgroup_controllers = "/sys/fs/cgroup/cgroup.controllers";
    std::ifstream cgroup_controllers_info(cgroup_controllers);
    // If cgroup.controllers is open, we assume we are running on a system with cgroups v2
    // Otherwise v1
    bool enabled_v2 = cgroup_controllers_info.is_open();
    std::string cpu_filter = enabled_v2 ? "0::" : "cpuset:";
    std::ifstream cgroup_cpu_info("/proc/self/cgroup");
    if (cgroup_cpu_info.is_open())
    {
        std::string line;
        while (std::getline(cgroup_cpu_info, line))
        {
            std::string::size_type cpu_str_idx = line.find(cpu_filter);
            if (cpu_str_idx != std::string::npos)
            {
                line = line.substr(cpu_str_idx + cpu_filter.length(), line.length());
                if (enabled_v2)
                {
                    auto [cgroup_quota, cgroup_period] = read_quota_and_period_v2(fmt::format("/sys/fs/cgroup{}/cpu.max", line).c_str());
                    // If can't read cgroup_quota here, it means current process may in docker
                    if (cgroup_quota == -2)
                    {
                        return getCGroupDefaultLimitedCPUCores(default_cpu_count);
                    }
                    default_cpu_count = read_cpuset_count_from(fmt::format("/sys/fs/cgroup{}/cpuset/cpuset.cpus", line).c_str(), default_cpu_count);
                    return calCPUCores(cgroup_quota, cgroup_period, default_cpu_count);
                }
                else
                {
                    int cgroup_quota = read_int_from(fmt::format("/sys/fs/cgroup{}/cpu.cfs_quota_us", line).c_str(), -2);
                    // If can't read cgroup_quota here, it means current process may in docker
                    if (cgroup_quota == -2)
                    {
                        return getCGroupDefaultLimitedCPUCores(default_cpu_count);
                    }
                    int cgroup_period = read_int_from(fmt::format("/sys/fs/cgroup{}/cpu.cfs_period_us", line).c_str(), -2);
                    default_cpu_count = read_cpuset_count_from(fmt::format("/sys/fs/cgroup/cpuset{}/cpuset.cpus", line).c_str(), default_cpu_count);
                    return calCPUCores(cgroup_quota, cgroup_period, default_cpu_count);
                }
            }
        }
    }
    return getCGroupDefaultLimitedCPUCores(default_cpu_count);
}
#endif // __linux__

unsigned getNumberOfLogicalCPUCores()
{
    unsigned logical_cpu_count = std::thread::hardware_concurrency();
#if defined(__linux__)
    logical_cpu_count = getCGroupLimitedCPUCores(logical_cpu_count);
#endif // __linux__
    return logical_cpu_count;
}