// Copyright 2022 PingCAP, Ltd.
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

#include <Common/Exception.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <fmt/format.h>

#include <thread>

#if defined(__x86_64__)
#include <cpuid/libcpuid.h>
#endif
#if defined(__linux__)
#include <cmath>
#include <fstream>
#endif

namespace DB
{
namespace ErrorCodes
{
extern const int CPUID_ERROR;
}
} // namespace DB

#if defined(__linux__)

// Try to look at cgroups limit if it is available.
static inline int read_int_from(const char * filename, int default_value)
{
    std::ifstream infile(filename);
    if (!infile.is_open())
    {
        return default_value;
    }
    int idata;
    if (infile >> idata)
        return idata;
    else
        return default_value;
}

unsigned calCPUCores(int cgroup_quota, int cgroup_period, int cgroup_share, unsigned default_cpu_count)
{
    unsigned quota_count = default_cpu_count;

    if (cgroup_quota > -1 && cgroup_period > 0)
    {
        quota_count = ceil(static_cast<float>(cgroup_quota) / static_cast<float>(cgroup_period));
    }

    // Convert 1024 to no shares setup
    if (cgroup_share == 1024)
        cgroup_share = -1;

#define PER_CPU_SHARES 1024
    unsigned share_count = default_cpu_count;
    if (cgroup_share > -1)
    {
        share_count = ceil(static_cast<float>(cgroup_share) / static_cast<float>(PER_CPU_SHARES));
    }

    return std::min(default_cpu_count, std::min(share_count, quota_count));
}

unsigned getCGroupDefaultLimitedCPUCores(unsigned default_cpu_count)
{
    // Return the number of milliseconds per period process is guaranteed to run.
    // -1 for no quota
    int cgroup_quota = read_int_from("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", -1);
    int cgroup_period = read_int_from("/sys/fs/cgroup/cpu/cpu.cfs_period_us", -1);
    // Share number (typically a number relative to 1024) (2048 typically expresses 2 CPUs worth of processing)
    int cgroup_share = read_int_from("/sys/fs/cgroup/cpu/cpu.shares", -1);

    return calCPUCores(cgroup_quota, cgroup_period, cgroup_share, default_cpu_count);
}

unsigned getCGroupLimitedCPUCores(unsigned default_cpu_count)
{
    std::string cpu_filter = "cpuset:";
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
                int cgroup_quota = read_int_from(fmt::format("/sys/fs/cgroup/cpu{}/cpu.cfs_quota_us", line).c_str(), -2);

                // If can't read cgroup_quota here
                // It means current process may in docker
                if (cgroup_quota == -2)
                {
                    return getCGroupDefaultLimitedCPUCores(default_cpu_count);
                }
                int cgroup_period = read_int_from(fmt::format("/sys/fs/cgroup/cpu{}/cpu.cfs_quota_us", line).c_str(), -1);
                int cgroup_share = read_int_from(fmt::format("/sys/fs/cgroup/cpu{}/cpu.shares", line).c_str(), -1);

                return calCPUCores(cgroup_quota, cgroup_period, cgroup_share, default_cpu_count);
            }
        }
    }

    return getCGroupDefaultLimitedCPUCores(default_cpu_count);
}
#endif // __linux__

unsigned getNumberOfPhysicalCPUCores()
{
    static const unsigned number = [] {
        unsigned cpu_count = 0; // start with an invalid num
#if defined(__x86_64__)
        do
        {
            cpu_raw_data_t raw_data;
            cpu_id_t data;

            /// On Xen VMs, libcpuid returns wrong info (zero number of cores). Fallback to alternative method.
            /// Also, libcpuid does not support some CPUs like AMD Hygon C86 7151.
            if (0 != cpuid_get_raw_data(&raw_data) || 0 != cpu_identify(&raw_data, &data) || data.num_logical_cpus == 0)
            {
                // Just fallback
                break;
            }

            cpu_count = data.num_cores * data.total_logical_cpus / data.num_logical_cpus;

            /// Also, libcpuid gives strange result on Google Compute Engine VMs.
            /// Example:
            ///  num_cores = 12,            /// number of physical cores on current CPU socket
            ///  total_logical_cpus = 1,    /// total number of logical cores on all sockets
            ///  num_logical_cpus = 24.     /// number of logical cores on current CPU socket
            /// It means two-way hyper-threading (24 / 12), but contradictory, 'total_logical_cpus' == 1.
        } while (false);
#endif

        /// As a fallback (also for non-x86 architectures) assume there are no hyper-threading on the system.
        /// (Actually, only Aarch64 is supported).
        if (cpu_count == 0)
            cpu_count = std::thread::hardware_concurrency();
        if (cpu_count == 0)
            throw DB::Exception("Cannot Identify CPU: Unsupported processor", DB::ErrorCodes::CPUID_ERROR);

#if defined(__linux__)
        cpu_count = getCGroupLimitedCPUCores(cpu_count);
#endif // __linux__
        return cpu_count;
    }();
    return number;
}
