#include <Common/Exception.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <thread>

#if defined(__x86_64__)
    #include <cpuid/libcpuid.h>
#endif

namespace DB { namespace ErrorCodes { extern const int CPUID_ERROR; }}

unsigned getNumberOfPhysicalCPUCores()
{
    unsigned res = 0;
#if defined(__x86_64__)

    cpu_raw_data_t raw_data;
    cpu_id_t data;
    if (0 == cpuid_get_raw_data(&raw_data) && 0 == cpu_identify(&raw_data, &data) && data.num_logical_cpus != 0)
    {
        res = data.num_cores * data.total_logical_cpus / data.num_logical_cpus;
    }

    /// Also, libcpuid gives strange result on Google Compute Engine VMs.
    /// Example:
    ///  num_cores = 12,            /// number of physical cores on current CPU socket
    ///  total_logical_cpus = 1,    /// total number of logical cores on all sockets
    ///  num_logical_cpus = 24.     /// number of logical cores on current CPU socket
    /// It means two-way hyper-threading (24 / 12), but contradictory, 'total_logical_cpus' == 1.
    if (res != 0)
    {
        return res;
    }
    // else fallback
#endif

    /// As a fallback (also for non-x86 architectures) assume there are no hyper-threading on the system.
    /// (Actually, only Aarch64 is supported).
    res = std::thread::hardware_concurrency();
    if (res == 0)
    {
        throw DB::Exception("Cannot Identify CPU: Unsupported processor", DB::ErrorCodes::CPUID_ERROR);
    }
    return res;
}
