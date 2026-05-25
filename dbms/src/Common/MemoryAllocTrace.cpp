// Copyright 2024 PingCAP, Inc.
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

#include <Common/MemoryAllocTrace.h>
#include <common/config_common.h> // Included for `USE_JEMALLOC`

#include <fstream>
#include <sstream>
#include <string_view>

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

namespace DB
{
std::tuple<uint64_t *, uint64_t *> getAllocDeallocPtr()
{
#if USE_JEMALLOC
    uint64_t * ptr1 = nullptr;
    size_t size1 = sizeof(ptr1);
    je_mallctl("thread.allocatedp", reinterpret_cast<void *>(&ptr1), &size1, nullptr, 0);
    uint64_t * ptr2 = nullptr;
    size_t size2 = sizeof(ptr2);
    je_mallctl("thread.deallocatedp", reinterpret_cast<void *>(&ptr2), &size2, nullptr, 0);
    return std::make_tuple(ptr1, ptr2);
#else
    return std::make_tuple(nullptr, nullptr);
#endif
}

static bool parseStatusFieldKb(const std::string & line, std::string_view field_name, UInt64 & out_kb)
{
    if (line.rfind(field_name, 0) != 0)
        return false;

    const auto colon_pos = line.find(':');
    if (colon_pos == std::string::npos)
        return false;

    std::istringstream iss(line.substr(colon_pos + 1));
    iss >> out_kb;
    return !iss.fail();
}

static bool parseStatusFieldInt(const std::string & line, std::string_view field_name, Int64 & out_value)
{
    if (line.rfind(field_name, 0) != 0)
        return false;

    const auto colon_pos = line.find(':');
    if (colon_pos == std::string::npos)
        return false;

    std::istringstream iss(line.substr(colon_pos + 1));
    iss >> out_value;
    return !iss.fail();
}

bool process_mem_usage(
    UInt64 & resident_bytes,
    UInt64 & rss_file_bytes,
    Int64 & cur_proc_num_threads,
    UInt64 & cur_virt_size)
{
    std::ifstream status_stream("/proc/self/status", std::ios_base::in);
    if (!status_stream.is_open())
        return false;

    UInt64 vm_rss_kb = 0;
    UInt64 rss_file_kb = 0;
    UInt64 vm_size_kb = 0;
    Int64 threads = 1;

    std::string line;
    while (std::getline(status_stream, line))
    {
        if (parseStatusFieldKb(line, "VmRSS", vm_rss_kb))
            resident_bytes = vm_rss_kb * 1024;
        if (parseStatusFieldKb(line, "RssFile", rss_file_kb))
            rss_file_bytes = rss_file_kb * 1024;
        if (parseStatusFieldKb(line, "VmSize", vm_size_kb))
            cur_virt_size = vm_size_kb * 1024;
        if (parseStatusFieldInt(line, "Threads", threads))
            cur_proc_num_threads = threads;
    }

    return true;
}

ProcessMemoryUsage get_process_mem_usage()
{
    UInt64 raw_rss = 0;
    UInt64 rss_file = 0;
    Int64 cur_proc_num_threads = 1;
    UInt64 cur_virt_size = 0;
    process_mem_usage(raw_rss, rss_file, cur_proc_num_threads, cur_virt_size);

    return ProcessMemoryUsage{
        raw_rss,
        rss_file,
        cur_virt_size,
        cur_proc_num_threads,
    };
}

} // namespace DB
