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

bool process_mem_usage(double & resident_set, Int64 & cur_proc_num_threads, UInt64 & cur_virt_size)
{
    resident_set = 0.0;

    // 'file' stat seems to give the most reliable results
    std::ifstream stat_stream("/proc/self/stat", std::ios_base::in);
    // if "/proc/self/stat" is not supported
    if (!stat_stream.is_open())
        return false;

    // dummy vars for leading entries in stat that we don't care about
    std::string pid, comm, state, ppid, pgrp, session, tty_nr;
    std::string tpgid, flags, minflt, cminflt, majflt, cmajflt;
    std::string utime, stime, cutime, cstime, priority, nice;
    std::string itrealvalue, starttime;

    // the field we want
    Int64 rss;

    stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr >> tpgid >> flags >> minflt >> cminflt
        >> majflt >> cmajflt >> utime >> stime >> cutime >> cstime >> priority >> nice >> cur_proc_num_threads
        >> itrealvalue >> starttime >> cur_virt_size >> rss; // don't care about the rest

    stat_stream.close();

    Int64 page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024; // in case x86-64 is configured to use 2MB pages
    resident_set = rss * page_size_kb;
    return true;
}

ProcessMemoryUsage get_process_mem_usage()
{
    double resident_set;
    Int64 cur_proc_num_threads = 1;
    UInt64 cur_virt_size = 0;
    process_mem_usage(resident_set, cur_proc_num_threads, cur_virt_size);
    resident_set *= 1024; // transfrom from KB to bytes
    return ProcessMemoryUsage{
        static_cast<size_t>(resident_set),
        cur_virt_size,
        cur_proc_num_threads,
    };
}

} // namespace DB
