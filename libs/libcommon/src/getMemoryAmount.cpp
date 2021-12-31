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

#include "common/getMemoryAmount.h"

#include <sys/param.h>
#include <sys/types.h>
#include <unistd.h>

#include <fstream>
#if defined(BSD)
#include <sys/sysctl.h>
#endif

int64_t getPageSize()
{
    int64_t page_size = sysconf(_SC_PAGESIZE);
    if (page_size < 0)
        abort();
    return page_size;
}

/** Returns the size of physical memory (RAM) in bytes.
  * Returns 0 on unsupported platform
  */
uint64_t getMemoryAmount()
{
    int64_t num_pages = sysconf(_SC_PHYS_PAGES);
    if (num_pages <= 0)
        return 0;

    int64_t page_size = getPageSize();
    if (page_size <= 0)
        return 0;

    uint64_t memory_amount = num_pages * page_size;

#if defined(__linux__)
    // Try to lookup at the Cgroup limit
    std::ifstream cgroup_limit("/sys/fs/cgroup/memory/memory.limit_in_bytes");
    if (cgroup_limit.is_open())
    {
        uint64_t memory_limit = 0; // in case of read error
        cgroup_limit >> memory_limit;
        if (memory_limit > 0 && memory_limit < memory_amount)
            memory_amount = memory_limit;
    }
#endif // __linux__

    return memory_amount;
}
