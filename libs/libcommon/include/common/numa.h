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

#pragma once

#if defined(__linux__)
#include <sys/syscall.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>

#ifndef MPOL_PREFERRED
#define MPOL_PREFERRED 1
#endif
#endif

namespace common::numa
{

static inline size_t getNumaNode()
{
#ifdef SYS_getcpu
    uint64_t node = 0;
    uint64_t ncpu = 0;
    int64_t err = syscall(SYS_getcpu, &ncpu, &node, NULL);
    if (err != 0)
        return 0;
    return node;
#else
    return 0;
#endif
}

static inline size_t getNumaCount()
{
#if defined(__linux__)
    char buf[128];
    unsigned node = 0;
    for (node = 0; node < 256; node++)
    {
        snprintf(buf, 127, "/sys/devices/system/node/node%u", node + 1);
        if (access(buf, R_OK) != 0)
            break;
    }
    return (node + 1);
#else
    return 1;
#endif
}

static inline void bindMemoryToNuma(void * address, size_t len, size_t node)
{
#if defined(SYS_mbind)
    uintptr_t numa_mask = (1UL << node);
    syscall(SYS_mbind, address, len, MPOL_PREFERRED, &numa_mask, 8 * sizeof(uintptr_t), 0);
#endif
}

} // namespace common::numa
