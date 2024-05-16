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

#include <common/config_common.h>

#ifdef USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

namespace DB
{
std::tuple<uint64_t *, uint64_t *> getAllocDeallocPtr()
{
#ifdef USE_JEMALLOC
    uint64_t * ptr1 = nullptr;
    uint64_t size1 = sizeof ptr1;
    je_mallctl("thread.allocatedp", (void *)&ptr1, &size1, nullptr, 0);
    uint64_t * ptr2 = nullptr;
    uint64_t size2 = sizeof ptr2;
    je_mallctl("thread.deallocatedp", (void *)&ptr2, &size2, nullptr, 0);
    return std::make_tuple(ptr1, ptr2);
#else
    return std::make_tuple(nullptr, nullptr);
#endif
}
} // namespace DB