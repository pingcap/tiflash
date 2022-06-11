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
#include <Storages/DeltaMerge/DeltaTreeMemory.h>
namespace DB::DM
{

static NumaAwareMemoryHierarchy::GlobalPagePool GLOBAL_PAGE_POOL {};

#pragma push_macro  ("thread_local")
#undef thread_local
static thread_local std::shared_ptr<NumaAwareMemoryHierarchy::ThreadLocalMemPool> THREAD_LOCAL_MEM_POOL = nullptr;
#pragma pop_macro  ("thread_local")

static inline std::shared_ptr<NumaAwareMemoryHierarchy::ThreadLocalMemPool> getThreadLocalMemPool() {
    if (!THREAD_LOCAL_MEM_POOL) {
        THREAD_LOCAL_MEM_POOL = std::make_shared<NumaAwareMemoryHierarchy::ThreadLocalMemPool>(&GLOBAL_PAGE_POOL.getFreeList());
    }
    return THREAD_LOCAL_MEM_POOL;
}

NumaAwareMemoryHierarchy::Client getClient(size_t size, size_t alignment)
{
    auto upstream = getThreadLocalMemPool();
    return { upstream, size, alignment};
}



}
