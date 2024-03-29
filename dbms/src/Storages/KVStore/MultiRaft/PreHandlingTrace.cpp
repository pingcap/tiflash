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

#include <Storages/KVStore/MultiRaft/PreHandlingTrace.h>

#pragma GCC diagnostic push
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
// include to suppress warnings on NO_THREAD_SAFETY_ANALYSIS. clang can't work without this include, don't know why
#include <grpcpp/security/credentials.h>
#pragma GCC diagnostic pop

namespace DB
{

std::shared_ptr<PreHandlingTrace::Item> PreHandlingTrace::registerTask(uint64_t region_id) NO_THREAD_SAFETY_ANALYSIS
{
    // Automaticlly override the old one.
    auto _ = genLockGuard();
    auto b = std::make_shared<Item>();
    tasks[region_id] = b;
    return b;
}

std::shared_ptr<PreHandlingTrace::Item> PreHandlingTrace::deregisterTask(uint64_t region_id) NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    auto it = tasks.find(region_id);
    if (it != tasks.end())
    {
        auto b = it->second;
        tasks.erase(it);
        return b;
    }
    return nullptr;
}
bool PreHandlingTrace::hasTask(uint64_t region_id) NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    return tasks.find(region_id) != tasks.end();
}

} // namespace DB
