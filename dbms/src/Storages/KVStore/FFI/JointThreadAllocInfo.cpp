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

#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>
#include <Storages/KVStore/FFI/JointThreadAllocInfo.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>

#include <Common/MemoryAllocrace.cpp>
#include <mutex>
#include <thread>
#include <unordered_set>


namespace DB
{

JointThreadInfoJeallocMap::JointThreadInfoJeallocMap()
{
    monitoring_thread = new std::thread([&]() {
        setThreadName("ThdMemTrace");
        while (true)
        {
            using namespace std::chrono_literals;
            std::unique_lock l(monitoring_mut);
            monitoring_cv.wait_for(l, 5000ms, [&]() { return is_terminated; });
            if (is_terminated)
                return;
            recordThreadAllocInfo();
        }
    });
}

void JointThreadInfoJeallocMap::recordThreadAllocInfo()
{
    recordThreadAllocInfoForKVStore();
}

JointThreadInfoJeallocMap::~JointThreadInfoJeallocMap()
{
    stopThreadAllocInfo();
}

static std::string getThreadNameAggPrefix(const std::string_view & s)
{
    if (auto pos = s.find_last_of('-'); pos != std::string::npos)
    {
        return std::string(s.begin(), s.begin() + pos);
    }
    return std::string(s.begin(), s.end());
}

void JointThreadInfoJeallocMap::reportThreadAllocInfoImpl(
    std::unordered_map<std::string, ThreadInfoJealloc> & m,
    std::string_view thdname,
    ReportThreadAllocateInfoType type,
    uint64_t value)
{
    // Many threads have empty name, better just not handle.
    if (thdname.empty())
        return;
    std::string tname(thdname.begin(), thdname.end());
    switch (type)
    {
    case ReportThreadAllocateInfoType::Reset:
    {
        auto & metrics = TiFlashMetrics::instance();
        metrics.registerProxyThreadMemory(getThreadNameAggPrefix(tname));
        {
            std::unique_lock l(memory_allocation_mut);
            m.insert_or_assign(tname, ThreadInfoJealloc());
        }
        break;
    }
    case ReportThreadAllocateInfoType::Remove:
    {
        std::unique_lock l(memory_allocation_mut);
        m.erase(tname);
        break;
    }
    case ReportThreadAllocateInfoType::AllocPtr:
    {
        std::shared_lock l(memory_allocation_mut);
        if (value == 0)
            return;
        auto it = m.find(tname);
        if unlikely (it == m.end())
        {
            return;
        }
        it->second.allocated_ptr = value;
        break;
    }
    case ReportThreadAllocateInfoType::DeallocPtr:
    {
        std::shared_lock l(memory_allocation_mut);
        if (value == 0)
            return;
        auto it = m.find(tname);
        if unlikely (it == m.end())
        {
            return;
        }
        it->second.deallocated_ptr = value;
        break;
    }
    }
}

void JointThreadInfoJeallocMap::reportThreadAllocInfoForKVStore(
    std::string_view thdname,
    ReportThreadAllocateInfoType type,
    uint64_t value)
{
    reportThreadAllocInfoImpl(kvstore_map, thdname, type, value);
}

static const std::unordered_set<std::string> PROXY_RECORD_WHITE_LIST_THREAD_PREFIX = {"ReadIndexWkr"};

void JointThreadInfoJeallocMap::recordThreadAllocInfoForKVStore()
{
    std::shared_lock l(memory_allocation_mut);
    std::unordered_map<std::string, uint64_t> agg_allocate;
    std::unordered_map<std::string, uint64_t> agg_deallocate;
    for (const auto & [k, v] : kvstore_map)
    {
        auto agg_thread_name = getThreadNameAggPrefix(k);
        // Some thread may have shorter lifetime, we can't use this timed task here to upgrade.
        if (PROXY_RECORD_WHITE_LIST_THREAD_PREFIX.contains(agg_thread_name) && v.has_ptr())
        {
            agg_allocate[agg_thread_name] += v.allocated();
            agg_deallocate[agg_thread_name] += v.deallocated();
        }
    }
    for (const auto & [k, v] : agg_allocate)
    {
        auto & tiflash_metrics = TiFlashMetrics::instance();
        tiflash_metrics.setProxyThreadMemory(TiFlashMetrics::MemoryAllocType::Alloc, k, v);
    }
    for (const auto & [k, v] : agg_deallocate)
    {
        auto & tiflash_metrics = TiFlashMetrics::instance();
        tiflash_metrics.setProxyThreadMemory(TiFlashMetrics::MemoryAllocType::Dealloc, k, v);
    }
}

void JointThreadInfoJeallocMap::reportThreadAllocBatchForKVStore(
    std::string_view name,
    ReportThreadAllocateInfoBatch data)
{
    // Many threads have empty name, better just not handle.
    if (name.empty())
        return;
    // TODO(jemalloc-trace) Could be costy.
    auto k = getThreadNameAggPrefix(name);
    auto & tiflash_metrics = TiFlashMetrics::instance();
    tiflash_metrics.setProxyThreadMemory(TiFlashMetrics::MemoryAllocType::Alloc, k, data.alloc);
    tiflash_metrics.setProxyThreadMemory(TiFlashMetrics::MemoryAllocType::Dealloc, k, data.dealloc);
}

void JointThreadInfoJeallocMap::stopThreadAllocInfo()
{
    LOG_INFO(DB::Logger::get(), "Stop collecting thread alloc metrics");
    {
        std::unique_lock lk(monitoring_mut);
        // Only one caller can successfully stop the thread.
        if (is_terminated)
            return;
        if (monitoring_thread == nullptr)
            return;
        is_terminated = true;
        monitoring_cv.notify_all();
    }
    LOG_INFO(DB::Logger::get(), "JointThreadInfoJeallocMap shutdown, wait thread alloc monitor join");
    monitoring_thread->join();
    {
        std::unique_lock lk(monitoring_mut);
        delete monitoring_thread;
        monitoring_thread = nullptr;
    }
}

std::tuple<uint64_t *, uint64_t *> JointThreadInfoJeallocMap::getPtrs()
{
    return getAllocDeallocPtr();
}
} // namespace DB
