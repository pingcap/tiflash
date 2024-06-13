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
#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>
#include <Storages/KVStore/FFI/JointThreadAllocInfo.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/Page/V3/PageDirectory.h>

#include <magic_enum.hpp>
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
    recordThreadAllocInfoForProxy();
    recordThreadAllocInfoForStorage();
    recordClassdAlloc();
}

JointThreadInfoJeallocMap::~JointThreadInfoJeallocMap()
{
    stopThreadAllocInfo();
}

static std::string getThreadNameAggPrefix(const std::string_view & s, char delimer)
{
    if (delimer == '\0')
        return std::string(s.begin(), s.end());
    if (auto pos = s.find_last_of(delimer); pos != std::string::npos)
    {
        return std::string(s.begin(), s.begin() + pos);
    }
    return std::string(s.begin(), s.end());
}

void JointThreadInfoJeallocMap::reportThreadAllocInfoImpl(
    JointThreadInfoJeallocMap::AllocMap & m,
    const std::string & tname,
    ReportThreadAllocateInfoType type,
    uint64_t value,
    char aggregate_delimer)
{
    switch (type)
    {
    case ReportThreadAllocateInfoType::Reset:
    {
        {
            std::unique_lock l(memory_allocation_mut);
            m.insert_or_assign(tname, ThreadInfoJealloc(aggregate_delimer));
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

void JointThreadInfoJeallocMap::reportThreadAllocInfoForProxy(
    std::string_view thdname,
    ReportThreadAllocateInfoType type,
    uint64_t value)
{
    // Many threads have empty name, better just not handle.
    if (thdname.empty())
        return;
    std::string tname(thdname.begin(), thdname.end());
    reportThreadAllocInfoImpl(proxy_map, tname, type, value, '-');
    // Extra logics for Reset to set up metrics
    if (type == ReportThreadAllocateInfoType::Reset)
    {
        auto & metrics = TiFlashMetrics::instance();
        metrics.registerProxyThreadMemory(getThreadNameAggPrefix(tname, '-'));
    }
}


static const std::unordered_set<std::string> PROXY_RECORD_WHITE_LIST_THREAD_PREFIX = {"ReadIndexWkr"};

void JointThreadInfoJeallocMap::recordThreadAllocInfoForProxy()
{
    std::unordered_map<std::string, uint64_t> agg_allocate;
    std::unordered_map<std::string, uint64_t> agg_deallocate;

    {
        std::shared_lock l(memory_allocation_mut);
        for (const auto & [k, v] : proxy_map)
        {
            auto agg_thread_name = getThreadNameAggPrefix(k, '-');
            // Some thread may have shorter lifetime, we can't use this timed task here to upgrade.
            if (PROXY_RECORD_WHITE_LIST_THREAD_PREFIX.contains(agg_thread_name) && v.hasPtr())
            {
                agg_allocate[agg_thread_name] += v.allocated();
                agg_deallocate[agg_thread_name] += v.deallocated();
            }
        }
    }

    auto & tiflash_metrics = TiFlashMetrics::instance();
    for (const auto & [k, v] : agg_allocate)
    {
        tiflash_metrics.setProxyThreadMemory(TiFlashMetrics::MemoryAllocType::Alloc, k, v);
    }
    for (const auto & [k, v] : agg_deallocate)
    {
        tiflash_metrics.setProxyThreadMemory(TiFlashMetrics::MemoryAllocType::Dealloc, k, v);
    }
}

void JointThreadInfoJeallocMap::reportThreadAllocBatchForProxy(
    std::string_view name,
    ReportThreadAllocateInfoBatch data)
{
    // Many threads have empty name, better just not handle.
    if (name.empty())
        return;
    // TODO(jemalloc-trace) Could be costy.
    auto k = getThreadNameAggPrefix(name, '-');
    auto & tiflash_metrics = TiFlashMetrics::instance();
    tiflash_metrics.setProxyThreadMemory(TiFlashMetrics::MemoryAllocType::Alloc, k, data.alloc);
    tiflash_metrics.setProxyThreadMemory(TiFlashMetrics::MemoryAllocType::Dealloc, k, data.dealloc);
}

void JointThreadInfoJeallocMap::accessProxyMap(std::function<void(const AllocMap &)> f)
{
    std::shared_lock l(memory_allocation_mut);
    f(proxy_map);
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


void JointThreadInfoJeallocMap::reportThreadAllocInfoForStorage(
    const std::string & tname,
    ReportThreadAllocateInfoType type,
    uint64_t value,
    char aggregate_delimer)
{
    // Many threads have empty name, better just not handle.
    if (tname.empty())
        return;
    reportThreadAllocInfoImpl(storage_map, tname, type, value, aggregate_delimer);
    // Extra logics for Reset to set up metrics
    if (type == ReportThreadAllocateInfoType::Reset)
    {
        auto & metrics = TiFlashMetrics::instance();
        metrics.registerStorageThreadMemory(getThreadNameAggPrefix(tname, aggregate_delimer));
    }
}

void JointThreadInfoJeallocMap::recordThreadAllocInfoForStorage()
{
    std::unordered_map<std::string, uint64_t> agg_allocate;
    std::unordered_map<std::string, uint64_t> agg_deallocate;

    {
        std::shared_lock l(memory_allocation_mut);
        for (const auto & [k, v] : storage_map)
        {
            auto agg_thread_name = getThreadNameAggPrefix(k, v.aggregate_delimer);
            // Some thread may have shorter lifetime, we can't use this timed task here to upgrade.
            if (v.hasPtr())
            {
                agg_allocate[agg_thread_name] += v.allocated();
                agg_deallocate[agg_thread_name] += v.deallocated();
            }
        }
    }

    auto & tiflash_metrics = TiFlashMetrics::instance();
    for (const auto & [k, v] : agg_allocate)
    {
        tiflash_metrics.setStorageThreadMemory(TiFlashMetrics::MemoryAllocType::Alloc, k, v);
    }
    for (const auto & [k, v] : agg_deallocate)
    {
        tiflash_metrics.setStorageThreadMemory(TiFlashMetrics::MemoryAllocType::Dealloc, k, v);
    }
}

void JointThreadInfoJeallocMap::accessStorageMap(std::function<void(const AllocMap &)> f)
{
    std::shared_lock l(memory_allocation_mut);
    f(storage_map);
}

void JointThreadInfoJeallocMap::recordClassdAlloc()
{
    GET_METRIC(tiflash_memory_usage_by_class, type_uni_page_ids)
        .Set(PS::PageStorageMemorySummary::uni_page_id_bytes.load());
    GET_METRIC(tiflash_memory_usage_by_class, type_versioned_entry_or_delete)
        .Set(PS::PageStorageMemorySummary::versioned_entry_or_delete_bytes.load());
}


} // namespace DB
