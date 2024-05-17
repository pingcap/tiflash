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

#pragma once

#include <shared_mutex>
#include <unordered_map>

namespace DB
{

namespace tests
{
class RegionKVStoreTest;
} // namespace tests

enum class ReportThreadAllocateInfoType : uint64_t;
struct ReportThreadAllocateInfoBatch;

struct ThreadInfoJealloc
{
    uint64_t allocated_ptr{0};
    uint64_t deallocated_ptr{0};

    bool has_ptr() const { return allocated_ptr != 0 && deallocated_ptr != 0; }

    uint64_t allocated() const
    {
        if (allocated_ptr == 0)
            return 0;
        return *reinterpret_cast<uint64_t *>(allocated_ptr);
    }
    uint64_t deallocated() const
    {
        if (deallocated_ptr == 0)
            return 0;
        return *reinterpret_cast<uint64_t *>(deallocated_ptr);
    }
    int64_t remaining() const
    {
        uint64_t a = allocated();
        uint64_t d = deallocated();
        if (a > d)
        {
            return static_cast<int64_t>(a - d);
        }
        else
        {
            return -static_cast<int64_t>(d - a);
        }
    }
};

/// Works in two different ways:
/// NOTE in both ways, call reportThreadAllocInfo to register by `Reset` for every thread to be monitored.
/// And call reportThreadAllocInfo to deregister by `Remove` for every thread that is guaranteed to no longer be monitored.
/// - Register by reportThreadAllocInfo with AllocPtr/DellocPtr
///   In this way, by recordThreadAllocInfo the routine thread will update the allocation info.
///   One must guarantee that the thread must exist before `Remove`.
/// - Directly report by reportThreadAllocBatch
///   The call will immedialy update the alloc info of the specified thread.
class JointThreadInfoJeallocMap
{
public:
    JointThreadInfoJeallocMap();
    ~JointThreadInfoJeallocMap();
    // Stop the periodic c
    void stopThreadAllocInfo();

    /// For those everlasting threads, we can directly access their allocatedp/allocatedp.
    void reportThreadAllocInfoForKVStore(std::string_view, ReportThreadAllocateInfoType type, uint64_t value);
    /// For those threads with shorter life, we can only update in their call chain.
    /// Note that this function rely on `TiFlashMetrics::instance` is alive
    static void reportThreadAllocBatchForKVStore(std::string_view, ReportThreadAllocateInfoBatch data);


    friend class tests::RegionKVStoreTest;

private:
    /// Be called periodicly to submit the alloc info to TiFlashMetrics
    /// Note that this function rely on `TiFlashMetrics::instance` is alive
    void recordThreadAllocInfo();
    void recordThreadAllocInfoForKVStore();

    /// Note that this function rely on `TiFlashMetrics::instance` is alive
    void reportThreadAllocInfoImpl(
        std::unordered_map<std::string, ThreadInfoJealloc> &,
        std::string_view,
        ReportThreadAllocateInfoType type,
        uint64_t value);

private:
    mutable std::shared_mutex memory_allocation_mut;
    std::unordered_map<std::string, ThreadInfoJealloc> kvstore_map;

    bool is_terminated{false};
    mutable std::mutex monitoring_mut;
    std::condition_variable monitoring_cv;
    std::thread * monitoring_thread{nullptr};
};

using JointThreadInfoJeallocMapPtr = std::shared_ptr<JointThreadInfoJeallocMap>;

} // namespace DB
