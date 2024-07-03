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

#include <Interpreters/JoinV2/HashJoinBuild.h>

namespace DB
{

class HashJoinPointerTable
{
public:
    HashJoinPointerTable() = default;
    ~HashJoinPointerTable()
    {
        if (pointer_table != nullptr)
            alloc.free(reinterpret_cast<void *>(pointer_table), pointer_table_size * sizeof(std::atomic<RowPtr>));
    }

    DISALLOW_COPY_AND_MOVE(HashJoinPointerTable);

    static size_t pointerTableCapacity(size_t count) { return std::max(roundUpToPowerOfTwoOrZero(count * 2), 1 << 10); }

    void init(size_t row_count, size_t probe_prefetch_threshold);

    template <typename HashValueType>
    bool build(
        const HashJoinRowLayout & row_layout,
        JoinBuildWorkerData & worker_data,
        std::vector<std::unique_ptr<MultipleRowContainer>> & multi_row_containers,
        size_t max_build_size);

    size_t getBucketNum(size_t hash) const
    {
        return (hash & pointer_table_size_mask) >> (32 - pointer_table_size_degree);
    }

    size_t getPointerTableSize() const { return pointer_table_size; }

    bool enableProbePrefetch() const { return enable_probe_prefetch; }

    RowPtr getHeadPointer(size_t hash) const
    {
        return pointer_table[getBucketNum(hash)].load(std::memory_order_relaxed);
    }

private:
    size_t pointer_table_size = 0;
    size_t pointer_table_size_degree = 0;
    size_t pointer_table_size_mask = 0;
    std::atomic<RowPtr> * pointer_table = nullptr;
    Allocator<true> alloc;
    bool enable_probe_prefetch = false;

    std::mutex build_scan_table_lock;
    size_t build_table_index = 0;
};


} // namespace DB
