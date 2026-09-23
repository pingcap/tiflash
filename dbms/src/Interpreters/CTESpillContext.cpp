// Copyright 2025 PingCAP, Inc.
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

#include <Common/Exception.h>
#include <Interpreters/CTESpillContext.h>

#include <algorithm>
#include <mutex>
#include <utility>

namespace DB
{
Int64 CTESpillContext::triggerSpillImpl(Int64 expected_released_memories)
{
    size_t partition_num = this->partitions.size();
    std::vector<std::pair<size_t, size_t>> part_idx_with_mem_usages;
    part_idx_with_mem_usages.reserve(partition_num);
    for (size_t i = 0; i < partition_num; i++)
        part_idx_with_mem_usages.push_back(std::make_pair(this->partitions[i]->memory_usage.load(), i));

    std::sort(
        part_idx_with_mem_usages.begin(),
        part_idx_with_mem_usages.end(),
        [](const std::pair<size_t, size_t> & l, const std::pair<size_t, size_t> & r) { return l.first > r.first; });

    for (const auto & item : part_idx_with_mem_usages)
    {
        auto memory_usage = item.first;
        const std::shared_ptr<CTEPartition> & partition = partitions[item.second];
        std::lock_guard<std::mutex> aux_lock(*(partition->aux_lock));
        if (partition->status != CTEPartitionStatus::NORMAL)
            continue;

        if (memory_usage == 0)
            continue;

        partition->status = CTEPartitionStatus::NEED_SPILL;

        expected_released_memories = std::max(expected_released_memories - memory_usage, 0);
        if (expected_released_memories <= 0)
            return expected_released_memories;
    }
    return expected_released_memories;
}
} // namespace DB
