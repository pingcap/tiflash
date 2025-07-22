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

#include <mutex>

namespace DB
{
Int64 CTESpillContext::triggerSpillImpl(Int64 expected_released_memories)
{
    String info = "xzxdebug CTE auto spill is triggered, spilled partitions: ";
    auto * log = &Poco::Logger::get("LRUCache");

    for (auto & partition : this->partitions)
    {
        std::lock_guard<std::mutex> aux_lock(*(partition->aux_lock));
        if (partition->status != CTEPartitionStatus::NORMAL)
            continue;

        partition->status = CTEPartitionStatus::NEED_SPILL;
        info = fmt::format("{}, {}", info, partition->partition_id);

        std::lock_guard<std::mutex> lock(*(partition->mu));
        expected_released_memories = std::max(expected_released_memories - partition->memory_usage, 0);
        if (expected_released_memories <= 0)
        {
            LOG_INFO(log, info);
            return expected_released_memories;
        }
    }
    LOG_INFO(log, info);
    return expected_released_memories;
}
} // namespace DB
