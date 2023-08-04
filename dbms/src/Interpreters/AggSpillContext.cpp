// Copyright 2023 PingCAP, Ltd.
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

#include <Common/ThresholdUtils.h>
#include <Interpreters/AggSpillContext.h>

namespace DB
{
AggSpillContext::AggSpillContext(size_t concurrency, const SpillConfig & spill_config_, UInt64 operator_spill_threshold_, const LoggerPtr & log)
    : OperatorSpillContext(operator_spill_threshold_, "aggregator", log)
    , per_thread_revocable_memories(concurrency)
    , spill_config(spill_config_)
{
    per_thread_spill_threshold = getAverageThreshold(operator_spill_threshold, concurrency);
    for (auto & memory : per_thread_revocable_memories)
        memory = 0;
}

void AggSpillContext::buildSpiller(const Block & input_schema)
{
    /// for aggregation, the input block is sorted by bucket number
    /// so it can work with MergingAggregatedMemoryEfficientBlockInputStream
    spiller = std::make_unique<Spiller>(spill_config, true, 1, input_schema, log);
}

bool AggSpillContext::updatePerThreadRevocableMemory(Int64 new_value, size_t thread_num)
{
    if (!in_spillable_stage)
        return false;
    per_thread_revocable_memories[thread_num] = new_value;
    if (enable_spill && per_thread_spill_threshold > 0 && new_value > static_cast<Int64>(per_thread_spill_threshold))
    {
        per_thread_revocable_memories[thread_num] = 0;
        return true;
    }
    return false;
}

Int64 AggSpillContext::getTotalRevocableMemoryImpl()
{
    Int64 ret = 0;
    for (const auto & x : per_thread_revocable_memories)
        ret += x;
    return ret;
}
} // namespace DB
