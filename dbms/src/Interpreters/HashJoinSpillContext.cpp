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

#include <Interpreters/HashJoinSpillContext.h>

namespace DB
{
HashJoinSpillContext::HashJoinSpillContext(const SpillConfig & build_spill_config_, const SpillConfig & probe_spill_config_, UInt64 operator_spill_threshold, const LoggerPtr & log)
    : OperatorSpillContext(operator_spill_threshold, log)
    , build_spill_config(build_spill_config_)
    , probe_spill_config(probe_spill_config_)
{}

void HashJoinSpillContext::init(size_t partition_num)
{
    partition_revocable_memories = std::make_unique<std::vector<std::atomic<Int64>>>(partition_num);
    partition_spill_status = std::make_unique<std::vector<std::atomic<SpillStatus>>>(partition_num);
    for (auto & memory : *partition_revocable_memories)
        memory = 0;
    for (auto & status : *partition_spill_status)
        status = SpillStatus::NOT_SPILL;
}

Int64 HashJoinSpillContext::getTotalRevocableMemory()
{
    Int64 ret = 0;
    for (const auto & x : *partition_revocable_memories)
    {
        auto current_value = x.load();
        if (current_value != INVALID_REVOCABLE_MEMORY)
            ret += current_value;
    }
    return ret;
}

void HashJoinSpillContext::buildBuildSpiller(const Block & input_schema)
{
    build_spiller = std::make_unique<Spiller>(build_spill_config, false, (*partition_revocable_memories).size(), input_schema, log);
}

void HashJoinSpillContext::buildProbeSpiller(const Block & input_schema)
{
    probe_spiller = std::make_unique<Spiller>(probe_spill_config, false, (*partition_revocable_memories).size(), input_schema, log);
}

void HashJoinSpillContext::markSpill()
{
    SpillStatus init_value = SpillStatus::NOT_SPILL;
    if (spill_status.compare_exchange_strong(init_value, SpillStatus::SPILL, std::memory_order_relaxed))
    {
        LOG_INFO(log, "Begin spill in join");
    }
}

void HashJoinSpillContext::clearPartitionRevocableMemory(size_t partition_num)
{
    (*partition_revocable_memories)[partition_num] = INVALID_REVOCABLE_MEMORY;
}

bool HashJoinSpillContext::updatePartitionRevocableMemory(Int64 new_value, size_t partition_num)
{
    assert(new_value > INVALID_REVOCABLE_MEMORY);
    if ((*partition_revocable_memories)[partition_num] == INVALID_REVOCABLE_MEMORY)
        return false;
    (*partition_revocable_memories)[partition_num] = new_value;
    return false;
}
} // namespace DB
