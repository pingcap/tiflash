// Copyright 2023 PingCAP, Inc.
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
HashJoinSpillContext::HashJoinSpillContext(
    const SpillConfig & build_spill_config_,
    const SpillConfig & probe_spill_config_,
    UInt64 operator_spill_threshold,
    const LoggerPtr & log)
    : OperatorSpillContext(operator_spill_threshold, "join", log)
    , build_spill_config(build_spill_config_)
    , probe_spill_config(probe_spill_config_)
    , max_cached_bytes(std::max(
          build_spill_config.max_cached_data_bytes_in_spiller,
          probe_spill_config.max_cached_data_bytes_in_spiller))
{
    /// join does not support auto spill mode
    auto_spill_mode = false;
}

void HashJoinSpillContext::init(size_t partition_num)
{
    partition_revocable_memories = std::make_unique<std::vector<std::atomic<Int64>>>(partition_num);
    partition_is_spilled = std::make_unique<std::vector<std::atomic<bool>>>(partition_num);
    for (auto & memory : *partition_revocable_memories)
        memory = 0;
    for (auto & status : *partition_is_spilled)
        status = false;
}

Int64 HashJoinSpillContext::getTotalRevocableMemoryImpl()
{
    Int64 ret = 0;
    for (const auto & x : *partition_revocable_memories)
        ret += x;
    return ret;
}

void HashJoinSpillContext::buildBuildSpiller(const Block & input_schema)
{
    build_spiller = std::make_unique<Spiller>(
        build_spill_config,
        false,
        (*partition_revocable_memories).size(),
        input_schema,
        log);
}

void HashJoinSpillContext::buildProbeSpiller(const Block & input_schema)
{
    probe_spiller = std::make_unique<Spiller>(
        probe_spill_config,
        false,
        (*partition_revocable_memories).size(),
        input_schema,
        log);
}

void HashJoinSpillContext::markPartitionSpilled(size_t partition_index)
{
    markSpilled();
    (*partition_is_spilled)[partition_index] = true;
}

bool HashJoinSpillContext::updatePartitionRevocableMemory(size_t partition_id, Int64 new_value)
{
    (*partition_revocable_memories)[partition_id] = new_value;
    /// this function only trigger spill if current partition is already chosen to spill
    /// the new partition to spill is chosen in getPartitionsToSpill
    if (!(*partition_is_spilled)[partition_id])
        return false;
    auto force_spill
        = operator_spill_threshold > 0 && getTotalRevocableMemoryImpl() > static_cast<Int64>(operator_spill_threshold);
    if (force_spill || (max_cached_bytes > 0 && (*partition_revocable_memories)[partition_id] > max_cached_bytes))
    {
        (*partition_revocable_memories)[partition_id] = 0;
        return true;
    }
    return false;
}

SpillConfig HashJoinSpillContext::createBuildSpillConfig(const String & spill_id) const
{
    return SpillConfig(
        build_spill_config.spill_dir,
        spill_id,
        build_spill_config.max_cached_data_bytes_in_spiller,
        build_spill_config.max_spilled_rows_per_file,
        build_spill_config.max_spilled_bytes_per_file,
        build_spill_config.file_provider);
}
SpillConfig HashJoinSpillContext::createProbeSpillConfig(const String & spill_id) const
{
    return SpillConfig(
        probe_spill_config.spill_dir,
        spill_id,
        build_spill_config.max_cached_data_bytes_in_spiller,
        build_spill_config.max_spilled_rows_per_file,
        build_spill_config.max_spilled_bytes_per_file,
        build_spill_config.file_provider);
}

std::vector<size_t> HashJoinSpillContext::getPartitionsToSpill()
{
    std::vector<size_t> ret;
    if (!in_spillable_stage || !isSpillEnabled())
        return ret;
    Int64 target_partition_index = -1;
    if (operator_spill_threshold <= 0 || getTotalRevocableMemoryImpl() <= static_cast<Int64>(operator_spill_threshold))
    {
        return ret;
    }
    Int64 max_bytes = 0;
    for (size_t j = 0; j < partition_revocable_memories->size(); ++j)
    {
        if (!isPartitionSpilled(j) && (target_partition_index == -1 || (*partition_revocable_memories)[j] > max_bytes))
        {
            target_partition_index = j;
            max_bytes = (*partition_revocable_memories)[j];
        }
    }
    if (target_partition_index == -1)
    {
        return ret;
    }
    ret.push_back(target_partition_index);
    (*partition_revocable_memories)[target_partition_index] = 0;
    // todo return more partitions so more memory can be released
    return ret;
}

Int64 HashJoinSpillContext::triggerSpill(Int64)
{
    throw Exception("Not supported yet");
}
} // namespace DB
