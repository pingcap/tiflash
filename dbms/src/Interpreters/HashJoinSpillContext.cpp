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

#include <Common/FailPoint.h>
#include <Interpreters/HashJoinSpillContext.h>

namespace DB
{
namespace FailPoints
{
extern const char random_marked_for_auto_spill[];
} // namespace FailPoints

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
{}

void HashJoinSpillContext::init(size_t partition_num)
{
    partition_revocable_memories = std::make_unique<std::vector<std::atomic<Int64>>>(partition_num);
    partition_is_spilled = std::make_unique<std::vector<std::atomic<bool>>>(partition_num);
    partition_spill_status = std::make_unique<std::vector<std::atomic<AutoSpillStatus>>>(partition_num);
    for (auto & memory : *partition_revocable_memories)
        memory = 0;
    for (auto & is_spilled : *partition_is_spilled)
        is_spilled = false;
    for (auto & spill_status : *partition_spill_status)
        spill_status = AutoSpillStatus::NO_NEED_AUTO_SPILL;
}

Int64 HashJoinSpillContext::getTotalRevocableMemoryImpl()
{
    Int64 ret = 0;
    for (size_t part_index = 0; part_index < partition_revocable_memories->size(); ++part_index)
    {
        if (in_build_stage || isPartitionSpilled(part_index))
            ret += (*partition_revocable_memories)[part_index];
    }
    return ret;
}

bool HashJoinSpillContext::supportFurtherSpill() const
{
    return in_spillable_stage && (in_build_stage || isSpilled());
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

void HashJoinSpillContext::finishBuild()
{
    LOG_INFO(log, "Hash join finish build stage");
    in_build_stage = false;
}

size_t HashJoinSpillContext::spilledPartitionCount()
{
    size_t ret = 0;
    for (auto & is_spilled : (*partition_is_spilled))
        if (is_spilled)
            ++ret;
    return ret;
}

bool HashJoinSpillContext::markPartitionForAutoSpill(size_t partition_id)
{
    auto old_value = AutoSpillStatus::NO_NEED_AUTO_SPILL;
    if (in_build_stage && isSpillEnabled())
    {
        return (*partition_spill_status)[partition_id].compare_exchange_strong(
            old_value,
            AutoSpillStatus::NEED_AUTO_SPILL);
    }
    if (!in_build_stage && in_spillable_stage && isSpillEnabled() && isPartitionSpilled(partition_id))
    {
        return (*partition_spill_status)[partition_id].compare_exchange_strong(
            old_value,
            AutoSpillStatus::NEED_AUTO_SPILL);
    }
    return false;
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
    if (!in_spillable_stage || !isSpillEnabled())
        return false;
    bool is_spilled = (*partition_is_spilled)[partition_id];
    (*partition_revocable_memories)[partition_id] = new_value;
    if (new_value == 0)
        return false;
    if (operator_spill_threshold > 0)
    {
        auto force_spill = is_spilled && operator_spill_threshold > 0
            && getTotalRevocableMemoryImpl() > static_cast<Int64>(operator_spill_threshold);
        return (
            force_spill
            || (is_spilled && max_cached_bytes > 0
                && (*partition_revocable_memories)[partition_id] > max_cached_bytes));
    }
    else
    {
        /// auto spill
        if ((*partition_spill_status)[partition_id] == AutoSpillStatus::NEED_AUTO_SPILL)
        {
            AutoSpillStatus old_value = AutoSpillStatus::NEED_AUTO_SPILL;
            return (*partition_spill_status)[partition_id].compare_exchange_strong(
                old_value,
                AutoSpillStatus::WAIT_SPILL_FINISH);
        }
        else if (
            is_spilled && (*partition_spill_status)[partition_id] == AutoSpillStatus::NO_NEED_AUTO_SPILL
            && max_cached_bytes > 0 && (*partition_revocable_memories)[partition_id] > max_cached_bytes)
        {
            AutoSpillStatus old_value = AutoSpillStatus::NO_NEED_AUTO_SPILL;
            return (*partition_spill_status)[partition_id].compare_exchange_strong(
                old_value,
                AutoSpillStatus::WAIT_SPILL_FINISH);
        }
        bool ret = false;
        fiu_do_on(FailPoints::random_marked_for_auto_spill, {
            if (in_build_stage || is_spilled)
            {
                AutoSpillStatus old_value = AutoSpillStatus::NO_NEED_AUTO_SPILL;
                if (new_value > 0
                    && (*partition_spill_status)[partition_id].compare_exchange_strong(
                        old_value,
                        AutoSpillStatus::WAIT_SPILL_FINISH))
                    ret = true;
            }
        });
        return ret;
    }
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

/// only used in operator level memory threshold
std::vector<size_t> HashJoinSpillContext::getPartitionsToSpill()
{
    std::vector<size_t> ret;
    if (!in_spillable_stage || !in_build_stage || !isSpillEnabled())
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

Int64 HashJoinSpillContext::triggerSpillImpl(Int64 expected_released_memories)
{
    std::vector<std::pair<size_t, std::pair<bool, Int64>>> partition_index_to_revocable_memories(
        partition_revocable_memories->size());
    for (size_t i = 0; i < (*partition_revocable_memories).size(); i++)
        partition_index_to_revocable_memories[i]
            = std::make_pair(i, std::make_pair(isPartitionSpilled(i), (*partition_revocable_memories)[i].load()));
    std::sort(
        partition_index_to_revocable_memories.begin(),
        partition_index_to_revocable_memories.end(),
        [](const auto & a, const auto & b) {
            if (a.second.first != b.second.first)
            {
                /// spilled partition has highest priority
                return a.second.first;
            }
            return a.second.second > b.second.second;
        });
    for (const auto & pair : partition_index_to_revocable_memories)
    {
        if (pair.second.second < MIN_SPILL_THRESHOLD)
            continue;
        if (!in_build_stage && !isPartitionSpilled(pair.first))
            /// no new partition spill is allowed if not in build stage
            continue;
        AutoSpillStatus old_value = AutoSpillStatus::NO_NEED_AUTO_SPILL;
        /// mark for spill
        if ((*partition_spill_status)[pair.first].compare_exchange_strong(old_value, AutoSpillStatus::NEED_AUTO_SPILL))
            LOG_DEBUG(log, "mark partition {} to spill for {}", pair.first, op_name);
        expected_released_memories
            = std::max(expected_released_memories - (*partition_revocable_memories)[pair.first], 0);
        if (expected_released_memories <= 0)
            return expected_released_memories;
    }
    return expected_released_memories;
}

void HashJoinSpillContext::finishOneSpill(size_t partition_id)
{
    LOG_DEBUG(log, "partition {} finish one spill for {}", partition_id, op_name);
    (*partition_spill_status)[partition_id] = AutoSpillStatus::NO_NEED_AUTO_SPILL;
    (*partition_revocable_memories)[partition_id] = 0;
}

} // namespace DB
