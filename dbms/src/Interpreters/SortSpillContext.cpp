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

#include <Interpreters/SortSpillContext.h>

namespace DB
{
SortSpillContext::SortSpillContext(
    const SpillConfig & spill_config_,
    UInt64 operator_spill_threshold_,
    const LoggerPtr & log)
    : OperatorSpillContext(operator_spill_threshold_, "sort", log)
    , spill_config(spill_config_)
{}

void SortSpillContext::buildSpiller(const Block & input_schema)
{
    spiller = std::make_unique<Spiller>(spill_config, true, 1, input_schema, log);
}

bool SortSpillContext::updateRevocableMemory(Int64 new_value)
{
    if (!supportFurtherSpill() || !enable_spill)
        return false;
    revocable_memory = new_value;
    if (auto_spill_mode)
    {
        AutoSpillStatus old_value = AutoSpillStatus::NEED_AUTO_SPILL;
        if (auto_spill_status.compare_exchange_strong(old_value, AutoSpillStatus::WAIT_SPILL_FINISH))
            /// in auto spill mode, don't set revocable_memory to 0 here, so in triggerSpill it will take
            /// the revocable_memory into account if current spill is on the way
            return true;
    }
    else
    {
        if (operator_spill_threshold > 0 && revocable_memory > static_cast<Int64>(operator_spill_threshold))
        {
            revocable_memory = 0;
            return true;
        }
    }
    return false;
}

Int64 SortSpillContext::triggerSpill(Int64 expected_released_memories)
{
    if unlikely(expected_released_memories <= 0)
        return expected_released_memories;
    if (!supportFurtherSpill() || !enable_spill)
        return expected_released_memories;
    RUNTIME_CHECK_MSG(operator_spill_threshold == 0, "The operator spill threshold should be 0 in auto spill mode");
    auto total_revocable_memory = getTotalRevocableMemory();
    if (total_revocable_memory >= MIN_SPILL_THRESHOLD)
    {
        AutoSpillStatus old_value = AutoSpillStatus::NO_NEED_AUTO_SPILL;
        auto_spill_status.compare_exchange_strong(old_value, AutoSpillStatus::NEED_AUTO_SPILL);
        expected_released_memories = std::max(expected_released_memories - revocable_memory, 0);
    }
    return expected_released_memories;
}

void SortSpillContext::finishOneSpill()
{
    auto_spill_status = AutoSpillStatus::NO_NEED_AUTO_SPILL;
    revocable_memory = 0;
}
} // namespace DB
