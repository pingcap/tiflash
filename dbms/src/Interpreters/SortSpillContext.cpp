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
#include <Interpreters/SortSpillContext.h>

namespace DB
{
namespace FailPoints
{
extern const char random_marked_for_auto_spill[];
} // namespace FailPoints

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
    if (!in_spillable_stage || !isSpillEnabled())
        return false;
    revocable_memory = new_value;
    if (new_value == 0)
        return false;
    if (auto_spill_mode)
    {
        AutoSpillStatus old_value = AutoSpillStatus::NEED_AUTO_SPILL;
        if (auto_spill_status.compare_exchange_strong(old_value, AutoSpillStatus::WAIT_SPILL_FINISH))
            /// in auto spill mode, don't set revocable_memory to 0 here, so in triggerSpill it will take
            /// the revocable_memory into account if current spill is on the way
            return true;
        bool ret = false;
        fiu_do_on(FailPoints::random_marked_for_auto_spill, {
            old_value = AutoSpillStatus::NO_NEED_AUTO_SPILL;
            if (new_value > 0
                && auto_spill_status.compare_exchange_strong(old_value, AutoSpillStatus::WAIT_SPILL_FINISH))
                ret = true;
        });
        return ret;
    }
    else
    {
        if (operator_spill_threshold > 0 && revocable_memory > static_cast<Int64>(operator_spill_threshold))
        {
            revocable_memory = 0;
            return true;
        }
        return false;
    }
}

Int64 SortSpillContext::triggerSpillImpl(DB::Int64 expected_released_memories)
{
    if (revocable_memory >= MIN_SPILL_THRESHOLD)
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

bool SortSpillContext::needFinalSpill()
{
    if (auto_spill_status != AutoSpillStatus::NO_NEED_AUTO_SPILL)
        return true;
    bool ret = false;
    fiu_do_on(FailPoints::random_marked_for_auto_spill, {
        auto old_value = AutoSpillStatus::NO_NEED_AUTO_SPILL;
        if (auto_spill_status.compare_exchange_strong(old_value, AutoSpillStatus::NEED_AUTO_SPILL))
            ret = true;
    });
    return ret;
}
} // namespace DB
