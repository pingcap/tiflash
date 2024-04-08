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
#include <Common/ThresholdUtils.h>
#include <Interpreters/AggSpillContext.h>

namespace DB
{
namespace FailPoints
{
extern const char random_marked_for_auto_spill[];
} // namespace FailPoints

AggSpillContext::AggSpillContext(
    size_t concurrency,
    const SpillConfig & spill_config_,
    UInt64 operator_spill_threshold_,
    const LoggerPtr & log)
    : OperatorSpillContext(operator_spill_threshold_, "aggregator", log)
    , per_thread_revocable_memories(concurrency)
    , per_thread_auto_spill_status(concurrency)
    , spill_config(spill_config_)
{
    per_thread_spill_threshold = getAverageThreshold(operator_spill_threshold, concurrency);
    for (auto & memory : per_thread_revocable_memories)
        memory = 0;
    for (auto & status : per_thread_auto_spill_status)
        status = AutoSpillStatus::NO_NEED_AUTO_SPILL;
}

void AggSpillContext::buildSpiller(const Block & input_schema)
{
    /// for aggregation, the input block is sorted by bucket number
    /// so it can work with MergingAggregatedMemoryEfficientBlockInputStream
    spiller = std::make_unique<Spiller>(spill_config, true, 1, input_schema, log);
}

bool AggSpillContext::updatePerThreadRevocableMemory(Int64 new_value, size_t thread_num)
{
    if (!in_spillable_stage || !isSpillEnabled())
        return false;
    per_thread_revocable_memories[thread_num] = new_value;
    if (new_value == 0)
        // new_value == 0 means no agg data to spill
        return false;
    if (auto_spill_mode)
    {
        AutoSpillStatus old_value = AutoSpillStatus::NEED_AUTO_SPILL;
        if (per_thread_auto_spill_status[thread_num].compare_exchange_strong(
                old_value,
                AutoSpillStatus::WAIT_SPILL_FINISH))
            /// in auto spill mode, don't set revocable_memory to 0 here, so in triggerSpill it will take
            /// the revocable_memory into account if current spill is on the way
            return true;
        bool ret = false;
        fiu_do_on(FailPoints::random_marked_for_auto_spill, {
            old_value = AutoSpillStatus::NO_NEED_AUTO_SPILL;
            if (new_value > 0
                && per_thread_auto_spill_status[thread_num].compare_exchange_strong(
                    old_value,
                    AutoSpillStatus::WAIT_SPILL_FINISH))
                ret = true;
        });
        return ret;
    }
    else
    {
        if (per_thread_spill_threshold > 0 && new_value > static_cast<Int64>(per_thread_spill_threshold))
        {
            per_thread_revocable_memories[thread_num] = 0;
            return true;
        }
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

Int64 AggSpillContext::triggerSpillImpl(Int64 expected_released_memories)
{
    size_t checked_thread = 0;
    for (; checked_thread < per_thread_revocable_memories.size(); ++checked_thread)
    {
        AutoSpillStatus old_value = AutoSpillStatus::NO_NEED_AUTO_SPILL;
        if (per_thread_revocable_memories[checked_thread] == 0)
            // meaningless to trigger spill if there is no data
            continue;
        if (per_thread_auto_spill_status[checked_thread].compare_exchange_strong(
                old_value,
                AutoSpillStatus::NEED_AUTO_SPILL))
        {
            LOG_DEBUG(
                log,
                "Mark thread {} to spill, expect to release {} bytes",
                checked_thread,
                per_thread_revocable_memories[checked_thread]);
        }
        expected_released_memories
            = std::max(expected_released_memories - per_thread_revocable_memories[checked_thread], 0);
        if (expected_released_memories == 0)
            break;
    }
    if (spill_config.max_cached_data_bytes_in_spiller > 0)
    {
        auto spill_threshold = static_cast<Int64>(spill_config.max_cached_data_bytes_in_spiller);
        for (size_t i = checked_thread + 1; i < per_thread_revocable_memories.size(); ++i)
        {
            /// unlike sort and hash join, the implementation of current agg spill does not support partial spill, that is to say,
            /// once agg spill is triggered, all the data will be spilled in the end, so here to spill the data if memory usage is large enough
            if (per_thread_revocable_memories[i] >= spill_threshold)
            {
                AutoSpillStatus old_value = AutoSpillStatus::NO_NEED_AUTO_SPILL;
                if (per_thread_auto_spill_status[i].compare_exchange_strong(
                        old_value,
                        AutoSpillStatus::NEED_AUTO_SPILL))
                {
                    LOG_DEBUG(
                        log,
                        "Mark thread {} to spill, expect to release {} bytes",
                        i,
                        per_thread_revocable_memories[i]);
                }
            }
        }
    }
    return expected_released_memories;
}

void AggSpillContext::finishOneSpill(size_t thread_num)
{
    per_thread_auto_spill_status[thread_num] = AutoSpillStatus::NO_NEED_AUTO_SPILL;
    per_thread_revocable_memories[thread_num] = 0;
}

bool AggSpillContext::markThreadForAutoSpill(size_t thread_num)
{
    if (in_spillable_stage && isSpillEnabled() && per_thread_revocable_memories[thread_num] > 0)
    {
        auto old_value = AutoSpillStatus::NO_NEED_AUTO_SPILL;
        return per_thread_auto_spill_status[thread_num].compare_exchange_strong(
            old_value,
            AutoSpillStatus::NEED_AUTO_SPILL);
    }
    return false;
}
} // namespace DB
