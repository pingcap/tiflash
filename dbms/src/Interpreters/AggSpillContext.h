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

#pragma once

#include <Core/OperatorSpillContext.h>
#include <Core/Spiller.h>


namespace DB
{
class AggSpillContext final : public OperatorSpillContext
{
private:
    std::vector<std::atomic<Int64>> per_thread_revocable_memories;
    std::vector<std::atomic<AutoSpillStatus>> per_thread_auto_spill_status;
    SpillConfig spill_config;
    SpillerPtr spiller;
    UInt64 per_thread_spill_threshold;

public:
    AggSpillContext(
        size_t concurrency,
        const SpillConfig & spill_config_,
        UInt64 operator_spill_threshold_,
        const LoggerPtr & log);
    void buildSpiller(const Block & input_schema);
    SpillerPtr & getSpiller() { return spiller; }
    bool hasSpilledData() const { return isSpilled() && spiller->hasSpilledData(); }
    bool updatePerThreadRevocableMemory(Int64 new_value, size_t thread_num);
    Int64 getTotalRevocableMemoryImpl() override;
    Int64 triggerSpillImpl(Int64 expected_released_memories) override;
    bool supportAutoTriggerSpill() const override { return true; }
    void finishOneSpill(size_t thread_num);
    bool isThreadMarkedForAutoSpill(size_t thread_num) const
    {
        return per_thread_auto_spill_status[thread_num] != AutoSpillStatus::NO_NEED_AUTO_SPILL;
    }
    /// only used in random failpoint
    bool markThreadForAutoSpill(size_t thread_num);
};

using AggSpillContextPtr = std::shared_ptr<AggSpillContext>;
} // namespace DB
