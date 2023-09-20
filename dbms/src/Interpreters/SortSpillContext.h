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
class SortSpillContext final : public OperatorSpillContext
{
private:
    std::atomic<Int64> revocable_memory;
    std::atomic<AutoSpillStatus> auto_spill_status{AutoSpillStatus::NO_NEED_AUTO_SPILL};
    SpillConfig spill_config;
    SpillerPtr spiller;

public:
    SortSpillContext(const SpillConfig & spill_config_, UInt64 operator_spill_threshold_, const LoggerPtr & log);
    void buildSpiller(const Block & input_schema);
    SpillerPtr & getSpiller() { return spiller; }
    void finishOneSpill();
    bool hasSpilledData() const { return isSpilled() && spiller->hasSpilledData(); }
    bool updateRevocableMemory(Int64 new_value);
    Int64 getTotalRevocableMemoryImpl() override { return revocable_memory; };
    Int64 triggerSpillImpl(Int64 expected_released_memories) override;
    bool needFinalSpill();
    bool supportAutoTriggerSpill() const override { return true; }
};

using SortSpillContextPtr = std::shared_ptr<SortSpillContext>;
} // namespace DB
