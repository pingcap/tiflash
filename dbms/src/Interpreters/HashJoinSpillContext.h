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

#pragma once

#include <Core/OperatorSpillContext.h>
#include <Core/Spiller.h>


namespace DB
{
class HashJoinSpillContext : public OperatorSpillContext
{
private:
    std::atomic<SpillStatus> spill_status{SpillStatus::NOT_SPILL};
    std::unique_ptr<std::vector<std::atomic<SpillStatus>>> partition_spill_status;
    std::unique_ptr<std::vector<std::atomic<Int64>>> partition_revocable_memories;
    SpillConfig build_spill_config;
    SpillerPtr build_spiller;
    SpillConfig probe_spill_config;
    SpillerPtr probe_spiller;

public:
    HashJoinSpillContext(const SpillConfig & build_spill_config_, const SpillConfig & probe_spill_config_, UInt64 operator_spill_threshold_, const LoggerPtr & log);
    void init(size_t partition_num);
    void buildBuildSpiller(const Block & input_schema);
    void buildProbeSpiller(const Block & input_schema);
    SpillerPtr & getBuildSpiller() { return build_spiller; }
    SpillerPtr & getProbeSpiller() { return probe_spiller; }
    bool isSpilled() const { return spill_status != SpillStatus::NOT_SPILL; }
    void markSpill();
    bool updatePartitionRevocableMemory(Int64 new_value, size_t partition_num);
    void clearPartitionRevocableMemory(size_t partition_num);
    Int64 getTotalRevocableMemory() override;
};

using HashJoinSpillContextPtr = std::shared_ptr<HashJoinSpillContext>;
} // namespace DB
