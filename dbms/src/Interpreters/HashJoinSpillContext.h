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
class HashJoinSpillContext final : public OperatorSpillContext
{
private:
    std::unique_ptr<std::vector<std::atomic<bool>>> partition_is_spilled;
    std::unique_ptr<std::vector<std::atomic<Int64>>> partition_revocable_memories;
    std::unique_ptr<std::vector<std::atomic<AutoSpillStatus>>> partition_spill_status;
    SpillConfig build_spill_config;
    SpillerPtr build_spiller;
    SpillConfig probe_spill_config;
    SpillerPtr probe_spiller;
    Int64 max_cached_bytes;
    std::atomic<bool> in_build_stage{true};

public:
    HashJoinSpillContext(
        const SpillConfig & build_spill_config_,
        const SpillConfig & probe_spill_config_,
        UInt64 operator_spill_threshold_,
        const LoggerPtr & log);
    void init(size_t partition_num);
    void buildBuildSpiller(const Block & input_schema);
    void buildProbeSpiller(const Block & input_schema);
    SpillerPtr & getBuildSpiller() { return build_spiller; }
    SpillerPtr & getProbeSpiller() { return probe_spiller; }
    bool isPartitionSpilled(size_t partition_index) const { return (*partition_is_spilled)[partition_index]; }
    void markPartitionSpilled(size_t partition_index);
    bool updatePartitionRevocableMemory(size_t partition_id, Int64 new_value);
    Int64 getTotalRevocableMemoryImpl() override;
    bool supportFurtherSpill() const override;
    SpillConfig createBuildSpillConfig(const String & spill_id) const;
    SpillConfig createProbeSpillConfig(const String & spill_id) const;
    std::vector<size_t> getPartitionsToSpill();
    Int64 triggerSpillImpl(Int64 expected_released_memories) override;
    bool supportAutoTriggerSpill() const override { return true; }
    void finishOneSpill(size_t partition_id);
    bool isPartitionMarkedForAutoSpill(size_t partition_id) const
    {
        return (*partition_spill_status)[partition_id] != AutoSpillStatus::NO_NEED_AUTO_SPILL;
    }
    /// only used in random failpoint
    bool markPartitionForAutoSpill(size_t partition_id);
    void finishBuild();
    size_t spilledPartitionCount();
};

using HashJoinSpillContextPtr = std::shared_ptr<HashJoinSpillContext>;
} // namespace DB
