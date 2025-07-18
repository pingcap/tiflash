// Copyright 2025 PingCAP, Inc.
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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Core/Block.h>
#include <Core/OperatorSpillContext.h>
#include <Core/SpillConfig.h>
#include <Core/Spiller.h>

#include <atomic>

namespace DB
{
enum CTEPartitionStatus
{
    NORMAL = 0,
    NEED_SPILL,
    IN_SPILLING,
};

class CTESpillContext final : public OperatorSpillContext
{
public:
    CTESpillContext(
        UInt64 operator_spill_threshold_,
        size_t partition_num_,
        const SpillConfig & spill_config_,
        const Block & spill_block_schema_,
        const String & query_id_and_cte_id_)
        : OperatorSpillContext(operator_spill_threshold_, "cte", Logger::get(query_id_and_cte_id_))
        , partition_num(partition_num_)
        , partition_memory_threoshold(operator_spill_threshold_ / partition_num_)
        , memory_usages(partition_num_)
        , spill_config(spill_config_)
        , spill_block_schema(spill_block_schema_)
        , query_id_and_cte_id(query_id_and_cte_id_)
        , aux_locks(partition_num)
        , statuses(partition_num)
        , tmp_blocks(partition_num)
    {
        for (auto & status : this->statuses)
            status = CTEPartitionStatus::NORMAL;
    }

    SpillerPtr getSpiller(size_t partition_id, size_t spill_id);
    LoggerPtr getLog() const { return this->log; }
    String getQueryIdAndCTEId() const { return this->query_id_and_cte_id; }

    bool supportAutoTriggerSpill() const override { return true; }
    Int64 triggerSpillImpl(Int64 expected_released_memories) override;

    std::mutex * getPartitionAuxMutex(size_t partition_id) { return &(this->aux_locks[partition_id]); }

    CTEPartitionStatus getPartitionStatusNoLock(size_t partition_id) const { return this->statuses[partition_id]; }
    void setPartitionStatusNoLock(size_t partition_id, CTEPartitionStatus status)
    {
        this->statuses[partition_id] = status;
    }

    void pushTmpBlock(size_t partition_id, const Block & block) { this->tmp_blocks[partition_id].push_back(block); }
    Blocks & getTmpBlocks(size_t partition_id) { return this->tmp_blocks[partition_id]; }

    void clearMemoryUsage(size_t partition_id) { this->memory_usages[partition_id].store(0); }
    void addMemoryUsage(size_t partition_id, size_t delta) { this->memory_usages[partition_id].fetch_add(delta); }
    bool exceedMemoryThreshold(size_t partition_id) const
    {
        if (this->partition_memory_threoshold == 0)
            return false;
        return this->memory_usages[partition_id].load() > this->partition_memory_threoshold;
    }

protected:
    Int64 getTotalRevocableMemoryImpl() override
    {
        Int64 total_memory = 0;
        for (const auto & memory_usage : this->memory_usages)
            total_memory += memory_usage.load();
        return total_memory;
    }

private:
    size_t partition_num;
    size_t partition_memory_threoshold;
    std::vector<std::atomic_size_t> memory_usages;

    SpillConfig spill_config;
    Block spill_block_schema;

    String query_id_and_cte_id;

    // Protecting cte_status and tmp_blocks
    std::vector<std::mutex> aux_locks;
    std::vector<CTEPartitionStatus> statuses;
    std::vector<Blocks> tmp_blocks;
};
} // namespace DB
