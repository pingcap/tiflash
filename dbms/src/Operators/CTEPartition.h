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

#include <Core/Spiller.h>
#include <Flash/Pipeline/Schedule/Tasks/PipeConditionVariable.h>
#include <Interpreters/CTESpillContext.h>

#include <cstddef>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB
{
enum CTEPartitionStatus
{
    NORMAL = 0,
    NEED_SPILL,
    IN_SPILLING,
};

enum class CTEOpStatus
{
    OK,
    BLOCK_NOT_AVAILABLE, // It means that we do not have specified block so far
    IO_OUT,
    IO_IN,
    END_OF_FILE,
    CANCELLED
};

struct CTEPartition
{
    explicit CTEPartition(size_t partition_id_)
        : partition_id(partition_id_)
        , mu(std::make_unique<std::mutex>())
        , pipe_cv(std::make_unique<PipeConditionVariable>())
        , status_lock(std::make_unique<std::mutex>())
    {}

    void init(std::shared_ptr<CTESpillContext> spill_context_) { this->spill_context = spill_context_; }

    size_t getIdxInMemoryNoLock(size_t cte_reader_id);
    bool isBlockAvailableInDiskNoLock(size_t cte_reader_id)
    {
        return this->fetch_block_idxs[cte_reader_id] < this->total_block_in_disk_num;
    }
    bool isBlockAvailableInMemoryNoLock(size_t cte_reader_id)
    {
        return this->getIdxInMemoryNoLock(cte_reader_id) < this->blocks.size();
    }
    void setCTEPartitionStatusNoLock(CTEPartitionStatus status) { this->status = status; }
    bool isSpillTriggeredNoLock() const { return this->total_block_in_disk_num > 0; }
    void addIdxNoLock(size_t cte_reader_id) { this->fetch_block_idxs[cte_reader_id]++; }
    bool exceedMemoryThresholdNoLock() const
    {
        if (this->memory_threoshold == 0)
            return false;
        return this->memory_usage >= this->memory_threoshold;
    }

    CTEOpStatus pushBlock(const Block & block);
    CTEOpStatus tryGetBlockAt(size_t cte_reader_id, Block & block);
    CTEOpStatus spillBlocks();
    CTEOpStatus getBlockFromDisk(size_t cte_reader_id, Block & block);

    bool isBlockAvailableNoLock(size_t cte_reader_id)
    {
        if (this->isBlockAvailableInDiskNoLock(cte_reader_id))
            return true;

        return this->isBlockAvailableInMemoryNoLock(cte_reader_id);
    }

    size_t partition_id;

    std::unique_ptr<std::mutex> mu;
    Blocks blocks;
    std::unordered_map<size_t, size_t> fetch_block_idxs;
    size_t memory_usage = 0;
    size_t memory_threoshold = 0; // TODO initialize it
    std::unique_ptr<PipeConditionVariable> pipe_cv;
    Blocks tmp_blocks;

    // Protecting cte_status
    std::unique_ptr<std::mutex> status_lock;
    CTEPartitionStatus status = CTEPartitionStatus::NORMAL;

    std::vector<UInt64> block_in_disk_nums;
    std::unordered_map<size_t, SpillerSharedPtr> spillers;
    std::unordered_map<size_t, BlockInputStreamPtr> cte_reader_restore_streams;
    UInt64 total_block_in_disk_num = 0;

    std::shared_ptr<CTESpillContext> spill_context;
};
} // namespace DB
