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

#include <Interpreters/CTESpillContext.h>
#include <Core/Spiller.h>
#include <Flash/Pipeline/Schedule/Tasks/PipeConditionVariable.h>

#include <cstddef>
#include <mutex>
#include <unordered_map>
#include <memory>

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
    size_t getIdxInMemoryNoLock(size_t cte_reader_id);
    bool isBlockAvailableInDiskNoLock(size_t cte_reader_id) { return this->fetch_block_idxs[cte_reader_id] < this->total_block_in_disk_num; }
    bool isBlockAvailableInMemoryNoLock(size_t cte_reader_id) { return this->getIdxInMemoryNoLock(cte_reader_id) < this->blocks.size(); }
    bool exceedMemoryThresholdNoLock() const { return this->memory_usage >= this->memory_threoshold; }
    void setCTEPartitionStatusNoLock(CTEPartitionStatus status) { this->status = status; }
    bool isSpillTriggeredNoLock() const { return this->total_block_in_disk_num > 0; }
    void addIdxNoLock(size_t cte_reader_id) { this->fetch_block_idxs[cte_reader_id]++; }

    void spillBlocks();
    void getBlockFromDisk(size_t cte_reader_id, Block & block);

    size_t partition_id; // TODO initialize it
    
    std::unique_ptr<std::mutex> mu;
    Blocks blocks;
    std::unordered_map<size_t, size_t> fetch_block_idxs;
    size_t memory_usage = 0;
    size_t memory_threoshold = 0;
    std::unique_ptr<PipeConditionVariable> pipe_cv;

    // Protecting cte_status and tmp_blocks
    std::mutex aux_lock;
    CTEPartitionStatus status;
    // TODO handle this, some blocks can not be spilled when spill is in execution, they can only be stored temporary
    Blocks tmp_blocks;

    std::vector<UInt64> block_in_disk_nums;
    std::unordered_map<size_t, SpillerPtr> spillers;
    std::unordered_map<size_t, BlockInputStreamPtr> cte_reader_restore_streams;
    UInt64 total_block_in_disk_num = 0;

    CTESpillContext * spill_context;
};
} // namespace DB
