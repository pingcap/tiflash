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
    WAIT_SPILL,
    IO_IN,
    NEED_SPILL,
    END_OF_FILE,
    CANCELLED
};

struct CTEPartition
{
    explicit CTEPartition(size_t partition_id_)
        : partition_id(partition_id_)
        , mu(std::make_unique<std::mutex>())
        , pipe_cv(std::make_unique<PipeConditionVariable>())
        , aux_lock(std::make_unique<std::mutex>())
    {}

    void debugOutput()
    {
        String info_block;
        for (const auto & item : this->total_fetch_block_nums)
            info_block = fmt::format("{} <{}: {}>", info_block, item.first, item.second);

        String info_row;
        for (const auto & item : this->total_fetch_row_nums)
            info_row = fmt::format("{} <{}: {}>", info_row, item.first, item.second);

        auto * log = &Poco::Logger::get("LRUCache");
        LOG_INFO(
            log,
            fmt::format(
                "xzxdebug CTEPartition total_recv_block_num: {}, row: {}, total_spill_block_num: {}, "
                "total_fetch_block_num: {}, row num: {}, "
                "total_byte_usage: {}",
                total_recv_block_num,
                total_recv_row_num,
                total_spill_block_num,
                info_block,
                info_row,
                total_byte_usage));
    }

    void init(std::shared_ptr<CTESpillContext> spill_context_, size_t memory_threoshold_)
    {
        this->spill_context = spill_context_;
        this->memory_threoshold = memory_threoshold_;
    }

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
        return this->memory_usage > this->memory_threoshold;
    }

    CTEOpStatus pushBlock(const Block & block);
    CTEOpStatus tryGetBlock(size_t cte_reader_id, Block & block);
    CTEOpStatus spillBlocks();
    CTEOpStatus getBlockFromDisk(size_t cte_reader_id, Block & block);

    bool isBlockAvailableNoLock(size_t cte_reader_id)
    {
        if (this->isBlockAvailableInDiskNoLock(cte_reader_id))
            return true;

        return this->isBlockAvailableInMemoryNoLock(cte_reader_id);
    }

    size_t total_recv_block_num = 0;
    size_t total_recv_row_num = 0;
    size_t total_spill_block_num = 0;
    std::map<size_t, size_t> total_fetch_block_nums;
    std::map<size_t, size_t> total_fetch_row_nums;
    size_t total_byte_usage = 0;

    size_t partition_id;

    std::unique_ptr<std::mutex> mu;
    Blocks blocks;
    std::unordered_map<size_t, size_t> fetch_block_idxs;
    size_t memory_usage = 0;
    size_t memory_threoshold = 0;
    std::unique_ptr<PipeConditionVariable> pipe_cv;

    // Protecting cte_status and tmp_blocks
    std::unique_ptr<std::mutex> aux_lock;
    CTEPartitionStatus status = CTEPartitionStatus::NORMAL;
    Blocks tmp_blocks;

    std::vector<UInt64> block_in_disk_nums;
    std::unordered_map<size_t, SpillerPtr> spillers;
    std::unordered_map<size_t, BlockInputStreamPtr> cte_reader_restore_streams;
    UInt64 total_block_in_disk_num = 0;

    std::shared_ptr<CTESpillContext> spill_context;
};
} // namespace DB
