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
#include <Core/SpillConfig.h>
#include <Core/Spiller.h>
#include <Flash/Pipeline/Schedule/Tasks/PipeConditionVariable.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>

#include <atomic>
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
    CANCELLED,
    SINK_NOT_REGISTERED
};

struct CTEPartitionSharedConfig
{
    CTEPartitionSharedConfig(
        size_t memory_threshold_,
        SpillConfig spill_config_,
        Block spill_block_schema_,
        String query_id_and_cte_id_,
        LoggerPtr log_,
        size_t partition_num_)
        : memory_threshold(memory_threshold_)
        , spill_config(spill_config_)
        , spill_block_schema(spill_block_schema_)
        , query_id_and_cte_id(query_id_and_cte_id_)
        , log(log_)
        , partition_num(partition_num_)
    {}

    SpillerPtr getSpiller(size_t partition_id, size_t spill_id)
    {
        SpillConfig config(
            this->spill_config.spill_dir,
            fmt::format("cte_spill_{}_{}", partition_id, spill_id),
            this->spill_config.max_cached_data_bytes_in_spiller,
            this->spill_config.max_spilled_rows_per_file,
            this->spill_config.max_spilled_bytes_per_file,
            this->spill_config.file_provider,
            this->spill_config.for_all_constant_max_streams,
            this->spill_config.for_all_constant_block_size);

        return std::make_unique<Spiller>(
            config,
            false,
            this->partition_num,
            this->spill_block_schema,
            this->log,
            1,
            false);
    }

    size_t memory_threshold;
    SpillConfig spill_config;
    Block spill_block_schema;
    String query_id_and_cte_id;
    LoggerPtr log;
    size_t partition_num;
};

struct BlockWithCounter
{
    BlockWithCounter(const Block & block_, Int16 counter_)
        : block(block_)
        , counter(counter_)
    {}
    Block block;
    Int16 counter;
};

struct CTEPartition
{
    CTEPartition(size_t partition_id_, size_t expected_source_num_)
        : partition_id(partition_id_)
        , aux_lock(std::make_unique<std::mutex>())
        , status(CTEPartitionStatus::NORMAL)
        , mu(std::make_unique<std::mutex>())
        , pipe_cv(std::make_unique<PipeConditionVariable>())
        , expected_source_num(expected_source_num_)
    {}

    void setSharedConfig(std::shared_ptr<CTEPartitionSharedConfig> config) { this->config = config; }

    size_t getIdxInMemoryNoLock(size_t cte_reader_id);

    UInt64 getTotalEvictedBlockNumNoLock() const
    {
        return this->total_block_released_num + this->total_block_in_disk_num;
    }

    bool isBlockAvailableInDiskNoLock(size_t cte_reader_id)
    {
        auto idx = this->fetch_block_idxs[cte_reader_id];
        RUNTIME_CHECK_MSG(
            idx >= this->total_block_released_num,
            "partition: {}, idx: {}, total_block_released_num: {}",
            this->partition_id,
            idx,
            this->total_block_released_num);
        return idx < this->getTotalEvictedBlockNumNoLock();
    }

    bool isBlockAvailableInMemoryNoLock(size_t cte_reader_id)
    {
        return this->getIdxInMemoryNoLock(cte_reader_id) < this->blocks.size();
    }

    bool isSpillTriggeredNoLock() const { return this->total_block_in_disk_num > 0; }
    void addIdxNoLock(size_t cte_reader_id) { ++this->fetch_block_idxs[cte_reader_id]; }
    bool exceedMemoryThreshold() const
    {
#ifndef NDEBUG
        // config will be nullptr in test
        if unlikely (this->config == nullptr)
            return false;
#endif

        if (this->config->memory_threshold == 0)
            return false;
        return this->memory_usage.load() >= this->config->memory_threshold;
    }

    template <bool for_test>
    CTEOpStatus pushBlock(const Block & block);
    bool needSpill(bool try_mark_need_spill = false);
    CTEOpStatus tryGetBlock(size_t cte_reader_id, Block & block);
    CTEOpStatus spillBlocks();
    CTEOpStatus getBlockFromDisk(size_t cte_reader_id, Block & block);

    // Need aux_lock and mu
    void putTmpBlocksIntoBlocksNoLock()
    {
        for (const auto & block : this->tmp_blocks)
        {
            this->memory_usage.fetch_add(block.bytes());
            this->blocks.push_back(BlockWithCounter(block, static_cast<Int16>(this->expected_source_num)));
        }
        tmp_blocks.clear();
    }

    size_t total_byte_usage = 0;

    size_t partition_id;

    // Protect `status` and `tmp_blocks` variables
    std::unique_ptr<std::mutex> aux_lock;
    CTEPartitionStatus status;
    Blocks tmp_blocks;

    std::unique_ptr<std::mutex> mu;
    std::vector<BlockWithCounter> blocks;
    std::unordered_map<size_t, size_t> fetch_block_idxs;
    std::unique_ptr<PipeConditionVariable> pipe_cv;

    std::atomic_size_t memory_usage = 0;
    const size_t expected_source_num;

    std::unordered_map<size_t, SpillerPtr> spillers;
    std::unordered_map<size_t, BlockInputStreamPtr> cte_reader_restore_streams;
    UInt64 total_block_in_disk_num = 0;
    UInt64 total_block_released_num = 0;

    std::shared_ptr<CTEPartitionSharedConfig> config;

#ifndef NDEBUG
    std::unique_ptr<std::condition_variable> cv_for_test;
#endif
};
} // namespace DB
