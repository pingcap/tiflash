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
    {
        auto * log = &Poco::Logger::get("LRUCache");
        LOG_INFO(log, fmt::format("xzxdebug memory threshold is set for {}", this->memory_threshold));
    }

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

    // TODO remove it
    void debugOutput()
    {
        // String total_info = fmt::format("total_blocks: {}, total_spill_blocks: {}", total_blocks, total_spill_blocks);
        // String spill_ranges_info = "spill_ranges: ";
        // for (auto & range : this->spill_ranges)
        //     spill_ranges_info = fmt::format("{}, ({} - {})", spill_ranges_info, range.first, range.second);
        // String fetch_in_mem_idxs_info = "fetch_in_mem_idxs: ";
        // for (const auto & v : this->fetch_in_mem_idxs)
        // {
        //     String tmp_info;
        //     for (auto idx : v.second)
        //         tmp_info = fmt::format("{}, {}", tmp_info, idx);
        //     fetch_in_mem_idxs_info = fmt::format("{} <{}: {}>", fetch_in_mem_idxs_info, v.first, tmp_info);
        // }
        // String fetch_in_disk_idxs_info = "fetch_in_disk_idxs: ";
        // for (const auto & v : this->fetch_in_disk_idxs)
        // {
        //     String tmp_info;
        //     for (auto idx : v.second)
        //         tmp_info = fmt::format("{}, {}", tmp_info, idx);
        //     fetch_in_disk_idxs_info = fmt::format("{} <{}: {}>", fetch_in_disk_idxs_info, v.first, tmp_info);
        // }

        // auto * log = &Poco::Logger::get("LRUCache");
        // LOG_INFO(log, fmt::format("xzxdebug | {} | {} | {} | {}", total_info, spill_ranges_info, fetch_in_mem_idxs_info, fetch_in_disk_idxs_info));
    }

    void setSharedConfig(std::shared_ptr<CTEPartitionSharedConfig> config) { this->config = config; }

    size_t getIdxInMemoryNoLock(size_t cte_reader_id);

    UInt64 getTotalEvictedBlockNumnoLock() const
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
        return idx < this->getTotalEvictedBlockNumnoLock();
    }

    bool isBlockAvailableInMemoryNoLock(size_t cte_reader_id)
    {
        return this->getIdxInMemoryNoLock(cte_reader_id) < this->blocks.size();
    }

    bool isSpillTriggeredNoLock() const { return this->total_block_in_disk_num > 0; }
    void addIdxNoLock(size_t cte_reader_id) { ++this->fetch_block_idxs[cte_reader_id]; }
    bool exceedMemoryThresholdNoLock() const
    {
        // config will be nullptr in test
        if unlikely (this->config == nullptr)
            return false;

        if (this->config->memory_threshold == 0)
            return false;
        return this->memory_usage >= this->config->memory_threshold;
    }

    template <bool for_test>
    CTEOpStatus pushBlock(const Block & block);
    CTEOpStatus tryGetBlock(size_t cte_reader_id, Block & block);
    CTEOpStatus spillBlocks(std::atomic_size_t & block_num, std::atomic_size_t & row_num);
    CTEOpStatus getBlockFromDisk(size_t cte_reader_id, Block & block);

    bool isBlockAvailableNoLock(size_t cte_reader_id)
    {
        if (this->isBlockAvailableInDiskNoLock(cte_reader_id))
            return true;

        return this->isBlockAvailableInMemoryNoLock(cte_reader_id);
    }

    // Need aux_lock and mu
    void putTmpBlocksIntoBlocksNoLock()
    {
        for (const auto & block : this->tmp_blocks)
        {
            this->memory_usage += block.bytes();
            this->blocks.push_back(BlockWithCounter(block, static_cast<Int16>(this->expected_source_num)));
        }
        tmp_blocks.clear();
    }

    // -----------
    // TODO delete them
    std::atomic_size_t total_blocks = 0;
    std::atomic_size_t total_spill_blocks = 0;

    std::vector<std::pair<size_t, size_t>> spill_ranges;
    std::map<size_t, std::vector<size_t>> fetch_in_mem_idxs;
    std::map<size_t, std::vector<size_t>> fetch_in_disk_idxs;

    bool first_log = true;
    // -----------

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

    size_t memory_usage = 0;
    const size_t expected_source_num;

    std::unordered_map<size_t, SpillerPtr> spillers;
    std::unordered_map<size_t, BlockInputStreamPtr> cte_reader_restore_streams;
    UInt64 total_block_in_disk_num = 0;
    UInt64 total_block_released_num = 0;

    std::shared_ptr<CTEPartitionSharedConfig> config;

    std::unique_ptr<std::mutex> mu_for_test;
    std::unique_ptr<std::condition_variable> cv_for_test;
};
} // namespace DB
