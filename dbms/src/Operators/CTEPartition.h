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

#include <Core/SpillConfig.h>
#include <Core/Spiller.h>
#include <Flash/Pipeline/Schedule/Tasks/PipeConditionVariable.h>

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

struct CTEPartition
{
    explicit CTEPartition(size_t partition_id_)
        : partition_id(partition_id_)
        , aux_lock(std::make_unique<std::mutex>())
        , status(CTEPartitionStatus::NORMAL)
        , mu(std::make_unique<std::mutex>())
        , pipe_cv(std::make_unique<PipeConditionVariable>())
    {}

    void debugOutput()
    {
        String info_block;
        for (const auto & item : this->total_fetch_block_nums)
            info_block = fmt::format("{} <{}: {}>", info_block, item.first, item.second);

        String info_row;
        for (const auto & item : this->total_fetch_row_nums)
            info_row = fmt::format("{} <{}: {}>", info_row, item.first, item.second);

        String disk_info_block;
        for (const auto & item : this->total_fetch_disk_block_nums)
            disk_info_block = fmt::format("{} <{}: {}>", disk_info_block, item.first, item.second);

        String disk_info_row;
        for (const auto & item : this->total_fetch_disk_row_nums)
            disk_info_row = fmt::format("{} <{}: {}>", disk_info_row, item.first, item.second);

        String infos;
        // for (const auto & item : this->fetch_idxs_disk)
        // {
        //     String nums;
        //     for (auto idx : item.second)
        //         nums = fmt::format("{} {}", nums, idx);
        //     infos = fmt::format("{} <cte_reader_id: {}, idxs: {}>", infos, item.first, nums);
        // }

        auto * log = &Poco::Logger::get("LRUCache");
        LOG_INFO(
            log,
            fmt::format(
                "xzxdebug CTEPartition total_recv_block_num: {}, row: {}, total_spill_block_num: {}, "
                "total_fetch_block_num: {}, row num: {}, "
                "disk: {}, {}"
                "total_byte_usage: {}, idxs_disk: {}",
                total_recv_block_num,
                total_recv_row_num,
                total_spill_block_num,
                info_block,
                info_row,
                disk_info_block,
                disk_info_row,
                total_byte_usage,
                infos));
    }

    void setSharedConfig(std::shared_ptr<CTEPartitionSharedConfig> config) { this->config = config; }

    size_t getIdxInMemoryNoLock(size_t cte_reader_id);
    bool isBlockAvailableInDiskNoLock(size_t cte_reader_id)
    {
        return this->fetch_block_idxs[cte_reader_id] < this->total_block_in_disk_num;
    }
    bool isBlockAvailableInMemoryNoLock(size_t cte_reader_id)
    {
        return this->getIdxInMemoryNoLock(cte_reader_id) < this->blocks.size();
    }

    bool isSpillTriggeredNoLock() const { return this->total_block_in_disk_num > 0; }
    void addIdxNoLock(size_t cte_reader_id) { this->fetch_block_idxs[cte_reader_id]++; }
    bool exceedMemoryThresholdNoLock() const { return this->memory_usage >= this->config->memory_threshold; }

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
    std::map<size_t, size_t> total_fetch_disk_block_nums;
    std::map<size_t, size_t> total_fetch_disk_row_nums;
    std::map<size_t, std::vector<size_t>> fetch_idxs_disk;
    size_t total_byte_usage = 0;

    size_t partition_id;

    std::unique_ptr<std::mutex> aux_lock;
    CTEPartitionStatus status;
    Blocks tmp_blocks;

    std::unique_ptr<std::mutex> mu;
    Blocks blocks;
    std::unordered_map<size_t, size_t> fetch_block_idxs;
    std::unique_ptr<PipeConditionVariable> pipe_cv;

    size_t memory_usage = 0;

    std::vector<UInt64> block_in_disk_nums;
    std::unordered_map<size_t, SpillerPtr> spillers;
    std::unordered_map<size_t, BlockInputStreamPtr> cte_reader_restore_streams;
    UInt64 total_block_in_disk_num = 0;

    std::shared_ptr<CTEPartitionSharedConfig> config;
};
} // namespace DB
