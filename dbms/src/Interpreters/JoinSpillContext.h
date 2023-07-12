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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Core/Spiller.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <common/types.h>

#include <memory>
#include <mutex>

namespace DB
{
struct PartitionBlockVec;
using PartitionBlockVecs = std::vector<PartitionBlockVec>;
struct PartitionBlockVec
{
    size_t partition_index;
    Blocks blocks;

    PartitionBlockVec(size_t partition_index_, Blocks && blocks_)
        : partition_index(partition_index_)
        , blocks(std::move(blocks_))
    {}

    static PartitionBlockVecs toVecs(size_t partition_index, Blocks && blocks)
    {
        PartitionBlockVecs vecs;
        vecs.emplace_back(partition_index, std::move(blocks));
        return vecs;
    }
};

class JoinSpillContext;
using JoinSpillContextPtr = std::shared_ptr<JoinSpillContext>;

class JoinSpillContext
{
public:
    JoinSpillContext(
        const String & req_id,
        const SpillConfig & build_spill_config_,
        const SpillConfig & probe_spill_config_)
        : log(Logger::get(req_id))
        , build_spill_config(build_spill_config_)
        , probe_spill_config(probe_spill_config_)
    {}

    virtual ~JoinSpillContext() = default;

    void initBuild(size_t partitions, const Block & sample_block)
    {
        build_spiller = std::make_unique<Spiller>(build_spill_config, false, partitions, sample_block, log);
    }

    void initProbe(size_t partitions, const Block & sample_block)
    {
        probe_spiller = std::make_unique<Spiller>(probe_spill_config, false, partitions, sample_block, log);
    }

    virtual void spillBuildSideBlocks(PartitionBlockVecs && partition_block_vecs, bool is_last_spill, size_t /*stream_index*/)
    {
        for (auto & partition_block_vec : partition_block_vecs)
        {
            build_spiller->spillBlocks(std::move(partition_block_vec.blocks), partition_block_vec.partition_index);
        }
        if (is_last_spill)
            build_spiller->finishSpill();
    }

    virtual bool isBuildSideSpilling(size_t /*stream_index*/)
    {
        return false;
    }

    virtual void spillProbeSideBlocks(PartitionBlockVecs && partition_block_vecs, bool is_last_spill, size_t /*stream_index*/)
    {
        for (auto & partition_block_vec : partition_block_vecs)
        {
            probe_spiller->spillBlocks(std::move(partition_block_vec.blocks), partition_block_vec.partition_index);
        }
        if (is_last_spill)
            probe_spiller->finishSpill();
    }

    virtual bool isProbeSideSpilling(size_t /*stream_index*/)
    {
        return false;
    }

    UInt64 maxBuildCacheBytes() const
    {
        return build_spill_config.max_cached_data_bytes_in_spiller;
    }

    UInt64 maxProbeCacheBytes() const
    {
        return probe_spill_config.max_cached_data_bytes_in_spiller;
    }

    virtual JoinSpillContextPtr cloneWithNewId(const String & new_build_spill_id, const String & new_probe_spill_id)
    {
        auto new_build_config = SpillConfig(build_spill_config.spill_dir, new_build_spill_id, build_spill_config.max_cached_data_bytes_in_spiller, build_spill_config.max_spilled_rows_per_file, build_spill_config.max_spilled_bytes_per_file, build_spill_config.file_provider);
        auto new_probe_config = SpillConfig(probe_spill_config.spill_dir, new_probe_spill_id, probe_spill_config.max_cached_data_bytes_in_spiller, probe_spill_config.max_spilled_rows_per_file, probe_spill_config.max_spilled_bytes_per_file, probe_spill_config.file_provider);
        return std::make_shared<JoinSpillContext>(log->identifier(), new_build_config, new_probe_config);
    }

    BlockInputStreams restoreBuildSide(UInt64 partition_id, size_t concurrency)
    {
        return build_spiller->restoreBlocks(partition_id, concurrency, true);
    }

    BlockInputStreams restoreProbeSide(UInt64 partition_id, size_t concurrency)
    {
        return probe_spiller->restoreBlocks(partition_id, concurrency, true);
    }

protected:
    LoggerPtr log;

    SpillConfig build_spill_config;
    SpillConfig probe_spill_config;

    SpillerPtr build_spiller;
    SpillerPtr probe_spiller;
};

class PipelineJoinSpillContext : public std::enable_shared_from_this<PipelineJoinSpillContext>
    , public JoinSpillContext
{
public:
    PipelineJoinSpillContext(
        const String & req_id,
        const SpillConfig & build_spill_config_,
        const SpillConfig & probe_spill_config_,
        PipelineExecutorContext & exec_context_)
        : JoinSpillContext(req_id, build_spill_config_, probe_spill_config_)
        , exec_context(exec_context_)
    {
        exec_context.incActiveRefCount();
    }

    ~PipelineJoinSpillContext() override
    {
        // In order to ensure that `PipelineExecutorContext` will not be destructed before `PipelineJoinSpillContext` is destructed.
        exec_context.decActiveRefCount();
    }

    // void spillBuildSideBlocks(PartitionBlockVecs && partition_block_vecs, bool is_last_spill, size_t stream_index) override
    // {
    // }

    // bool isBuildSideSpilling(size_t stream_index) override
    // {
    //     return false;
    // }

    // void spillProbeSideBlocks(PartitionBlockVecs && partition_block_vecs, bool is_last_spill, size_t stream_index) override
    // {
    // }

    // bool isProbeSideSpilling(size_t stream_index) override
    // {
    //     return false;
    // }

    JoinSpillContextPtr cloneWithNewId(const String & new_build_spill_id, const String & new_probe_spill_id) override
    {
        auto new_build_config = SpillConfig(build_spill_config.spill_dir, new_build_spill_id, build_spill_config.max_cached_data_bytes_in_spiller, build_spill_config.max_spilled_rows_per_file, build_spill_config.max_spilled_bytes_per_file, build_spill_config.file_provider);
        auto new_probe_config = SpillConfig(probe_spill_config.spill_dir, new_probe_spill_id, probe_spill_config.max_cached_data_bytes_in_spiller, probe_spill_config.max_spilled_rows_per_file, probe_spill_config.max_spilled_bytes_per_file, probe_spill_config.file_provider);
        return std::make_shared<PipelineJoinSpillContext>(log->identifier(), new_build_config, new_probe_config, exec_context);
    }

private:
    PipelineExecutorContext & exec_context;
};
} // namespace DB
