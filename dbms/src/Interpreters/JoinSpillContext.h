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

    virtual void initBuild(size_t /*concurrency*/, size_t partitions, const Block & sample_block);

    virtual void initProbe(size_t /*concurrency*/, size_t partitions, const Block & sample_block);

    virtual void spillProbeSideBlocks(PartitionBlockVecs && partition_block_vecs, bool is_last_spill, size_t /*stream_index*/);

    virtual void spillBuildSideBlocks(PartitionBlockVecs && partition_block_vecs, bool is_last_spill, size_t /*stream_index*/);

    UInt64 maxBuildCacheBytes() const;

    UInt64 maxProbeCacheBytes() const;

    virtual JoinSpillContextPtr cloneWithNewId(const String & new_build_spill_id, const String & new_probe_spill_id);

    BlockInputStreams restoreBuildSide(UInt64 partition_id, size_t concurrency);

    BlockInputStreams restoreProbeSide(UInt64 partition_id, size_t concurrency);

protected:
    LoggerPtr log;

    std::mutex mu;

    SpillConfig build_spill_config;
    SpillConfig probe_spill_config;

    SpillerPtr build_spiller;
    SpillerPtr probe_spiller;
};

class PipelineExecutorContext;

class PipelineJoinSpillContext
    : public std::enable_shared_from_this<PipelineJoinSpillContext>
    , public JoinSpillContext
{
public:
    PipelineJoinSpillContext(
        const String & req_id,
        const SpillConfig & build_spill_config_,
        const SpillConfig & probe_spill_config_,
        PipelineExecutorContext & exec_context_);

    ~PipelineJoinSpillContext() override;

    void initBuild(size_t concurrency, size_t partitions, const Block & sample_block) override;

    void initProbe(size_t concurrency, size_t partitions, const Block & sample_block) override;

    bool isBuildSideSpilling(size_t stream_index);

    bool isProbeSideSpilling(size_t stream_index);

    void spillBuildSideBlocks(PartitionBlockVecs && partition_block_vecs, bool is_last_spill, size_t stream_index) override;

    void spillProbeSideBlocks(PartitionBlockVecs && partition_block_vecs, bool is_last_spill, size_t stream_index) override;

    JoinSpillContextPtr cloneWithNewId(const String & new_build_spill_id, const String & new_probe_spill_id) override;

private:
    friend class JoinSpillEvent;
    friend class JoinSpillTask;

    PipelineExecutorContext & exec_context;

    std::mutex mu;
    std::vector<UInt64> build_spilling_tasks;
    std::vector<UInt64> probe_spilling_tasks;
};
using PipelineJoinSpillContextPtr = std::shared_ptr<PipelineJoinSpillContext>;

} // namespace DB
