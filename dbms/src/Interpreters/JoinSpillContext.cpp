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

#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Pipeline/Schedule/Events/JoinSpillEvent.h>
#include <Interpreters/JoinSpillContext.h>

#include <memory>

namespace DB
{
void JoinSpillContext::initBuild(size_t /*concurrency*/, size_t partitions, const Block & sample_block)
{
    std::lock_guard lock(mu);
    build_spiller = std::make_unique<Spiller>(build_spill_config, false, partitions, sample_block, log);
}

void JoinSpillContext::initProbe(size_t /*concurrency*/, size_t partitions, const Block & sample_block)
{
    std::lock_guard lock(mu);
    probe_spiller = std::make_unique<Spiller>(probe_spill_config, false, partitions, sample_block, log);
}

void JoinSpillContext::spillBuildSideBlocks(PartitionBlockVecs && partition_block_vecs, bool is_last_spill, size_t /*stream_index*/)
{
    for (auto & partition_block_vec : partition_block_vecs)
    {
        build_spiller->spillBlocks(std::move(partition_block_vec.blocks), partition_block_vec.partition_index);
    }
    if (is_last_spill)
        build_spiller->finishSpill();
}

void JoinSpillContext::spillProbeSideBlocks(PartitionBlockVecs && partition_block_vecs, bool is_last_spill, size_t /*stream_index*/)
{
    for (auto & partition_block_vec : partition_block_vecs)
    {
        probe_spiller->spillBlocks(std::move(partition_block_vec.blocks), partition_block_vec.partition_index);
    }
    if (is_last_spill)
        probe_spiller->finishSpill();
}

UInt64 JoinSpillContext::maxBuildCacheBytes() const
{
    return build_spill_config.max_cached_data_bytes_in_spiller;
}

UInt64 JoinSpillContext::maxProbeCacheBytes() const
{
    return probe_spill_config.max_cached_data_bytes_in_spiller;
}

JoinSpillContextPtr JoinSpillContext::cloneWithNewId(const String & new_build_spill_id, const String & new_probe_spill_id)
{
    auto new_build_config = SpillConfig(build_spill_config.spill_dir, new_build_spill_id, build_spill_config.max_cached_data_bytes_in_spiller, build_spill_config.max_spilled_rows_per_file, build_spill_config.max_spilled_bytes_per_file, build_spill_config.file_provider);
    auto new_probe_config = SpillConfig(probe_spill_config.spill_dir, new_probe_spill_id, probe_spill_config.max_cached_data_bytes_in_spiller, probe_spill_config.max_spilled_rows_per_file, probe_spill_config.max_spilled_bytes_per_file, probe_spill_config.file_provider);
    return std::make_shared<JoinSpillContext>(log->identifier(), new_build_config, new_probe_config);
}

BlockInputStreams JoinSpillContext::restoreBuildSide(UInt64 partition_id, size_t concurrency)
{
    return build_spiller->restoreBlocks(partition_id, concurrency, true);
}

BlockInputStreams JoinSpillContext::restoreProbeSide(UInt64 partition_id, size_t concurrency)
{
    return probe_spiller->restoreBlocks(partition_id, concurrency, true);
}

PipelineJoinSpillContext::PipelineJoinSpillContext(
    const String & req_id,
    const SpillConfig & build_spill_config_,
    const SpillConfig & probe_spill_config_,
    PipelineExecutorContext & exec_context_)
    : JoinSpillContext(req_id, build_spill_config_, probe_spill_config_)
    , exec_context(exec_context_)
{
    exec_context.incActiveRefCount();
}

PipelineJoinSpillContext::~PipelineJoinSpillContext()
{
    // In order to ensure that `PipelineExecutorContext` will not be destructed before `PipelineJoinSpillContext` is destructed.
    exec_context.decActiveRefCount();
}

void PipelineJoinSpillContext::initBuild(size_t concurrency, size_t partitions, const Block & sample_block)
{
    JoinSpillContext::initBuild(concurrency, partitions, sample_block);
    build_spilling_tasks.resize(concurrency, 0);
}

void PipelineJoinSpillContext::initProbe(size_t concurrency, size_t partitions, const Block & sample_block)
{
    JoinSpillContext::initProbe(concurrency, partitions, sample_block);
    probe_spilling_tasks.resize(concurrency, 0);
}

bool PipelineJoinSpillContext::isBuildSideSpilling(size_t stream_index)
{
    std::lock_guard lock(mu);
    return 0 == build_spilling_tasks[stream_index];
}

bool PipelineJoinSpillContext::isProbeSideSpilling(size_t stream_index)
{
    std::lock_guard lock(mu);
    return 0 == probe_spilling_tasks[stream_index];
}

void PipelineJoinSpillContext::spillBuildSideBlocks(PartitionBlockVecs && partition_block_vecs, bool is_last_spill, size_t stream_index)
{
    {
        std::lock_guard lock(mu);
        ++build_spilling_tasks[stream_index];
    }
    auto spill_event = std::make_shared<JoinSpillEvent>(exec_context, log->identifier(), shared_from_this(), stream_index, true, std::move(partition_block_vecs), is_last_spill);
    RUNTIME_CHECK(spill_event->prepare());
    spill_event->schedule();
}

void PipelineJoinSpillContext::spillProbeSideBlocks(PartitionBlockVecs && partition_block_vecs, bool is_last_spill, size_t stream_index)
{
    {
        std::lock_guard lock(mu);
        ++probe_spilling_tasks[stream_index];
    }
    auto spill_event = std::make_shared<JoinSpillEvent>(exec_context, log->identifier(), shared_from_this(), stream_index, false, std::move(partition_block_vecs), is_last_spill);
    RUNTIME_CHECK(spill_event->prepare());
    spill_event->schedule();
}

JoinSpillContextPtr PipelineJoinSpillContext::cloneWithNewId(const String & new_build_spill_id, const String & new_probe_spill_id)
{
    auto new_build_config = SpillConfig(build_spill_config.spill_dir, new_build_spill_id, build_spill_config.max_cached_data_bytes_in_spiller, build_spill_config.max_spilled_rows_per_file, build_spill_config.max_spilled_bytes_per_file, build_spill_config.file_provider);
    auto new_probe_config = SpillConfig(probe_spill_config.spill_dir, new_probe_spill_id, probe_spill_config.max_cached_data_bytes_in_spiller, probe_spill_config.max_spilled_rows_per_file, probe_spill_config.max_spilled_bytes_per_file, probe_spill_config.file_provider);
    return std::make_shared<PipelineJoinSpillContext>(log->identifier(), new_build_config, new_probe_config, exec_context);
}
} // namespace DB
