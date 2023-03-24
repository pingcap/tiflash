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

#include <Common/Arena.h>
#include <Interpreters/JoinPartition.h>

namespace DB
{
void JoinPartition::insertBlockForBuild(Block && block)
{
    size_t rows = block.rows();
    size_t bytes = block.bytes();
    build_partition.rows += rows;
    build_partition.bytes += bytes;
    build_partition.blocks.push_back(block);
    build_partition.original_blocks.push_back(std::move(block));
    addMemoryUsage(bytes);
}

void JoinPartition::insertBlockForProbe(Block && block)
{
    size_t rows = block.rows();
    size_t bytes = block.bytes();
    probe_partition.rows += rows;
    probe_partition.bytes += bytes;
    probe_partition.blocks.push_back(std::move(block));
    addMemoryUsage(bytes);
}
std::unique_lock<std::mutex> JoinPartition::lockPartition()
{
    return std::unique_lock(partition_mutex);
}
void JoinPartition::releaseBuildPartitionBlocks(std::unique_lock<std::mutex> &)
{
    subMemoryUsage(build_partition.bytes);
    build_partition.bytes = 0;
    build_partition.rows = 0;
    build_partition.blocks.clear();
    build_partition.original_blocks.clear();
}
void JoinPartition::releaseProbePartitionBlocks(std::unique_lock<std::mutex> &)
{
    subMemoryUsage(probe_partition.bytes);
    probe_partition.bytes = 0;
    probe_partition.rows = 0;
    probe_partition.blocks.clear();
}
void JoinPartition::releasePartitionPool(std::unique_lock<std::mutex> &)
{
    size_t pool_bytes = pool->size();
    pool.reset();
    subMemoryUsage(pool_bytes);
}
Blocks JoinPartition::trySpillBuildPartition(bool force, size_t max_cached_data_bytes, std::unique_lock<std::mutex> & partition_lock)
{
    if (spill && ((force && build_partition.bytes) || build_partition.bytes >= max_cached_data_bytes))
    {
        auto ret = build_partition.original_blocks;
        releaseBuildPartitionBlocks(partition_lock);
        if unlikely (memory_usage > 0)
        {
            memory_usage = 0;
            LOG_WARNING(log, "Incorrect memory usage after spill");
        }
        return ret;
    }
    else
    {
        return {};
    }
}
Blocks JoinPartition::trySpillProbePartition(bool force, size_t max_cached_data_bytes, std::unique_lock<std::mutex> & partition_lock)
{
    if (spill && ((force && probe_partition.bytes) || probe_partition.bytes >= max_cached_data_bytes))
    {
        auto ret = probe_partition.blocks;
        releaseProbePartitionBlocks(partition_lock);
        if unlikely (memory_usage != 0)
        {
            memory_usage = 0;
            LOG_WARNING(log, "Incorrect memory usage after spill");
        }
        return ret;
    }
    else
        return {};
}
} // namespace DB
