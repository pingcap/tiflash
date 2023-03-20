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

#include <Core/Block.h>

namespace DB
{
class Arena;
using ArenaPtr = std::shared_ptr<Arena>;
struct BuildPartition
{
    BlocksList blocks;
    Blocks original_blocks;
    size_t rows{0};
    size_t bytes{0};
};
struct ProbePartition
{
    Blocks blocks;
    size_t rows{0};
    size_t bytes{0};
};

class JoinPartition
{
public:
    JoinPartition(const LoggerPtr & log_)
        : pool(std::make_shared<Arena>())
        , spill(false)
        , log(log_)
    {}
    void insertBlockForBuild(Block && block);
    void insertBlockForProbe(Block && block);
    Blocks trySpillProbePartition(bool force, size_t max_cached_data_bytes)
    {
        std::unique_lock lock(partition_mutex);
        return trySpillProbePartition(force, max_cached_data_bytes, lock);
    }
    Blocks trySpillBuildPartition(bool force, size_t max_cached_data_bytes)
    {
        std::unique_lock lock(partition_mutex);
        return trySpillBuildPartition(force, max_cached_data_bytes, lock);
    }
    std::unique_lock<std::mutex> lockPartition();
    /// use lock as the argument to force the caller acquire the lock before call them
    void releaseBuildPartitionBlocks(std::unique_lock<std::mutex> &);
    void releaseProbePartitionBlocks(std::unique_lock<std::mutex> &);
    void releasePartitionPool(std::unique_lock<std::mutex> &);
    Blocks trySpillBuildPartition(bool force, size_t max_cached_data_bytes, std::unique_lock<std::mutex> & partition_lock);
    Blocks trySpillProbePartition(bool force, size_t max_cached_data_bytes, std::unique_lock<std::mutex> & partition_lock);
    bool hasBuildData() const { return !build_partition.original_blocks.empty(); }
    void addMemoryUsage(size_t delta)
    {
        memory_usage += delta;
    }
    void subMemoryUsage(size_t delta)
    {
        if likely (memory_usage >= delta)
            memory_usage -= delta;
        else
            memory_usage = 0;
    }
    bool isSpill() const { return spill; }
    void markSpill() { spill = true; }
    Block * getLastBuildBlock() { return &build_partition.blocks.back(); }
    /// after move hash table to `JoinPartition`, we can rename it to getPartitionHashMapSize
    /// and return the bytes used by both hash map and the pool
    size_t getPartitionPoolSize() const { return pool->size(); }
    ArenaPtr & getPartitionPool() { return pool; }
    size_t getMemoryUsage() const { return memory_usage; }

private:
    /// mutex to protect concurrent modify partition
    /// note if you wants to acquire both build_probe_mutex and partition_mutex,
    /// please lock build_probe_mutex first
    std::mutex partition_mutex;
    BuildPartition build_partition;
    ProbePartition probe_partition;
    ArenaPtr pool;
    bool spill;
    /// only update this field when spill is enabled. todo support this field in non-spill mode
    std::atomic<size_t> memory_usage{0};
    const LoggerPtr log;
};
using JoinPartitions = std::vector<std::unique_ptr<JoinPartition>>;
} // namespace DB
