// Copyright 2023 PingCAP, Inc.
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

#include <Common/nocopyable.h>
#include <Server/ServerInfo.h>
#include <Storages/DeltaMerge/ReadThread/WorkQueue.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB
{
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;
} // namespace DB

namespace DB::DM
{
class MergedTask;
using MergedTaskPtr = std::shared_ptr<MergedTask>;

class SegmentReader;
using SegmentReaderUPtr = std::unique_ptr<SegmentReader>;

class SegmentReaderPool
{
public:
    SegmentReaderPool(int thread_count, const std::vector<int> & cpus);
    ~SegmentReaderPool();

    DISALLOW_COPY_AND_MOVE(SegmentReaderPool);

    void addTask(MergedTaskPtr && task);
    std::vector<std::thread::id> getReaderIds() const;

private:
    void init(int thread_count, const std::vector<int> & cpus);

    WorkQueue<MergedTaskPtr> task_queue;
    std::vector<SegmentReaderUPtr> readers;
    LoggerPtr log;
};

// SegmentReaderPoolManager is a NUMA-aware singleton that manages several SegmentReaderPool objects.
// The number of SegmentReadPool object is the same as the number of CPU NUMA node.
// Thread number of a SegmentReadPool object is the same as the number of CPU logical core of a CPU NUMA node.
// Function `addTask` dispatches MergedTask to SegmentReadPool by their segment id, so a segment read task
// wouldn't be processed across NUMA nodes.
class SegmentReaderPoolManager
{
public:
    static SegmentReaderPoolManager & instance()
    {
        static SegmentReaderPoolManager pool_manager;
        return pool_manager;
    }
    void init(UInt32 logical_cpu_cores, double read_thread_count_scale);
    ~SegmentReaderPoolManager();
    DISALLOW_COPY_AND_MOVE(SegmentReaderPoolManager);

    void addTask(MergedTaskPtr && task);
    bool isSegmentReader() const;

    // Explicitly, release reader_pools and reader_ids.
    // Threads need to be stop before Context::shutdown().
    void stop();

private:
    SegmentReaderPoolManager();
    std::vector<std::unique_ptr<SegmentReaderPool>> reader_pools;
    std::unordered_set<std::thread::id> reader_ids;
    LoggerPtr log;
};

} // namespace DB::DM
