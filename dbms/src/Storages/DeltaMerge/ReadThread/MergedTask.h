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
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB::DM
{
struct MergedUnit
{
    MergedUnit(const SegmentReadTaskPoolPtr & pool_, const SegmentReadTaskPtr & task_)
        : pool(pool_)
        , task(task_)
    {}

    SegmentReadTaskPoolPtr pool; // The information of a read request.
    SegmentReadTaskPtr task; // The information of a segment that want to read.
    BlockInputStreamPtr stream; // BlockInputStream of a segment, will be created by read threads.
};

// MergedTask merges the same segment of different SegmentReadTaskPools.
// Read segment input streams of different SegmentReadTaskPools sequentially to improve cache sharing.
// MergedTask is NOT thread-safe.
class MergedTask
{
public:
    static int64_t getPassiveMergedSegments() { return passive_merged_segments.load(std::memory_order_relaxed); }

    MergedTask(uint64_t seg_id_, std::vector<MergedUnit> && units_)
        : seg_id(seg_id_)
        , units(std::move(units_))
        , inited(false)
        , cur_idx(-1)
        , finished_count(0)
        , log(Logger::get())
    {
        passive_merged_segments.fetch_add(units.size() - 1, std::memory_order_relaxed);
        GET_METRIC(tiflash_storage_read_thread_gauge, type_merged_task).Increment();
    }
    ~MergedTask()
    {
        passive_merged_segments.fetch_sub(units.size() - 1, std::memory_order_relaxed);
        GET_METRIC(tiflash_storage_read_thread_gauge, type_merged_task).Decrement();
        GET_METRIC(tiflash_storage_read_thread_seconds, type_merged_task).Observe(sw.elapsedSeconds());
        // `setAllStreamFinished` must be called to explicitly releasing all streams for updating memory statistics of `MemoryTracker`.
        setAllStreamsFinished();
    }

    int readBlock();

    bool allStreamsFinished() const { return finished_count >= units.size(); }

    uint64_t getSegmentId() const { return seg_id; }

    size_t getPoolCount() const { return units.size(); }

    std::vector<uint64_t> getPoolIds() const
    {
        std::vector<uint64_t> ids;
        ids.reserve(units.size());
        for (const auto & unit : units)
        {
            if (unit.pool != nullptr)
            {
                ids.push_back(unit.pool->pool_id);
            }
        }
        return ids;
    }

    bool containPool(uint64_t pool_id) const
    {
        for (const auto & unit : units)
        {
            if (unit.pool != nullptr && unit.pool->pool_id == pool_id)
            {
                return true;
            }
        }
        return false;
    }
    void setException(const DB::Exception & e);

#ifndef DBMS_PUBLIC_GTEST
private:
#else
public:
#endif
    void initOnce();
    int readOneBlock();

    bool isStreamFinished(size_t i) const
    {
        return units[i].pool == nullptr && units[i].task == nullptr && units[i].stream == nullptr;
    }

    void setStreamFinished(size_t i)
    {
        if (!isStreamFinished(i))
        {
            // `MergedUnit.stream` must be released explicitly for updating memory statistics of `MemoryTracker`.
            auto & [pool, task, stream] = units[i];
            {
                MemoryTrackerSetter setter(true, pool->mem_tracker.get());
                task = nullptr;
                stream = nullptr;
            }
            pool = nullptr;
            finished_count++;
        }
    }

    void setAllStreamsFinished()
    {
        for (size_t i = 0; i < units.size(); ++i)
        {
            setStreamFinished(i);
        }
    }
    uint64_t seg_id;
    std::vector<MergedUnit> units;
    bool inited;
    int cur_idx;
    size_t finished_count;
    LoggerPtr log;
    Stopwatch sw;
    inline static std::atomic<int64_t> passive_merged_segments{0};
};

using MergedTaskPtr = std::shared_ptr<MergedTask>;

// MergedTaskPool is a MergedTask list.
// When SegmentReadTaskPool's block queue reaching limit, read thread will push MergedTask into it.
// The scheduler thread will try to pop a MergedTask of a related pool_id before build a new MergedTask object.
class MergedTaskPool
{
public:
    MergedTaskPool()
        : log(Logger::get())
    {}

    MergedTaskPtr pop(uint64_t pool_id);
    void push(const MergedTaskPtr & t);
    bool has(UInt64 pool_id);

private:
    std::mutex mtx;
    std::list<MergedTaskPtr> merged_task_pool;
    LoggerPtr log;
};
} // namespace DB::DM
